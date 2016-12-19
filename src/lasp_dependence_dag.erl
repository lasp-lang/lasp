%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Christopher S. Meiklejohn.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(lasp_dependence_dag).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("lasp.hrl").
-behaviour(gen_server).

%% API
-export([start_link/0,
         will_form_cycle/2,
         vertices/0,
         add_edges/6,
         add_vertex/1,
         add_vertices/1]).

%% Utility
-export([to_dot/0,
         export_dot/1]).

%% Test
%% @todo Only export on test.
-export([n_vertices/0,
         process_map/0,
         n_edges/0,
         out_degree/1,
         in_degree/1,
         out_edges/1,
         in_edges/1]).

-ifdef(TEST).
-export([contract/0,
         cleave/1,
         cleave_all/0]).
-endif.

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% Defines how often an optimization pass happens.
-define(CONTRACTION_INTERVAL, 1000).

%% Initial depth of all nodes.
-define(BASE_DEPTH, 0).

%%%===================================================================
%%% Type definitions
%%%===================================================================

%% To make contractions reversible, we have to hold all the
%% information about the removed vertices and the edges that were
%% connecting them. When a contraction is reversed, we use this infomation
%% to restart the intermediate processes, linking together the correct
%% vertices. One a contraction is reversed, all this metadata is removed.
%%
%% Stored metadata for a lasp process.
%% We don't store the input(s) and output vertices as that information
%% is implicitly stored by edges in the graph.
%%
%% Used to represent the metadata of processes inside a contracted path,
%% that only can have one parent and one child, hence why there is only
%% a single read function.
-record(process_metadata, {read :: function(),
                           transform :: function(),
                           write :: function()}).


%% A process hash is defined as the hash of their metadata (process_args).
%% This hash keeps constant through process restarts, and is used to uniquely
%% identify a process that connects the endpoints of a contracted path.
%% Removed vertices keep a reference to a process hash to identify in what path
%% they were before being removed (vertex_label).
-type process_hash() :: non_neg_integer().
-record(vertex_label, {process_pointer :: process_hash()}).
-type process_args() :: {[{lasp_vertex(), function()}],
                         function(),
                         {lasp_vertex(), function()}}.

%% We monitor all lasp processes, but the only information we get once that
%% happens is the process pid. This table keeps a mapping between hashes and
%% pids, and is used to during a cleaving pass to get to the information in
%% the optimized map.
-type pid_table() :: dict:dict(pid(), process_hash()).

%% This structure maps process hashes, identifying processes that connect
%% endpoints in a contracted path, to a list of vertices representing
%% the old path before being contracted, and a list of the metadata of
%% the lasp processes that linked those vertices together.
%% In this metadata list, each element at a position i represents the metadata
%% connecting the i-th and (i+1)-th vertices in the vertex list.
-type optimized_map() :: dict:dict(process_hash(),
                                   {pid(),
                                    contract_path(),
                                    list(#process_metadata{})}).

%% Only used to represent paths suitable to be contracted. These paths
%% consist of two necessary endpoints and a list of unnecessary vertices
%% in between.
-type contract_path() :: list(lasp_vertex()).

%% We store a mapping Pid -> [{parent_node, child_node}] to
%% find the edge labeled with it without traversing the graph.
%%
%% This is useful when the Pid of a lasp process changes
%% (because it gets restarted or it just terminates), as it
%% lets us quickly delete those edges.
-type process_map() :: dict:dict(pid(), {id(), id()}).

%% State. The graph is constructed using the digraph module.
%% See definitions above for further information.
-record(state, {dag :: digraph:graph(),
                process_map :: process_map(),
                optimized_map :: optimized_map(),
                pid_table :: pid_table(),
                contraction_timer :: timer:tref()}).

%% We store the function metadata as the edge label.
%% An edge in the graph represents a lasp process, and contains
%% the various functions used by it.
%%
%% Processes that have n inputs are modeled by n edges, all with the same
%% pid, connecting each input to the output. This means that we only have
%% to collect a single read function.
%% @todo merge with process_metadata record
-record(edge_label, {pid :: pid(),
                     read :: function(),
                     transform :: function(),
                     write :: function()}).

%% A vertex in the dag represents either a crdt value or a lasp process pid
%% Pids are used to model reads and updates to a value.
-type lasp_vertex() :: id() | pid().

%% Return type of digraph:edge/2
-type lasp_edge() :: {digraph:edge(),
                      digraph:vertex(),
                      digraph:vertex(),
                      #edge_label{}}.

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec add_vertex(lasp_vertex()) -> ok.
add_vertex(V) ->
    add_vertices([V]).

-spec add_vertices(list(lasp_vertex())) -> ok.
add_vertices([]) ->
    ok;

add_vertices(Vs) ->
    gen_server:call(?MODULE, {add_vertices, Vs}, infinity).

vertices() ->
    gen_server:call(?MODULE, vertices, infinity).

%% @doc Check if linking the given vertices will form a loop.
%%
%%      The user may accidentally form a loop while writing a dataflow
%%      computation.
%%
%%      Imagine this example:
%%
%%      A = declare(),
%%      B = declare(),
%%      map(A, \x.x+1, B)
%%      bind_to(A, B
%%
%%      As soon as A is given a value, it will start to grow in size,
%%      as its own internal value is incremented forever.
-spec will_form_cycle(list(lasp_vertex()), lasp_vertex()) -> boolean().
will_form_cycle(Src, Dst) ->
    gen_server:call(?MODULE, {will_form_cycle, Src, Dst}, infinity).

%% @doc For all V in Src, create an edge from V to Dst labelled with Pid.
%%
%%      Returns error if it couldn't create some of the edges,
%%      either because it formed a loop, or because some of the
%%      vertices weren't in the graph.
%%
-spec add_edges(list(lasp_vertex()),
                lasp_vertex(),
                pid(),
                list({lasp_vertex(), function()}),
                     function(),
                     {lasp_vertex(), function()}) -> ok | error.

add_edges(Src, Dst, Pid, ReadFuns, TransFun, WriteFun) ->
    gen_server:call(?MODULE, {add_edges, Src, Dst, Pid, ReadFuns, TransFun, WriteFun}, infinity).

%% @doc Return the dot representation as a string.
-spec to_dot() -> {ok, string()} | {error, no_data}.
to_dot() ->
    gen_server:call(?MODULE, to_dot, infinity).

%% @doc Write the dot representation of the dag to the given file path.
-spec export_dot(string()) -> ok | {error, no_data}.
export_dot(Path) ->
    gen_server:call(?MODULE, {export_dot, Path}, infinity).

n_vertices() ->
    gen_server:call(?MODULE, n_vertices, infinity).

n_edges() ->
    gen_server:call(?MODULE, n_edges, infinity).

in_degree(V) ->
    gen_server:call(?MODULE, {in_degree, V}, infinity).

out_degree(V) ->
    gen_server:call(?MODULE, {out_degree, V}, infinity).

out_edges(V) ->
    gen_server:call(?MODULE, {out_edges, V}, infinity).

in_edges(V) ->
    gen_server:call(?MODULE, {in_edges, V}, infinity).

process_map() ->
    gen_server:call(?MODULE, get_process_map, infinity).

-ifdef(TEST).

%% @doc Contract all suitable paths in the graph.
%%
%%      A path can be contracted if it contains one or more
%%      unnecessary vertices. An unnecessary vertex is one
%%      that only has one child and one parent.
%%
%%      This removes intermediate vertices from the graph.
%%
contract() ->
    gen_server:call(?MODULE, contract, infinity).

%% @doc Perform vertex cleaving on the given vertex.
%%
%%      Given a vertex that was removed as part of a path contraction,
%%      undo the contraction on all vertices of the path.
%%
%%      Does nothing if the vertex was not removed.
%%
cleave(Vertex) ->
    gen_server:call(?MODULE, {cleave, Vertex}, infinity).

%% @doc Perform vertex cleaving on all removed vertices.
%%
%%      Same as cleave/1, but on all removed vertices of the graph.
%%
cleave_all() ->
    gen_server:call(?MODULE, cleave_all, infinity).

-endif.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @doc Initialize state.
init([]) ->
    Timer = case lasp_config:get(automatic_contraction, false) of
        true ->
            {ok, Tref} = timer:send_after(?CONTRACTION_INTERVAL, contract),
            Tref;

        _ ->
            undefined
    end,
    {ok, #state{dag=digraph:new([acyclic]),
                process_map=dict:new(),
                optimized_map=dict:new(),
                pid_table=dict:new(),
                contraction_timer=Timer}}.

-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.

handle_call(vertices, _From, #state{dag=Dag}=State) ->
    %% Retrieve vertices from the graph.
    Vertices = digraph:vertices(Dag),

    %% Add information about out edges.
    Terminals = case lasp_config:get(back_propagate_nonterminals, true) of
                   true ->
                       Vertices;
                   false ->
                       lists:foldl(fun(V, Acc) ->
                                        case digraph:out_degree(Dag, V) of
                                            0 ->
                                                Acc ++ [V];
                                            _ ->
                                                Acc
                                        end
                                   end, [], Vertices)
               end,

    %% Annotate vertices with their label.
    Annotated = lists:map(fun(V) -> digraph:vertex(Dag, V) end, Terminals),

    {reply, {ok, Annotated}, State};

handle_call(n_vertices, _From, #state{dag=Dag}=State) ->
    {reply, {ok, digraph:no_vertices(Dag)}, State};

handle_call(n_edges, _From, #state{dag=Dag}=State) ->
    {reply, {ok, digraph:no_edges(Dag)}, State};

handle_call({in_degree, V}, _From, #state{dag=Dag}=State) ->
    {reply, {ok, digraph:in_degree(Dag, V)}, State};

handle_call({out_degree, V}, _From, #state{dag=Dag}=State) ->
    {reply, {ok, digraph:out_degree(Dag, V)}, State};

handle_call({out_edges, V}, _From, #state{dag=Dag}=State) ->
    Edges = [digraph:edge(Dag, E) || E <- digraph:out_edges(Dag, V)],
    {reply, {ok, Edges}, State};

handle_call({in_edges, V}, _From, #state{dag=Dag}=State) ->
    Edges = [digraph:edge(Dag, E) || E <- digraph:in_edges(Dag, V)],
    {reply, {ok, Edges}, State};

handle_call({add_vertices, Vs}, _From, #state{dag=Dag}=State) ->
    [digraph:add_vertex(Dag, V, ?BASE_DEPTH) || V <- Vs],
    {reply, ok, State};

handle_call(to_dot, _From, #state{dag=Dag}=State) ->
    {reply, to_dot(Dag), State};

handle_call({export_dot, Path}, _From, #state{dag=Dag}=State) ->
    R = case to_dot(Dag) of
        {ok, Content} -> file:write_file(Path, Content);
        Error -> Error
    end,
    {reply, R, State};

handle_call(get_process_map, _From, #state{process_map=PM}=State) ->
    {reply, {ok, dict:to_list(PM)}, State};

handle_call(contract, _From, State) ->
    NewState = contract_all(State),
    {reply, ok, NewState};

handle_call({cleave, Vertex}, _From, #state{dag=Dag, optimized_map=OptMap}=State) ->
    cleave_if_contracted(Dag, Vertex, OptMap),
    {reply, ok, State};

handle_call(cleave_all, _From, #state{optimized_map=OptMap}=State) ->
    lists:foreach(fun(Id) ->
        {Pid, _, _} = dict:fetch(Id, OptMap),
        spawn_link(fun() ->
            lasp_process_sup:terminate_child(lasp_process_sup, Pid)
        end)
    end, dict:fetch_keys(OptMap)),
    {reply, ok, State};

%% @doc Check if linking the given vertices will introduce a cycle in the graph.
%%
%%      Naive approach first: check if To is a member of From
%%
%%      Second approach: check if there is a path from `Dst` to
%%      any of the vertices in `Src`.
%%
%%      We want to check this before spawning a lasp process, otherwise
%%      an infinite loop can be created if the vertices form a loop.
%%
handle_call({will_form_cycle, Src, Dst}, _From, #state{dag=Dag, optimized_map=OptMap}=State) ->

    DirectCycle = lists:member(Dst, Src) orelse lists:any(fun(V) ->
        digraph:get_path(Dag, Dst, V) =/= false
    end, Src),

    Response = DirectCycle orelse optimized_cycle(Dag, Src, Dst, OptMap),

    {reply, Response, State};

%% @doc For all V in Src, create an edge from V to Dst labelled with Pid.
%%
%%      We monitor all edge Pids to know when they die or get restarted.
%%
handle_call({add_edges, Src, Dst, Pid, ReadFuns, TransFun, WriteFun}, _From, State) ->
    {Reply, NewState} = add_edges(Src, Dst, Pid,
                                  ReadFuns, TransFun, WriteFun, State),
    {reply, Reply, NewState}.

%% @private
-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
handle_cast(_Request, State) ->
    {noreply, State}.

%% @private
-spec handle_info(term(), #state{}) -> {noreply, #state{}}.

%% @doc Remove the edges associated with a lasp process when it terminates.
%%
%%      Given that lasp processes might get restarted or terminated,
%%      we have to know when it happens so we can delete the appropiate
%%      edges in the graph.
%%
handle_info({'DOWN', _, process, Pid, Reason}, #state{dag=Dag,
                                                      process_map=PM,
                                                      optimized_map=OptMap,
                                                      pid_table=PidTable}=State) ->

    {ok, Edges} = dict:find(Pid, PM),
    NewDag = lists:foldl(fun({F, T}, G) ->
        delete_with_pid(G, F, T, Pid)
    end, Dag, Edges),

    %% If terminated by supervisor, cleave any associated paths.
    NewState = case dict:find(Pid, PidTable) of
        {ok, Hash} -> case Reason of
            shutdown ->
                CleavedState = cleave_associated_path(NewDag, Hash, State),
                CleavedState#state{optimized_map=dict:erase(Hash, OptMap),
                                   pid_table=dict:erase(Pid, PidTable),
                                   process_map=dict:erase(Pid, CleavedState#state.process_map)};

            _ ->
                State#state{dag=NewDag,
                            process_map=dict:erase(Pid, PM),
                            pid_table=dict:erase(Pid, PidTable)}
        end;
        _ -> State#state{dag=NewDag,
                         process_map=dict:erase(Pid, PM)}
    end,

    {noreply, NewState};

handle_info(contract, #state{contraction_timer=OldTRef}=State) ->
    timer:cancel(OldTRef),
    NewState = contract_all(State),
    {ok, TRef} = timer:send_after(?CONTRACTION_INTERVAL, contract),
    {noreply, NewState#state{contraction_timer=TRef}};

handle_info(Msg, State) ->
    _ = lager:warning("Unhandled messages ~p", [Msg]),
    {noreply, State}.

%% @private
-spec terminate(term(), #state{}) -> term().
terminate(_Reason, _State) ->
    ok.

%% @private
-spec code_change(term() | {down, term()}, #state{}, term()) -> {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

add_edges(Src, Dst, Pid, ReadFuns, TransFun, {Dst, WriteFun}, State) ->

    Dag      = State#state.dag,
    Pm       = State#state.process_map,
    OptMap   = State#state.optimized_map,
    PidTable = State#state.pid_table,

    %% Add vertices only if they are either sources or sinks. (See add_if)
    %% All user-defined variables are tracked through the `declare` function.
    lists:foreach(fun(V) -> add_if_pid(Dag, V) end, Src),
    add_if_pid(Dag, Dst),

    %% Check if this edge is the replacement for an old edge in the graph.
    {NewOptMap, NewPidTable} = replace_if_restarted(OptMap, PidTable, Pid,
                                                                      {ReadFuns,
                                                                       TransFun,
                                                                       {Dst, WriteFun}}),

    %% @todo This should happen before creating the process
    %%
    %%       Otherwise the process acts on old data. This is only a problem
    %%       with queries and reads, since they return the value of the vertex.
    %%       Binds and updates are ok, since values will eventually propagate
    %%       through the graph.
    %%
    %% Undo any optimizations involving these vertices.
    lists:foreach(fun(V) ->
        cleave_if_contracted(Dag, V, NewOptMap)
    end, [Dst | Src]),

    %% For all V in Src, make edge (V, Dst) with label {Pid, Read, Trans, Write}
    %% (where {Id, Read} = ReadFuns s.t. Id = V)
    Status = lists:map(fun(V) ->
        Read = lists:nth(1, [ReadF || {Id, ReadF} <- ReadFuns, Id =:= V]),
        EdgeResult = digraph:add_edge(Dag, V, Dst, #edge_label{pid=Pid,
                                                               read=Read,
                                                               transform=TransFun,
                                                               write=WriteFun}),

        %% Determine depth; but ignore nodes that are transient,
        %% single-fire processes.
        case Dst of
            {_Id, _Type} ->
                Depth = depth(Dag, Dst, ?BASE_DEPTH),

                %% Re-create destination vertex with new depth.
                case digraph:vertex(Dag, Dst) of
                    {_, Label} ->
                        case Label of
                            Depth ->
                                %% Already correct depth; ignore.
                                ok;
                            Depth0 ->
                                VertexResult = digraph:add_vertex(Dag, Dst, Depth),
                                lager:info("Vertex: ~p re-created with depth ~p => ~p; result: ~p",
                                           [Dst, Depth0, Depth, VertexResult]),
                                lager:info("Vertex: ~p edges: ~p",
                                           [Dst, digraph:edges(Dag, Dst)])
                        end;
                    _ ->
                        ok
                end;
            _ ->
                ok
        end,

        EdgeResult
    end, Src),

    {R, St} = case lists:any(fun is_graph_error/1, Status) of
        true ->
            %% Sometimes if someone tries to read an object
            %% that's not there, we'll try to make an edge to a
            %% non-existent vertex and will trigger this
            %% error.  Ignore.
            {ok, State};
        false ->
            erlang:monitor(process, Pid),

            %% For all V in Src, append Pid -> {V, Dst}
            %% in the process map.
            ProcessMap = lists:foldl(fun(El, D) ->
                dict:append(Pid, {El, Dst}, D)
            end, Pm, Src),

            {ok, State#state{process_map=ProcessMap}}
    end,

    {R, St#state{optimized_map = NewOptMap, pid_table = NewPidTable}}.


is_graph_error({error, _}) ->
    true;

is_graph_error(_) ->
    false.

%% @doc Delete all edges between Src and Dst with the given pid..
-spec delete_with_pid(digraph:graph(), lasp_vertex(), lasp_vertex(), term()) -> digraph:graph().
delete_with_pid(Graph, Src, Dst, Pid) ->
    lists:foreach(fun
        ({E, _, _, #edge_label{pid=TargetPid}}) when TargetPid =:= Pid ->
            digraph:del_edge(Graph, E);
        (_) -> ok
    end, get_direct_edges(Graph, Src, Dst)),
    Graph.

%% @doc Return all direct edges linking V1 and V2.
%%
%%      If V1 and V2 are not linked, return the empty list.
%%
%%      Otherwise, get all emanating edges from V1, and return
%%      only the ones linking to V2.
%%
-spec get_direct_edges(digraph:graph(),
                       lasp_vertex(), lasp_vertex()) -> list(lasp_edge()).

get_direct_edges(G, V1, V2) ->
    lists:flatmap(fun(Ed) ->
        case digraph:edge(G, Ed) of
            {_, _, To, _}=E when To =:= V2 -> [E];
            _ -> []
        end
    end, digraph:out_edges(G, V1)).

%% @doc Add a vertex only if it is a pid
%%
%%      We only add it if it isn't already present on the dag,
%%      as adding the same vertex multiple times removes any
%%      metadata (labels).
%%
-spec add_if_pid(digraph:graph(), lasp_vertex()) -> ok.
add_if_pid(Dag, Pid) when is_pid(Pid) ->
   case digraph:vertex(Dag, Pid) of
      false ->
           digraph:add_vertex(Dag, Pid);
      _ ->
           ok
   end;

add_if_pid(_, _) ->
    ok.

%%%===================================================================
%%% Contraction Functions
%%%===================================================================

%% @doc Return a list of contraction candidate paths in the graph.
%%
%%      A contraction path is formed by two necessary endpoints, and
%%      a list of unnecessary vertices connecting them.
%%
%%      If no paths are found, the empty list is returned.
%%
-spec contraction_paths(digraph:graph()) -> list(contract_path()).
contraction_paths(G) ->
    Result = contraction_paths(G, digraph_utils:topsort(G), sets:new(), [[]]),
    lists:filter(fun(L) -> length(L) > 0 end, Result).

-spec contraction_paths(digraph:graph(),
                        list(lasp_vertex()),
                        sets:set(lasp_vertex()),
                        list(digraph:vertex())) -> list(contract_path()).

contraction_paths(G, [V | Vs], Visited, Acc) ->
    case sets:is_element(V, Visited) of
        true -> contraction_paths(G, Vs, Visited, Acc);
        _ -> case is_unnecessary(G, V) of
            true ->
                Path = get_children_while(fun(El) ->
                    is_unnecessary(G, El)
                end, G, V),

                AllVisited = lists:foldl(fun sets:add_element/2, Visited, Path),

                %% We already know it only has one parent.
                [Parent | _] = digraph:in_neighbours(G, V),

                contraction_paths(G, Vs, AllVisited, [[Parent | Path] | Acc]);
            false ->
                contraction_paths(G, Vs, sets:add_element(V, Visited), Acc)
        end
    end;

contraction_paths(_, [], _, Acc) -> Acc.

%% @doc Recursively get all the children of a given vertex that satisfy
%%      the given predicate.
%%
%%      Returns a list of the children, in depth-first order, with the
%%      first element that doesn't satisfy the predicate in the last
%%      position of the list.
%%
%%      If the given vertex has no children, or if it doesn't satisfy
%%      the predicate, a list with it as the only element is returned.
%%
-spec get_children_while(fun((lasp_vertex()) -> boolean()),
                         digraph:graph(),
                         lasp_vertex()) -> list(lasp_vertex()).

get_children_while(Pred, G, V) ->
    lists:reverse(get_children_while(Pred, G, V, [])).

-spec get_children_while(fun((lasp_vertex()) -> boolean()),
                         digraph:graph(),
                         lasp_vertex(),
                         list(lasp_vertex())) -> list(lasp_vertex()).

get_children_while(Pred, G, V, Acc) ->
    case Pred(V) of
        true ->
            Res = lists:flatmap(fun(Child) ->
                get_children_while(Pred, G, Child, Acc)
            end, digraph:out_neighbours(G, V)),
            Res ++ Acc ++ [V];
        false -> [V | Acc]
    end.

%% @doc Unnecessary vertex.
%%
%%      An unnecessary vertex iff its out degree = in degree = 1, where
%%      the parent and the child are regular vertices (not pids) and the
%%      child only has one parent.
%%
%%      Unnecessary vertices can be contracted in the graph.
%%
-spec is_unnecessary(digraph:graph(), lasp_vertex()) -> boolean().
is_unnecessary(G, V) ->
    case digraph:in_degree(G, V) =:= 1 andalso digraph:out_degree(G, V) =:= 1 of
        false -> false;
        true ->
            %% We already know it only has one parent and one child.
            [Parent | _] = digraph:in_neighbours(G, V),
            [Child  | _] = digraph:out_neighbours(G, V),
            %% Parent isn't a pid, Child isn't a pid _and_ only has a parent.
            not is_pid(Parent) andalso maybe_unnecessary(G, Child)
    end.

%% @doc Unnecessary vertex candidate.
-spec maybe_unnecessary(digraph:graph(), lasp_vertex()) -> boolean().
maybe_unnecessary(_G, V) when is_pid(V) ->
    false;

maybe_unnecessary(G, V) ->
    digraph:in_degree(G, V) =:= 1.

%% @doc Replace the Pid in the optimized map if the old one was restarted.
%%
%%      Removed vertices hold a reference to the Id (hash) of the process
%%      that connects the endpoints of the path that was contracted.
%%
%%      If this process is restarted, we have to update the pid in the
%%      optimized map, otherwise vertices will hold an old reference.
%%
-spec replace_if_restarted(optimized_map(), pid_table(), pid(),
                           process_args()) -> {optimized_map(), pid_table()}.

replace_if_restarted(OptMap, PidTable, Pid, ProcessArgs) ->
    Id = process_hash(ProcessArgs),
    case dict:find(Id, OptMap) of
        error -> {OptMap, PidTable};
        {ok, {_, VSeq, Metadata}} ->
          {dict:store(Id, {Pid, VSeq, Metadata}, OptMap), dict:store(Pid, Id, PidTable)}
    end.

%% @doc Find and contract all suitable paths to be contracted in the graph.
-spec contract_all(#state{}) -> #state{}.
contract_all(#state{dag=Dag}=State) ->
    lists:foldl(fun(P, S) ->
        contract(Dag, P, S)
    end, State, contraction_paths(Dag)).

%% @doc Perform path contraction in the given sequence of vertices.
%%
%%      The resulting edge represents a lasp process with the read
%%      function of the first vertex, the write function of the last
%%      and the composition of all inner transform functions.
%%
%%      Given two consecutive edges, (v1, v2) = f and (v2, v3) = g, with
%%      metadata:
%%
%%      f = <r_f, t_f, w_f>
%%
%%      g = <r_g, t_g, w_g>
%%
%%      where `r`, `t` and `w` represent the read, transform and write
%%      functions, we define the composition of `f` and `g` as
%%
%%      g . f = <r_f, (t_g . t_f), w_g >
%%
%%      where ( . ) is defined as the usual composition operator.
%%      The result of this operation is a new edge h = (v1, v3).
%%
-spec contract(digraph:graph(), contract_path(), #state{}) -> #state{}.
contract(G, VSeq, State) ->

    [First, Second | _] = VSeq,
    Last = lists:last(VSeq),
    SndLast = lists:nth(length(VSeq) - 1, VSeq),

    %% Read function from the first vertex.
    ReadFun = lists:nth(1, get_read_functions(G, First, Second)),
    Read = {First, ReadFun},

    %% List of all transforming functions.
    TransFuns = collect_trans_funs(G, VSeq),

    %% Write function from the last vertex.
    WriteFun = lists:nth(1, get_write_functions(G, SndLast, Last)),
    Write = {Last, WriteFun},

    %% Since all transforming functions (with arity one) are
    %% of type (CRDT -> value), we need an intermediate
    %% function (value -> CRDT) to be able to compose them.
    %%
    %% The last function gets back the result from the last output.
    %%
    %% We define path contraction on those containing unnecessary
    %% vertices only, so we don't care for multi-arity functions.
    TransFun = fun({Id, T, Metadata, _OldValue}=X) ->
        apply_sequentially(X, TransFuns, fun(NewValue) ->
            {Id, T, Metadata, NewValue}
        end, fun({_, _, _, V}) ->  V  end)
    end,

    ProcessHash = process_hash({[Read], TransFun, Write}),

    %% Manually start a new lasp process and add the edges to the graph.
    {ok, Pid} = lasp_process:start_manual_process([[Read], TransFun, Write]),
    {ok, NewState} = add_edges([First], Last, Pid,
                               [Read], TransFun, Write, State),

    NewDag   = NewState#state.dag,
    OptMap   = NewState#state.optimized_map,
    PidTable = NewState#state.pid_table,

    %% Remove the intermediate edges by terminating the associated processes.
    NewOptMap = remove_edges(NewDag, VSeq, ProcessHash, Pid, OptMap),

    NewState#state{optimized_map = NewOptMap, pid_table = dict:store(Pid, ProcessHash, PidTable)}.

%% @doc Remove intermediate edges in a contracted path.
%%
%%      Deletes all intermediate edges in the path, and tags
%%      all unnecessary vertices with the given Pid, that should
%%      represent the resulting lasp process of the path contraction.
%%
-spec remove_edges(digraph:graph(), contract_path(), process_hash(), pid(), optimized_map()) -> optimized_map().
remove_edges(Dag, VSeq, Id, Pid, OptMap) ->
    %% Store process metadata in the optimized map
    Metadata = get_metadata(Dag, VSeq),

    %% Tag all unnecessary vertices in the path with the new process Pid
    UnnecesaryVertices = lists:sublist(VSeq, 2, length(VSeq) - 2),
    tag_vertices(Dag, UnnecesaryVertices, #vertex_label{process_pointer=Id}),

    %% Delete the intermediate edges and kill the associated processes.
    OldPids = collect_pids(Dag, VSeq),
    spawn_link(fun() ->
        lists:foreach(fun(P) ->
            lasp_process_sup:terminate_child(lasp_process_sup, P)
        end, OldPids)
    end),

    dict:store(Id, {Pid, VSeq, Metadata}, OptMap).

%% @doc Check if a list of future edges involving contracted vertices introduce a loop.
%%
%%      Checks optimized nodes so that we don't accidentally introduce
%%      loops while a vertex is not connected.
%%
%%      For example, A -> B -> C, B -> A is a loop, but if (A, B) is
%%      optimized, we could make that edge. If we cleave after that,
%%      trying to make (A, B) will fail.
%%
-spec optimized_cycle(digraph:graph(), list(lasp_vertex()), lasp_vertex(), optimized_map()) -> boolean().
optimized_cycle(G, From, To, OptMap) ->
    OptimizedTails = lists:filter(fun(F) ->
        not (contracted(G, F) =:= false)
    end, From),
    case {OptimizedTails, contracted(G, To)} of

        %% Contracted -> Contracted forms a loop if the tails of both are
        %% the same, or if it exists a path from the tail of the child
        %% to the tail of the parent.
        {[_|_]=ContractedVertices, {true, ChildId}} ->

            ParentTails = lists:map(fun(V) ->
                {_, #vertex_label{process_pointer=ParentId}} = digraph:vertex(G, V),
                get_process_tail(ParentId, OptMap)
            end, ContractedVertices),

            ChildTail = get_process_tail(ChildId, OptMap),

            lists:any(fun(PTail) ->
                not (digraph:get_path(G, ChildTail, PTail) =:= false)
                orelse (ChildTail =:= PTail)
            end, ParentTails);

        %% Contracted -> Uncontracted forms a loop if there exists a path
        %% from the child to the tail of the parent, or if the child is the
        %% source.
        {[_|_]=ContractedVertices, false} ->

            Tails = lists:map(fun(V) ->
                {_, #vertex_label{process_pointer = Id}} = digraph:vertex(G, V),
                get_process_tail(Id, OptMap)
            end, ContractedVertices),

            lists:any(fun(Tail) ->
                not (digraph:get_path(G, To, Tail) =:= false)
                orelse (Tail =:= To)
            end, Tails);

        %% Uncontracted -> Contracted forms a loop if there exists a path
        %% from the source of the child to the parent, or the source is
        %% the parent.
        {[], {true, Id}} ->

            Tail = get_process_tail(Id, OptMap),

            lists:any(fun(V) ->
                not (digraph:get_path(G, Tail, V) =:= false)
                orelse Tail =:= V
            end, From);

        %% Both are uncontracted, we can let callee deal with this.
        {[], false} -> false
    end.

%% @doc Get the tail of the process that contracted a path.
-spec get_process_tail(process_hash(), optimized_map()) -> lasp_vertex().
get_process_tail(Id, OptMap) ->
    {_, [Tail | _], _} = dict:fetch(Id, OptMap),
    Tail.

%%%===================================================================
%%% Cleave Functions
%%%===================================================================

%% @doc If the given vertex was part of a contracted path, cleave it
%%
%%      Contracted vertices contain a pointer to the Pid of the process
%%      that forms the contraction of the path.
%%
-spec cleave_if_contracted(digraph:graph(), lasp_vertex(), optimized_map()) -> ok.
cleave_if_contracted(G, Vertex, OptMap) ->
    case contracted(G, Vertex) of
        {true, Id} ->
            {Pid, _, _} = dict:fetch(Id, OptMap),
            spawn_link(fun() ->
                lasp_process_sup:terminate_child(lasp_process_sup, Pid)
            end),
            ok;
        false -> ok
    end.

%% @doc Cleave the path represented by the given process Pid.
%%
%%      If the Pid represents a process that contracted a path,
%%      start all the intermediate processes of the path,
%%      and then kill the given Pid.
%%
cleave_associated_path(G, Hash, #state{optimized_map=OptMap}=State) ->
    case dict:find(Hash, OptMap) of
        error -> State;
        {ok, {_, VSeq, MetadataList}} ->
            ProcessArgs = unpack_optimized_map(VSeq, MetadataList),
            UnnecesaryVertices = lists:sublist(VSeq, 2, length(VSeq) - 2),
            tag_vertices(G, UnnecesaryVertices, []),
            lists:foldl(fun({Reads, Transform, {Dst, Write}}=Args, St) ->
                Src = [To || {To, _} <- Reads],
                {ok, Pid} = lasp_process:start_manual_process(tuple_to_list(Args)),
                {ok, NewState} = add_edges(Src, Dst, Pid,
                                           Reads, Transform, {Dst, Write}, St),
                NewState
            end, State, ProcessArgs)
    end.

%% @doc Get the process arguments of the given optimized map inner dict.

-spec unpack_optimized_map(contract_path(), list(#process_metadata{})) -> list(process_args()).
unpack_optimized_map(VSeq, MetadataList) ->
    mapi(fun(Pos, El) ->
        ReadFun = El#process_metadata.read,
        Read = {lists:nth(Pos, VSeq), ReadFun},

        Transform = El#process_metadata.transform,

        WriteFun = El#process_metadata.write,
        Write = {lists:nth(Pos + 1, VSeq), WriteFun},

        {[Read], Transform, Write}
    end, MetadataList).

%% @doc Check if a vertex was optimized in the past.
%%
%%      If it was optimized, return also the pointer to the Pid
%%      of the process that contracted the path.
%%
-spec contracted(digraph:graph(), lasp_vertex()) -> {true, pid()} | false.
contracted(G, V) ->
    case digraph:vertex(G, V) of
        {_, #vertex_label{process_pointer=Id}} -> {true, Id};
        _ -> false
    end.

%% @doc Given a path contraction candidate in the graph, return the process
%%      metadata from all intermediate edges.
%%
%%      Used to build the optimized map.
%%
-spec get_metadata(digraph:graph(), contract_path()) -> list(#process_metadata{}).

get_metadata(G, [_ | Tail]=VSeq) ->
    zipwith(fun(Src, Dst) ->
        lists:nth(1, get_metadata(G, Src, Dst))
    end, VSeq, Tail).

%% @doc Get the process metadata for all edges between the given vertices.
-spec get_metadata(digraph:graph(),
                   lasp_vertex(),
                   lasp_vertex()) -> list(#process_metadata{}).

get_metadata(G, V1, V2) ->
    Edges = get_direct_edges(G, V1, V2),
    lists:map(fun({_, _, _, Metadata}) ->
        #process_metadata{read=Metadata#edge_label.read,
                          transform=Metadata#edge_label.transform,
                          write=Metadata#edge_label.write}
    end, Edges).

%% @doc Tag the unnecessary vertices in the given path with a pid.
-spec tag_vertices(digraph:graph(), contract_path(), #vertex_label{} | []) -> ok.
tag_vertices(Dag, VSeq, Label) ->
    lists:foreach(fun(V) ->
        digraph:add_vertex(Dag, V, Label)
    end, VSeq).

%%%===================================================================
%%% Utility Functions
%%%===================================================================

%% @doc Get the list of pids from the edges between V1 and V2
-spec get_connecting_pids(digraph:graph(),
                          lasp_vertex(),
                          lasp_vertex()) -> list(pid()).

get_connecting_pids(G, V1, V2) ->
    get_edge_properties(fun({_, _, _, E}) ->
        E#edge_label.pid
    end, G, V1, V2).

%% @doc Recursively get all pids from the given path.
-spec collect_pids(digraph:graph(), contract_path()) -> list(pid()).
collect_pids(G, [_ | T]=Seq) ->
    lists:flatten(zipwith(fun(Src, Dst) ->
        get_connecting_pids(G, Src, Dst)
    end, Seq, T)).

%% @doc Get the list of read functions from the edges between V1 and V2
-spec get_read_functions(digraph:graph(),
                         lasp_vertex(),
                         lasp_vertex()) -> list(function()).

get_read_functions(G, V1, V2) ->
    get_edge_properties(fun({_, _, _, E}) ->
        E#edge_label.read
    end, G, V1, V2).

%% @doc Get the list of transform functions from the edges between V1 and V2
-spec get_transform_functions(digraph:graph(),
                              lasp_vertex(),
                              lasp_vertex()) -> list(function()).

get_transform_functions(G, V1, V2) ->
    get_edge_properties(fun({_, _, _, E}) ->
        E#edge_label.transform
    end, G, V1, V2).

%% @doc Recursively get all transform functions from the given path.
-spec collect_trans_funs(digraph:graph(), contract_path()) -> list(function()).
collect_trans_funs(G, [_ | T]=Seq) ->
    lists:flatten(zipwith(fun(Src, Dst) ->
        get_transform_functions(G, Src, Dst)
    end, Seq, T)).

%% @doc Get the list of write functions from the edges between V1 and V2
-spec get_write_functions(digraph:graph(),
                          lasp_vertex(),
                          lasp_vertex()) -> list(function()).

get_write_functions(G, V1, V2) ->
    get_edge_properties(fun({_, _, _, E}) ->
        E#edge_label.write
    end, G, V1, V2).

-spec get_edge_properties(function(),
                          digraph:graph(),
                          lasp_vertex(),
                          lasp_vertex()) -> list(pid() | function()).

get_edge_properties(Fn, G, V1, V2) ->
    lists:map(Fn, get_direct_edges(G, V1, V2)).

%% @doc Zipwith that works with lists of different lengths.
%%
%%      Stops as soon as one of the lists is empty.
%%
%%      zipwith(fun(X, Y) -> {X, Y} end, [1,2,3], [1,2]).
%%      => [{1,1}, {2,2}]
%%
-spec zipwith(function(), list(any()), list(any())) -> list(any()).
zipwith(Fn, [X | Xs], [Y | Ys]) ->
    [Fn(X, Y) | zipwith(Fn, Xs, Ys)];

zipwith(Fn, _, _) when is_function(Fn, 2) -> [].

%% @doc Same as lists:map, but the function is applied to the index of
%%      the element as first argument (counting from 1), and the element
%%      itself as second argument.
%%
mapi(F, List) ->
    mapi(F, List, 1).

mapi(F, [H | T], Current) ->
    [F(Current, H) | mapi(F, T, Current + 1)];

mapi(F, [], _Current) when is_function(F, 2) -> [].

%% @doc Thread a value through a list of functions.
%%
%%      Takes an initial value, a list of functions, and two transforming
%%      functions. The first one transforms the output of a function into
%%      the input of the next one in the list. The second transforms the
%%      output of the final function in the list.
%%
%%      When Int and Final are the identity function, apply_sequentially
%%      is equivalent to applying X to the composition of all functions
%%      in the list.
%%
-spec apply_sequentially(any(), list(function()), function(), function()) -> any().
apply_sequentially(X, [], _, Final) -> Final(X);
apply_sequentially(X, [H | T], Int, Final) ->
    apply_sequentially(Int(H(X)), T, Int, Final).

%% @doc Get an unique identifier for a process in the graph.
-spec process_hash(process_args()) -> process_hash().
process_hash(Args) ->
    erlang:phash2(Args).

%%%===================================================================
%%% .DOT export functions
%%%===================================================================

to_dot(Graph) ->
    Vertices = lists:filter(fun(V) ->
        not (digraph:in_degree(Graph, V) =:= 0 andalso digraph:out_degree(Graph, V) =:= 0)
    end, digraph_utils:topsort(Graph)),
    case Vertices of
        [] -> {error, no_data};
        VertexList ->
            Start = ["digraph dag {\n"],
            DrawedVertices =  lists:foldl(fun(V, Acc) ->
                Acc ++ v_str(V) ++ " [fontcolor=black, style=filled, fillcolor=\"#613B93\"];\n"
            end, Start, VertexList),
            {ok, unicode:characters_to_list(write_edges(Graph, VertexList, [], DrawedVertices) ++ "}\n")}
    end.

write_edges(G, [V | Vs], Visited, Result) ->
    Edges = lists:map(fun(E) -> digraph:edge(G, E) end, digraph:out_edges(G, V)),
    R = lists:foldl(fun({_, _, To, #edge_label{pid=Pid}}, Acc) ->
        case lists:member(To, Visited) of
            true -> Acc;
            false ->
                Acc ++ v_str(V) ++ " -> " ++ v_str(To) ++
                " [label=" ++ erlang:pid_to_list(Pid) ++ "];\n"
        end
    end, Result, Edges),
    write_edges(G, Vs, [V | Visited], R);

write_edges(_G, [], _Visited, Result) ->
    Result.

%% @doc Generate an unique identifier for a vertex.
v_str({Id, _}) ->
    erlang:integer_to_list(erlang:phash2(Id));

v_str(V) when is_pid(V)->
    pid_to_list(V).

%% @private
depth(G, V, Max) ->
    InNeighbors = digraph:in_neighbours(G, V),

    %% Filter out single-fire nodes that are identified by process
    %% identifier only.
    FilterFun = fun({_Id, _Type}) ->
                        true;
                   (_) ->
                        false
                end,
    Neighbors = lists:filter(FilterFun, InNeighbors),

    case Neighbors of
        [] ->
            Max;
        _ ->
            lists:foldl(fun(V1, MaxAcc) ->
                                max(MaxAcc, depth(G, V1, Max + 1)) end, 
                        ?BASE_DEPTH, Neighbors)
    end.
