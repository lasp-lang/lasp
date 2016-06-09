-module(lasp_dependence_dag).

-include("lasp.hrl").
-behaviour(gen_server).

%% API
-export([start_link/0,
         will_form_cycle/2,
         add_edges/6,
         add_vertex/1,
         add_vertices/1]).

%% Utility
-export([to_dot/0,
         export_dot/1]).

%% Test
%% @todo Only export on test.
-export([n_vertices/0,
         merge_unary_edges/2,
         process_map/0,
         n_edges/0,
         out_degree/1,
         in_degree/1,
         out_edges/1,
         in_edges/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%%%===================================================================
%%% Type definitions
%%%===================================================================

%% We store a mapping Pid -> [{parent_node, child_node}] to
%% find the edge labeled with it without traversing the graph.
%%
%% This is useful when the Pid of a lasp process changes
%% (because it gets restarted or it just terminates), as it
%% lets us quickly delete those edges.
-type process_map() :: dict:dict(pid(), {id(), id()}).

-record(state, {dag :: digraph:graph(),
                process_map :: process_map()}).

%% We store the function metadata as the edge label.
-record(edge_label, {pid :: pid(),
                    read :: function(),
                    transform :: function(),
                    write :: function()}).

%% Return type of digraph:edge/2
-type edge() :: {digraph:edge(),
                 digraph:vertex(),
                 digraph:vertex(),
                 #edge_label{}}.

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec add_vertex(id()) -> ok.
add_vertex(V) ->
    gen_server:call(?MODULE, {add_vertex, V}, infinity).

-spec add_vertices(list(id())) -> ok.
add_vertices([]) ->
    ok;

add_vertices(Vs) ->
    gen_server:call(?MODULE, {add_vertices, Vs}, infinity).

%% @doc Check if linking the given vertices will form a loop.
-spec will_form_cycle(list(id()), id()) -> boolean().
will_form_cycle(Src, Dst) ->
    gen_server:call(?MODULE, {will_form_cycle, Src, Dst}, infinity).

%% @doc For all V in Src, create an edge from V to Dst labelled with Pid.
%%
%%      Returns error if it couldn't create some of the edges,
%%      either because it formed a loop, or because some of the
%%      vertices weren't in the graph.
%%
-spec add_edges(list(id()), id(), pid(), list({id(), function()}), function(), {id(), function()}) -> ok | error.
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

merge_unary_edges(Src, Dst) ->
    gen_server:call(?MODULE, {merge_unary_edges, Src, Dst}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @doc Initialize state.
init([]) ->
    {ok, #state{dag=digraph:new([acyclic]),
                process_map=dict:new()}}.

-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.

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

handle_call({add_vertex, V}, _From, #state{dag=Dag}=State) ->
    digraph:add_vertex(Dag, V),
    {reply, ok, State};

handle_call({add_vertices, Vs}, _From, #state{dag=Dag}=State) ->
    [digraph:add_vertex(Dag, V) || V <- Vs],
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

%% @doc Check if linking the given vertices will introduce a cycle in the graph.
%%
%%      Naive approach first: check if To is a member of From
%%
%%      Second approach: let the digraph module figure it out,
%%      as digraph:add_edge/3 will return {error, {bad_edge, _}}.
%%
%%      As this second approach creates edges, we delete them all
%%      after we're done (we don't want edges without an associated
%%      pid).
%%
%%      We want to check this before spawning a lasp process, otherwise
%%      an infinite loop can be created if the vertices form a loop.
%%
handle_call({will_form_cycle, From, To}, _From, #state{dag=Dag}=State) ->
    Response = case lists:member(To, From) of
        true -> true;
        false ->
            Status = [digraph:add_edge(Dag, F, To) || F <- From],
            {Ok, Filtered} = case lists:any(fun is_edge_error/1, Status) of
                false -> {false, Status};
                true ->
                    {true, lists:filter(fun(X) ->
                        not is_edge_error(X)
                    end, Status)}
            end,
            digraph:del_edges(Dag, Filtered),
            Ok
    end,
    {reply, Response, State};

%% @doc For all V in Src, create an edge from V to Dst labelled with Pid.
%%
%%      We monitor all edge Pids to know when they die or get restarted.
%%
handle_call({add_edges, Src, Dst, Pid, ReadFuns, TransFun, {Dst, WriteFun}},
            _From, #state{dag=Dag, process_map=Pm}=State) ->

    %% For all V in Src, make edge (V, Dst) with label {Pid, Read, Trans, Write}
    %% (where {Id, Read} = ReadFuns s.t. Id = V)
    Status = lists:map(fun(V) ->
        Read = lists:nth(1, [ReadF || {Id, ReadF} <- ReadFuns, Id =:= V]),
        digraph:add_edge(Dag, V, Dst, #edge_label{pid=Pid,
                                                  read=Read,
                                                  transform=TransFun,
                                                  write=WriteFun})
    end, Src),
    {R, St} = case lists:any(fun is_graph_error/1, Status) of
        true -> {error, State};
        false ->
            erlang:monitor(process, Pid),

            %% For all V in Src, append Pid -> {V, Dst}
            %% in the process map.
            ProcessMap = lists:foldl(fun(El, D) ->
                dict:append(Pid, {El, Dst}, D)
            end, Pm, Src),

            {ok, State#state{process_map=ProcessMap}}
    end,
    {reply, R, St};

handle_call({merge_unary_edges, Src, Dst}, _From, #state{dag=Dag}=State) ->
    merge_unary(Src, Dst, Dag),
    {reply, ok, State}.

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
handle_info({'DOWN', _, process, Pid, _Reason}, #state{dag=Dag, process_map=PM}=State) ->
    {ok, Edges} = dict:find(Pid, PM),
    NewDag = lists:foldl(fun({F, T}, G) ->
        delete_with_pid(G, F, T, Pid)
    end, Dag, Edges),
    {noreply, State#state{dag=NewDag, process_map=dict:erase(Pid, PM)}};

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

is_graph_error({error, _}) ->
    true;

is_graph_error(_) ->
    false.

is_edge_error({error, {bad_edge, _}}) ->
    true;

is_edge_error(_) ->
    false.

%% @doc Delete all edges between Src and Dst with the given pid..
-spec delete_with_pid(digraph:graph(), id(), id(), term()) -> digraph:graph().
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
                       digraph:vertex(), digraph:vertex()) -> list(edge()).

get_direct_edges(G, V1, V2) ->
    lists:flatmap(fun(Ed) ->
        case digraph:edge(G, Ed) of
            {_, _, To, _}=E when To =:= V2 -> [E];
            _ -> []
        end
    end, digraph:out_edges(G, V1)).

%% @doc Direct edges linking V1 and V2 that represent a process with an arity.
-spec get_direct_edges_with_arity(digraph:graph(), id(), id(), pos_integer()) -> [edge()].
get_direct_edges_with_arity(G, V1, V2, Arity) ->
    lists:filter(fun({_, _, _, L}) ->
        fun_arity(L#edge_label.transform) =:= Arity
    end, get_direct_edges(G, V1, V2)).

%% @doc Collect all transform functions and associated Pids from a list of edges.
-spec collect_transform_functions([edge()]) -> {[function()], [pid()]}.
collect_transform_functions(Edges) ->
    lists:foldl(fun({_, _, _, Label}, {Fs, Pids}) ->
        {[Label#edge_label.transform | Fs], [Label#edge_label.pid | Pids]}
    end, {[], []}, Edges).

%% @doc Merge all edges between Src and Dst into one single edge.
%%
%%      For each edge connecting Src and Dst that represents a
%%      function of arity 1, collect all transforming functions
%%      into a new process that represents the union of all of them.
%%
-spec merge_unary(id(), id(), digraph:graph()) -> ok | pid().
merge_unary({_, orset}=Src, Dst, G) ->
    Edges = get_direct_edges_with_arity(G, Src, Dst, 1),
    union_unary(Edges);

merge_unary(_, _, _) ->
    ok.

%% @doc Given a list of edges, merge them together into a single one.
%%
%%      Collect all transform functions into a new one that performs
%%      the union of all results. Then kill all old Pids and start a
%%      new lasp process with the mentioned function.
%%
%%      Note: assumes all read and write functions are the same in all
%%      edges.
%%
%%      @todo add support for deltas (see map:core/6)
%%
-spec union_unary([edge()]) -> ok | pid().
union_unary([{_, Src, Dst, L}, _ | _]=Edges) ->
    %% If we have less than 2 edges, we are already optimized.
    %%
    %% Since this function merges all edges representing unary
    %% functions, the optimization should only execute when we
    %% have more than 2 edges.

    Read = {Src, L#edge_label.read},

    %% Gather all transform functions with their associated Pids.
    {Transforms, Pids} = collect_transform_functions(Edges),

    Write = {Dst, L#edge_label.write},

    NewTransform = fun(V) ->
        orset_union_list(lists:map(fun(F) -> F(V) end, Transforms))
    end,

    kill_then_start(Pids, [[Read], NewTransform, Write]);

union_unary(_) -> ok.

%% @doc Perform the orset union of all the elements in the list.
orset_union_list([H | T]) ->
    lists:foldl(fun(R, Acc) ->
        state_orset_ext:union(R, Acc)
    end, H, T).

%% @doc Kill all given Pids and then start a new lasp process.
%%
%%      Old Pids that are killed will remove themselves from
%%      the dag, and the new one will register automatically.
%%
%%      Execute in a new process, otherwise we block.
%%
kill_then_start(Pids, Args) ->
    spawn(fun() ->
        lists:foreach(fun(P) ->
            ok = lasp_process_sup:terminate_child(lasp_process_sup, P)
        end, Pids),
        lasp_process:start_dag_link(Args)
    end).

-spec fun_arity(function()) -> non_neg_integer().
fun_arity(F) when is_function(F) ->
    {arity, N} = lists:keyfind(arity, 1, erlang:fun_info(F)),
    N;

fun_arity(_) -> 0.

to_dot(Graph) ->
    case digraph_utils:topsort(Graph) of
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
    erlang:integer_to_list(erlang:phash2(Id)).
