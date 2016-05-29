-module(lasp_dependence_dag).

-include("lasp.hrl").
-behaviour(gen_server).

%% API
-export([start_link/0,
         will_form_cycle/2,
         add_edges/3,
         add_vertex/1,
         add_vertices/1]).

%% Test
-export([n_vertices/0,
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

-record(state, {dag :: digraph:graph()}).

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
-spec add_edges(list(id()), id(), pid()) -> ok | error.
add_edges(Src, Dst, Pid) ->
    gen_server:call(?MODULE, {add_edges, Src, Dst, Pid}, infinity).

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

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @doc Initialize state.
init([]) ->
    {ok, #state{dag=digraph:new([acyclic])}}.

%% @private
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

%% @doc Check if linking the given vertices will introduce a cycle in the graph.
%%
%%      Naive approach first: see if To is a member of From
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
handle_call({add_edges, Src, Dst, Pid}, _From, #state{dag=Dag}=State) ->
    %% @todo Add metadata and duplicate checking.
    Status = [digraph:add_edge(Dag, V, Dst, Pid) || V <- Src],
    R = case lists:any(fun is_graph_error/1, Status) of
        false -> ok;
        true -> error
    end,
    {reply, R, State}.

%% @private
-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
handle_cast(_Request, State) ->
    {noreply, State}.

%% @private
-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info(_Info, State) ->
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
