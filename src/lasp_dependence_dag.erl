-module(lasp_dependence_dag).

-include("lasp.hrl").
-behaviour(gen_server).

%% API
-export([start_link/0,
         is_loop/2,
         add_edges/3,
         add_vertex/1,
         add_vertices/1]).

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
-spec is_loop(list(id()), id()) -> boolean().
is_loop(Src, Dst) ->
    gen_server:call(?MODULE, {is_loop, Src, Dst}, infinity).

%% @doc For all V in Src, create an edge from V to Dst labelled with Pid.
%%
%%      Returns error if it couldn't create some of the edges,
%%      either because it formed a loop, or because some of the
%%      vertices weren't in the graph.
%%
-spec add_edges(list(id()), id(), pid()) -> ok | error.
add_edges(Src, Dst, Pid) ->
    gen_server:call(?MODULE, {add_edges, Src, Dst, Pid}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @doc Initialize state.
init([]) ->
    {ok, #state{dag=digraph:new([acyclic])}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.

handle_call({add_vertex, V}, _From, #state{dag=Dag}=State) ->
    digraph:add_vertex(Dag, V),
    {reply, ok, State};

handle_call({add_vertices, Vs}, _From, #state{dag=Dag}=State) ->
    [digraph:add_vertex(Dag, V) || V <- Vs],
    {reply, ok, State};

%% @doc Check if linking the given vertices will form a loop.
%%
%%      Naive approach first: trying to link the same vertices
%%
%%      Second approach: let the digraph module figure it out,
%%      creating an edge between the vertices and then catching
%%      the {error, {bad_edge, _}} error.
%%
%%      As this second approach creates edges, we delete them all
%%      after we're done (we don't want edges without an associated
%%      pid).
%%
%%      We want to check this before spawning a lasp process, otherwise
%%      an infinite loop can be created if the vertices form a loop.
%%
handle_call({is_loop, From, To}, _From, #state{dag=Dag}=State) ->
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
