%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Christopher Meiklejohn.  All Rights Reserved.
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

%% @doc Read configuration information from Marathon and auto-cluster
%%      Erlang nodes based on this.

-module(lasp_marathon_peer_refresh_service).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).

%% API
-export([start_link/0,
         start_link/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("lasp.hrl").

-define(REFRESH_INTERVAL, 1000).
-define(REFRESH_MESSAGE,  refresh).

-define(NODES_INTERVAL, 20000).
-define(NODES_MESSAGE,  nodes).

-define(GRAPH_INTERVAL, 20000).
-define(GRAPH_MESSAGE,  graph).

%% State record.
-record(state, {nodes}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Same as start_link([]).
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    start_link([]).

%% @doc Start and link to calling process.
-spec start_link(list())-> {ok, pid()} | ignore | {error, term()}.
start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([]) -> {ok, #state{}}.
init([]) ->
    %% Don't start the timer if we're not running in Mesos.
    case os:getenv("MESOS_TASK_ID") of
        false ->
            ok;
        _ ->
            %% Stall messages; Plumtree has a race on startup, again.
            timer:send_after(?NODES_INTERVAL, ?NODES_MESSAGE),
            timer:send_after(?GRAPH_INTERVAL, ?GRAPH_MESSAGE),
            timer:send_after(?REFRESH_INTERVAL, ?REFRESH_MESSAGE)
    end,
    {ok, #state{nodes=sets:new()}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.

%% @private
handle_call(Msg, _From, State) ->
    _ = lager:warning("Unhandled messages: ~p", [Msg]),
    {reply, ok, State}.

%% @private
-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
handle_cast(Msg, State) ->
    _ = lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info(?REFRESH_MESSAGE, #state{nodes=SeenNodes}=State) ->
    timer:send_after(?REFRESH_INTERVAL, ?REFRESH_MESSAGE),

    %% Randomly get information from the server nodes and the
    %% regular nodes.
    %%
    Task = case rand_compat:uniform(10) rem 2 == 0 of
        true ->
            "lasp-server";
        false ->
            "lasp-client"
    end,

    %% Generate URL.
    Url = generate_task_url(Task),

    %% Generate decode function.
    DecodeFun = fun(Body) ->
                        jsx:decode(Body, [return_maps])
                end,

    %% Return list of nodes.
    Nodes = case request(get, Url, DecodeFun) of
        {ok, Response} ->
            generate_nodes(Response);
        Other ->
            _ = lager:info("Invalid Marathon response: ~p", [Other]),
            SeenNodes
    end,

    %% Attempt to connect nodes that are not connected.
    ConnectedNodes = maybe_connect(Nodes, SeenNodes),

    {noreply, State#state{nodes=ConnectedNodes}};
handle_info(?NODES_MESSAGE, State) ->
    {ok, Nodes} = lasp_peer_service:members(),
    _ = lager:info("Currently connected nodes via peer service: ~p", [Nodes]),
    timer:send_after(?NODES_INTERVAL, ?NODES_MESSAGE),
    {noreply, State};
handle_info(?GRAPH_MESSAGE, #state{nodes=Nodes}=State) ->
    GraphFun = fun(Node, Graph) ->
                    Url = generate_artifact_url(Node),
                    DecodeFun = fun(Body) -> binary_to_term(Body) end,
                    case request(get, Url, DecodeFun) of
                        {ok, Membership} ->
                            populate_graph(Node, Membership, Graph);
                        Other ->
                            _ = lager:info("Invalid Marathon response: ~p", [Other]),
                            Graph
                    end
               end,
    sets:fold(GraphFun, digraph:new(), Nodes),
    timer:send_after(?GRAPH_INTERVAL, ?GRAPH_MESSAGE),
    {noreply, State};
handle_info(Msg, State) ->
    _ = lager:warning("Unhandled messages: ~p", [Msg]),
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

%% @doc Generate a list of Erlang node names.
generate_nodes(#{<<"app">> := App}) ->
    #{<<"tasks">> := Tasks} = App,
    Nodes = lists:map(fun(Task) ->
                #{<<"host">> := Host,
                  <<"ports">> := [_WebPort, PeerPort]} = Task,
        generate_node(Host, PeerPort)
        end, Tasks),
    sets:from_list(Nodes).

%% @doc Generate a single Erlang node name.
generate_node(Host, PeerPort) ->
    Name = "lasp-" ++ integer_to_list(PeerPort) ++ "@" ++ binary_to_list(Host),
    {ok, IPAddress} = inet_parse:address(binary_to_list(Host)),
    Node = {list_to_atom(Name), IPAddress, PeerPort},
    Node.

%% @private
maybe_connect(Nodes, SeenNodes) ->
    %% If this is the first time you've seen the node, attempt to
    %% connect; only attempt to connect once, because node might be
    %% migrated to a passive view of the membership.
    %%
    ToConnect = sets:subtract(Nodes, SeenNodes),

    %% Attempt connection to any new nodes.
    sets:fold(fun(Node, Acc) ->
                      [connect(Node) | Acc]
              end, [], ToConnect),

    %% Return list of seen nodes with the new node.
    sets:union(Nodes, SeenNodes).

%% @private
connect(Node) ->
    lasp_peer_service:join(Node).

%% @private
dcos() ->
    os:getenv("DCOS", "false").

%% @private
ip() ->
    os:getenv("IP", "127.0.0.1").

%% @private
generate_artifact_url({Name, _, _}) ->
    IP = ip(),
    DCOS = dcos(),
    Filename = atom_to_list(Name),
    case DCOS of
        "false" ->
          "http://" ++ IP ++ ":8080/v2/artifacts/" ++ Filename;
        _ ->
          DCOS ++ "/marathon/v2/artifacts/" ++ Filename
    end.

%% @private
generate_task_url(Task) ->
    IP = ip(),
    DCOS = dcos(),
    case DCOS of
        "false" ->
          "http://" ++ IP ++ ":8080/v2/apps/" ++ Task ++ "?embed=app.taskStats";
        _ ->
          DCOS ++ "/marathon/v2/apps/" ++ Task ++ "?embed=app.taskStats"
    end.

%% @private
request(Type, Url, DecodeFun) ->
    DCOS = dcos(),
    Headers = case DCOS of
                "false" ->
                    [];
                _ ->
                    Token = os:getenv("TOKEN", "undefined"),
                    [{"Authorization", "token=" ++ Token}]
    end,
    case httpc:request(Type, {Url, Headers}, [], [{body_format, binary}]) of
        {ok, {{_, 200, _}, _, Body}} ->
            {ok, DecodeFun(Body)};
        Other ->
            _ = lager:info("Request failed; ~p", [Other]),
            {error, invalid}
    end.

%% @private
populate_graph({Name, _, _}, Membership, Graph) ->
    %% Add node to graph.
    digraph:add_vertex(Graph, Name),

    lists:foldl(fun({N, _, _}, _) ->

                        %% Add node to graph.
                        digraph:add_vertex(Graph, N),

                        %% Add edge to graph.
                        digraph:add_edge(Graph, Name, N)

                end, Graph, Membership).

