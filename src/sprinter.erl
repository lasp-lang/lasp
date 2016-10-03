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

-module(sprinter).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).

%% API
-export([start_link/0,
         start_link/1,
         graph/0,
         was_connected/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("lasp.hrl").

-define(REFRESH_INTERVAL, 20000).
-define(REFRESH_MESSAGE,  refresh).

-define(BUILD_GRAPH_INTERVAL, 20000).
-define(BUILD_GRAPH_MESSAGE,  build_graph).

-define(ARTIFACT_INTERVAL, 20000).
-define(ARTIFACT_MESSAGE,  artifact).

%% State record.
-record(state, {is_connected, was_connected, attempted_nodes, graph}).

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

graph() ->
    gen_server:call(?MODULE, graph, infinity).

was_connected() ->
    gen_server:call(?MODULE, was_connected, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([]) -> {ok, #state{}}.
init([]) ->
    %% Don't start the timer if we're not running in Mesos.
    case os:getenv("MESOS_TASK_ID") of
        false ->
            lager:info("Not running in Mesos; disabling Marathon service."),
            ok;
        _ ->
            %% Configure erlcloud.
            S3Host = "s3.amazonaws.com",
            AccessKeyId = os:getenv("AWS_ACCESS_KEY_ID"),
            SecretAccessKey = os:getenv("AWS_SECRET_ACCESS_KEY"),
            erlcloud_s3:configure(AccessKeyId, SecretAccessKey, S3Host),

            %% Create S3 bucket.
            try
                BucketName = bucket_name(),
                lager:info("Creating bucket: ~p", [BucketName]),
                ok = erlcloud_s3:create_bucket(BucketName)
            catch
                _:{aws_error, Error} ->
                    lager:info("Bucket creation failed: ~p", [Error]),
                    ok
            end,

            %% Only construct the graph and attempt to repair the graph
            %% from the designated server node.
            case partisan_config:get(tag, client) of
                server ->
                    schedule_build_graph();
                client ->
                    ok
            end,

            %% All nodes should upload artifacts.
            schedule_artifact_upload(),

            %% All nodes should attempt to refresh the membership.
            schedule_membership_refresh()
    end,
    {ok, #state{is_connected=false,
                was_connected=false,
                attempted_nodes=sets:new(),
                graph=digraph:new()}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.

%% @private
handle_call(was_connected, _From, #state{was_connected=WasConnected}=State) ->
    {reply, {ok, WasConnected}, State};

handle_call(graph, _From, #state{graph=Graph}=State) ->
    Vertices = digraph:vertices(Graph),
    Edges = lists:map(fun(Edge) ->
                      {_E, V1, V2, _Label} = digraph:edge(Graph, Edge),
                      {V1, V2}
              end, digraph:edges(Graph)),
    {reply, {ok, {Vertices, Edges}}, State};

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
handle_info(?REFRESH_MESSAGE, #state{attempted_nodes=SeenNodes}=State) ->
    Tag = partisan_config:get(tag, client),
    PeerServiceManager = lasp_config:peer_service_manager(),

    %% Get list of nodes to connect to: this specialized logic isn't
    %% required when the node count is small, but is required with a
    %% larger node count to ensure the network stabilizes correctly
    %% because HyParView doesn't guarantee graph connectivity: it is
    %% only probabilistic.
    %%
    ToConnectNodes = case {Tag, PeerServiceManager} of
        {client, partisan_client_server_peer_service_manager} ->
            %% If we're a client, and we're in client/server mode, then
            %% always connect with the server.
            servers_from_marathon();
        {server, partisan_client_server_peer_service_manager} ->
            %% If we're a server, and we're in client/server mode, then
            %% always initiate connections with clients.
            clients_from_marathon();
        {client, partisan_hyparview_peer_service_manager} ->
            %% If we're the server, and we're in HyParView, clients will
            %% ask the server to join the overlay and force outbound
            %% conenctions to the clients.
            servers_from_marathon();
        {server, partisan_hyparview_peer_service_manager} ->
            %% If we're in HyParView, and we're a client, only ever
            %% do nothing -- force all connection to go through the
            %% server.
            sets:new()
    end,

    %% Attempt to connect nodes that are not connected.
    AttemptedNodes = maybe_connect(ToConnectNodes, SeenNodes),

    schedule_membership_refresh(),

    {noreply, State#state{attempted_nodes=AttemptedNodes}};
handle_info(?ARTIFACT_MESSAGE, State) ->
    %% Get bucket name.
    BucketName = bucket_name(),

    %% Get current membership.
    {ok, Nodes} = lasp_peer_service:members(),

    %% Store membership.
    Node = prefix(atom_to_list(node())),
    Membership = term_to_binary(Nodes),
    try
        erlcloud_s3:put_object(BucketName, Node, Membership)
    catch
        _:{aws_error, Error} ->
            lager:info("Could not upload artifact: ~p", [Error])
    end,

    schedule_artifact_upload(),

    {noreply, State};
handle_info(?BUILD_GRAPH_MESSAGE, #state{was_connected=WasConnected0}=State) ->
    _ = lager:info("Beginning graph analysis."),

    %% Get all running nodes, because we need the list of *everything*
    %% to analyze the graph for connectedness.
    ClientsFromMarathon = clients_from_marathon(),
    ServersFromMarathon = servers_from_marathon(),
    Nodes = sets:union(ClientsFromMarathon, ServersFromMarathon),

    %% Build the graph.
    Graph = digraph:new(),

    %% Get bucket name.
    BucketName = bucket_name(),

    GraphFun = fun({N, _, _}=Peer, OrphanedNodes) ->
                       Node = prefix(atom_to_list(N)),
                       try
                           Result = erlcloud_s3:get_object(BucketName, Node),
                           Body = proplists:get_value(content, Result, undefined),
                           case Body of
                               undefined ->
                                   OrphanedNodes;
                               _ ->
                                   Membership = binary_to_term(Body),
                                   case Membership of
                                       [N] ->
                                           populate_graph(N, [], Graph),
                                           [Peer|OrphanedNodes];
                                       _ ->
                                           populate_graph(N, Membership, Graph),
                                           OrphanedNodes
                                   end
                           end
                       catch
                           _:{aws_error, Error} ->
                               populate_graph(N, [], Graph),
                               lager:info("Could not get graph object; ~p", [Error]),
                               OrphanedNodes
                       end
               end,
    Orphaned = sets:fold(GraphFun, [], Nodes),

    %% Verify connectedness.
    ConnectedFun = fun({Name, _, _}, Result0) ->
                           {Ns, Result} = sets:fold(fun({N, _, _}, {Disconnected, Result1}) ->
                                           Path = digraph:get_short_path(Graph, Name, N),
                                           case Path of
                                               false ->
                                                   {[N|Disconnected], Result1 andalso false};
                                               _ ->
                                                   {Disconnected, Result1 andalso true}
                                           end
                                      end, {[], Result0}, Nodes),
                           case Ns of
                               [] ->
                                   ok;
                               _ ->
                                lager:info("Node ~p can not find shortest path to: ~p", [Name, Ns])
                           end,
                           Result
                 end,
    Connected = sets:fold(ConnectedFun, true, Nodes)
        andalso sets:size(ClientsFromMarathon) == lasp_config:get(client_number, 3),

    IsConnected = case Connected of
        true ->
            lager:info("Graph is connected!"),
            true;
        false ->
            lager:info("Graph is not connected!"),
            false
    end,

    WasConnected = IsConnected orelse WasConnected0,

    case length(Orphaned) of
        0 ->
            ok;
        Length ->
            lager:info("~p isolated nodes: ~p", [Length, Orphaned])
    end,

    schedule_build_graph(),

    {noreply, State#state{is_connected=IsConnected,
                          was_connected=WasConnected,
                          graph=Graph}};

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
    sets:fold(fun(Node, Acc) -> [connect(Node) | Acc] end, [], ToConnect),

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
headers() ->
    case dcos() of
        "false" ->
            [];
        _ ->
            Token = os:getenv("TOKEN", "undefined"),
            [{"Authorization", "token=" ++ Token}]
    end.

%% @private
get_request(Url, DecodeFun) ->
    Headers = headers(),
    case httpc:request(get, {Url, Headers}, [], [{body_format, binary}]) of
        {ok, {{_, 200, _}, _, Body}} ->
            {ok, DecodeFun(Body)};
        Other ->
            _ = lager:info("Request failed; ~p", [Other]),
            {error, invalid}
    end.

%% @private
populate_graph(Name, Membership, Graph) ->
    %% Add node to graph.
    digraph:add_vertex(Graph, Name),

    lists:foldl(fun(N, _) ->
                        %% Add node to graph.
                        digraph:add_vertex(Graph, N),

                        %% Add edge to graph.
                        digraph:add_edge(Graph, Name, N)
                end, Graph, Membership).

%% @private
prefix(File) ->
    EvalTimestamp = lasp_config:get(evaluation_timestamp, 0),
    integer_to_list(EvalTimestamp) ++ "/" ++ File.

%% @private
bucket_name() ->
    "lasp-sprinter-metadata".

%% @private
clients_from_marathon() ->
    EvalTimestamp = lasp_config:get(evaluation_timestamp, 0),
    ClientApp = "lasp-client-" ++ integer_to_list(EvalTimestamp),
    app_tasks_from_marathon(ClientApp).

%% @private
servers_from_marathon() ->
    EvalTimestamp = lasp_config:get(evaluation_timestamp, 0),
    ServerApp = "lasp-server-" ++ integer_to_list(EvalTimestamp),
    app_tasks_from_marathon(ServerApp).

%% @private
app_tasks_from_marathon(App) ->
    DecodeFun = fun(Body) -> jsx:decode(Body, [return_maps]) end,

    case get_request(generate_task_url(App), DecodeFun) of
        {ok, Tasks} ->
            generate_nodes(Tasks);
        Error ->
            _ = lager:info("Invalid Marathon response: ~p", [Error]),
            sets:new()
    end.

%% @private
schedule_build_graph() ->
    %% Add random jitter.
    Jitter = rand_compat:uniform(?BUILD_GRAPH_INTERVAL),
    timer:send_after(?BUILD_GRAPH_INTERVAL + Jitter, ?BUILD_GRAPH_MESSAGE).

%% @private
schedule_artifact_upload() ->
    %% Add random jitter.
    Jitter = rand_compat:uniform(?ARTIFACT_INTERVAL),
    timer:send_after(?ARTIFACT_INTERVAL + Jitter, ?ARTIFACT_MESSAGE).

%% @private
schedule_membership_refresh() ->
    %% Add random jitter.
    Jitter = rand_compat:uniform(?REFRESH_INTERVAL),
    timer:send_after(?REFRESH_INTERVAL + Jitter, ?REFRESH_MESSAGE).

