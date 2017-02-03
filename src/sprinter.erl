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
         tree/0,
         was_connected/0,
         servers/0,
         nodes/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% debug functions
-export([debug_get_tree/2]).

-include("lasp.hrl").

-define(REFRESH_INTERVAL, 20000).
-define(REFRESH_MESSAGE,  refresh).

-define(BUILD_GRAPH_INTERVAL, 20000).
-define(BUILD_GRAPH_MESSAGE,  build_graph).

-define(ARTIFACT_INTERVAL, 20000).
-define(ARTIFACT_MESSAGE,  artifact).

%% State record.
-record(state, {orchestration,
                is_connected,
                was_connected,
                attempted_nodes,
                graph,
                tree,
                servers,
                nodes}).

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

tree() ->
    gen_server:call(?MODULE, tree, infinity).

was_connected() ->
    gen_server:call(?MODULE, was_connected, infinity).

-spec servers() -> {ok, [node()]}.
servers() ->
    gen_server:call(?MODULE, servers, infinity).

-spec nodes() -> {ok, [node()]}.
nodes() ->
    gen_server:call(?MODULE, nodes, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([]) -> {ok, #state{}}.
init([]) ->
    Orchestration = case {os:getenv("MESOS_TASK_ID"),
                          os:getenv("KUBERNETES_PORT")} of
        {Value, false} when is_list(Value) ->
            mesos;
        {false, Value} when is_list(Value) ->
            kubernetes;
        {_, _} ->
            undefined
    end,

    case Orchestration of
        undefined ->
            lager:info("Not using container orchestration; disabling."),
            ok;
        _ ->
            lager:info("Orchestration: ~p", [Orchestration]),

            %% Configure erlcloud.
            S3Host = "s3-us-west-2.amazonaws.com",
            AccessKeyId = os:getenv("AWS_ACCESS_KEY_ID"),
            SecretAccessKey = os:getenv("AWS_SECRET_ACCESS_KEY"),
            erlcloud_s3:configure(AccessKeyId, SecretAccessKey, S3Host),

            %% Create S3 bucket.
            try
                BucketName = bucket_name(),
                lager:info("Creating bucket: ~p", [BucketName]),
                ok = erlcloud_s3:create_bucket(BucketName),
                lager:info("Bucket created.")
            catch
                _:{aws_error, Error} ->
                    lager:info("Bucket creation failed: ~p", [Error]),
                    ok
            end,

            lager:info("S3 bucket creation succeeded."),

            %% Only construct the graph and attempt to repair the graph
            %% from the designated server node.
            case partisan_config:get(tag, client) of
                server ->
                    schedule_build_graph();
                client ->
                    ok;
                undefined ->
                    ok
            end,

            %% All nodes should upload artifacts.
            schedule_artifact_upload(),

            %% All nodes should attempt to refresh the membership.
            schedule_membership_refresh()
    end,

    Servers = case Orchestration of
        undefined ->
            case lasp_config:get(lasp_server, undefined) of
                undefined ->
                    [];
                Server ->
                    [Server]
            end;
        _ ->
            []
    end,

    Nodes = case Orchestration of
        undefined ->
            %% Assumes full membership.
            PeerServiceManager = lasp_config:peer_service_manager(),
            {ok, Members} = PeerServiceManager:members(),
            Members;
        _ ->
            []
    end,

    {ok, #state{nodes=Nodes,
                servers=Servers,
                is_connected=false,
                was_connected=false,
                orchestration=Orchestration,
                attempted_nodes=sets:new(),
                graph=digraph:new(),
                tree=digraph:new()}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.

%% @private
handle_call(nodes, _From, #state{nodes=Nodes}=State) ->
    {reply, {ok, Nodes}, State};

handle_call(servers, _From, #state{servers=Servers}=State) ->
    {reply, {ok, Servers}, State};

handle_call(was_connected, _From, #state{was_connected=WasConnected}=State) ->
    {reply, {ok, WasConnected}, State};

handle_call(graph, _From, #state{graph=Graph}=State) ->
    {Vertices, Edges} = vertices_and_edges(Graph),
    {reply, {ok, {Vertices, Edges}}, State};

handle_call(tree, _From, #state{tree=Tree}=State) ->
    {Vertices, Edges} = vertices_and_edges(Tree),
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
handle_info(?REFRESH_MESSAGE, #state{orchestration=Orchestration,
                                     attempted_nodes=SeenNodes}=State) ->
    Tag = partisan_config:get(tag, client),
    PeerServiceManager = lasp_config:peer_service_manager(),

    Servers = servers(Orchestration),
    lager:info("Found servers: ~p", [Servers]),

    Clients = clients(Orchestration),
    lager:info("Found clients: ~p", [Clients]),

    %% Get list of nodes to connect to: this specialized logic isn't
    %% required when the node count is small, but is required with a
    %% larger node count to ensure the network stabilizes correctly
    %% because HyParView doesn't guarantee graph connectivity: it is
    %% only probabilistic.
    %%
    ToConnectNodes = case {Tag, PeerServiceManager} of
        {_, partisan_default_peer_service_manager} ->
            %% Full connectivity.
            sets:union(Servers, Clients);
        {client, partisan_client_server_peer_service_manager} ->
            %% If we're a client, and we're in client/server mode, then
            %% always connect with the server.
            Servers;
        {server, partisan_client_server_peer_service_manager} ->
            %% If we're a server, and we're in client/server mode, then
            %% always initiate connections with clients.
            Clients;
        {client, partisan_hyparview_peer_service_manager} ->
            %% If we're the server, and we're in HyParView, clients will
            %% ask the server to join the overlay and force outbound
            %% conenctions to the clients.
            Servers;
        {server, partisan_hyparview_peer_service_manager} ->
            %% If we're in HyParView, and we're a client, only ever
            %% do nothing -- force all connection to go through the
            %% server.
            sets:new();
        {Tag, PeerServiceManager} ->
            %% Catch all.
            lager:info("Invalid mode: not connecting to any nodes."),
            lager:info("Tag: ~p; PeerServiceManager: ~p",
                       [Tag, PeerServiceManager]),
            sets:new()
    end,

    lager:info("Attempting to connect: ~p", [ToConnectNodes]),

    %% Attempt to connect nodes that are not connected.
    AttemptedNodes = maybe_connect(ToConnectNodes, SeenNodes),

    ServerNames = node_names(sets:to_list(Servers)),
    ClientNames = node_names(sets:to_list(Clients)),
    Nodes = ServerNames ++ ClientNames,

    schedule_membership_refresh(),

    {noreply, State#state{nodes=Nodes,
                          servers=ServerNames,
                          attempted_nodes=AttemptedNodes}};

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
handle_info(?BUILD_GRAPH_MESSAGE, #state{orchestration=Orchestration,
                                         was_connected=WasConnected0}=State) ->
    _ = lager:info("Beginning graph analysis."),

    %% Get all running nodes, because we need the list of *everything*
    %% to analyze the graph for connectedness.
    Clients = clients(Orchestration),
    Servers = servers(Orchestration),

    ServerNames = node_names(sets:to_list(Servers)),
    ClientNames = node_names(sets:to_list(Clients)),
    Nodes = ServerNames ++ ClientNames,

    %% Build the tree.
    Tree = digraph:new(),

    case lasp_config:get(broadcast, false) of
        true ->
            try
                Root = hd(ServerNames),
                populate_tree(Root, Nodes, Tree)
            catch
                _:_ ->
                    ok
            end;
        false ->
            ok
    end,

    %% Build the graph.
    Graph = digraph:new(),
    Orphaned = populate_graph(Nodes, Graph),

    {SymmetricViews, VisitedNames} = breath_first(node(), Graph, ordsets:new()),
    AllNodesVisited = length(Nodes) == length(VisitedNames),

    Connected = SymmetricViews andalso AllNodesVisited,

    case Connected of
        true ->
            lager:info("Graph is connected!");
        false ->
            lager:info("SymmetricViews ~p", [SymmetricViews]),
            lager:info("Visited ~p from ~p: ~p", [length(VisitedNames), node(), VisitedNames]),
            {ok, ServerMembership} = lasp_peer_service:members(),
            lager:info("Membership (~p) ~p", [length(ServerMembership), ServerMembership]),
            lager:info("Graph is not connected!")
    end,

    WasConnected = Connected orelse WasConnected0,

    case length(Orphaned) of
        0 ->
            ok;
        Length ->
            lager:info("~p isolated nodes: ~p", [Length, Orphaned])
    end,

    schedule_build_graph(),

    {noreply, State#state{is_connected=Connected,
                          was_connected=WasConnected,
                          graph=Graph,
                          tree=Tree}};

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
generate_mesos_nodes(#{<<"app">> := App}) ->
    #{<<"tasks">> := Tasks} = App,
    Nodes = lists:map(fun(Task) ->
                #{<<"host">> := Host,
                  <<"ports">> := [_WebPort, PeerPort]} = Task,
        generate_mesos_node(Host, PeerPort)
        end, Tasks),
    sets:from_list(Nodes).

%% @doc Generate a single Erlang node name.
generate_mesos_node(Host, PeerPort) ->
    Name = "lasp-" ++ integer_to_list(PeerPort) ++ "@" ++ binary_to_list(Host),
    {ok, IPAddress} = inet_parse:address(binary_to_list(Host)),
    Node = {list_to_atom(Name), IPAddress, PeerPort},
    Node.

%% @private
maybe_connect(Nodes, SeenNodes) ->
    %% If this is the first time you've seen the node, attempt to
    %% connect; only attempt to connect once, because node might be
    %% migrated to a passive view of the membership.
    %% If the node is isolated always try to connect.
    {ok, Membership0} = lasp_peer_service:members(),
    Membership1 = Membership0 -- [node()],
    Isolated = length(Membership1) == 0,

    ToConnect = case Isolated of
        true ->
            Nodes;
        false ->
            sets:subtract(Nodes, SeenNodes)
    end,

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
generate_mesos_task_url(Task) ->
    IP = ip(),
    DCOS = dcos(),
    case DCOS of
        "false" ->
          "http://" ++ IP ++ ":8080/v2/apps/" ++ Task ++ "?embed=app.taskStats";
        _ ->
          DCOS ++ "/marathon/v2/apps/" ++ Task ++ "?embed=app.taskStats"
    end.

%% @private
headers(Orchestration) ->
    Token = os:getenv("TOKEN"),

    case Orchestration of
        mesos ->
            [];
        kubernetes ->
            [{"Authorization", "Bearer " ++ Token}];
        _ ->
            []
    end.

%% @private
get_request(Url, DecodeFun, Orchestration) ->
    Headers = headers(Orchestration),
    case httpc:request(get, {Url, Headers}, [], [{body_format, binary}]) of
        {ok, {{_, 200, _}, _, Body}} ->
            {ok, DecodeFun(Body)};
        Other ->
            _ = lager:info("Request failed; ~p", [Other]),
            {error, invalid}
    end.

breath_first(Root, Graph, Visited0) ->
    %% Check if every link is bidirectional
    %% If not, stop traversal
    In = ordsets:from_list(digraph:in_neighbours(Graph, Root)),
    Out = ordsets:from_list(digraph:out_neighbours(Graph, Root)),

    Visited1 = ordsets:union(Visited0, [Root]),

    case In == Out of
        true ->
            {SymmetricViews, VisitedNodes} = ordsets:fold(
                fun(Peer, {SymmetricViews0, VisitedNodes0}) ->
                    {SymmetricViews1, VisitedNodes1} = breath_first(Peer, Graph, VisitedNodes0),
                    {SymmetricViews0 andalso SymmetricViews1, ordsets:union(VisitedNodes0, VisitedNodes1)}
                end,
                {true, Visited1},
                ordsets:subtract(Out, Visited1)
            ),
            {SymmetricViews, ordsets:union(VisitedNodes, Out)};
        false ->
            lager:info("Non symmetric views for node ~p. In ~p; Out ~p", [Root, In, Out]),
            {false, ordsets:new()}
    end.

%% @private
prefix(File) ->
    EvalTimestamp = lasp_config:get(evaluation_timestamp, 0),
    integer_to_list(EvalTimestamp) ++ "/" ++ File.

%% @private
bucket_name() ->
    "lasp-sprinter-metadata".

%% @private
clients_from_marathon(Orchestration) ->
    EvalTimestamp = lasp_config:get(evaluation_timestamp, 0),
    ClientApp = "lasp-client-" ++ integer_to_list(EvalTimestamp),
    app_tasks_from_marathon(ClientApp, Orchestration).

%% @private
servers_from_marathon(Orchestration) ->
    EvalTimestamp = lasp_config:get(evaluation_timestamp, 0),
    ServerApp = "lasp-server-" ++ integer_to_list(EvalTimestamp),
    app_tasks_from_marathon(ServerApp, Orchestration).

%% @private
app_tasks_from_marathon(App, Orchestration) ->
    DecodeFun = fun(Body) -> jsx:decode(Body, [return_maps]) end,

    case get_request(generate_mesos_task_url(App), DecodeFun, Orchestration) of
        {ok, Tasks} ->
            generate_mesos_nodes(Tasks);
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

%% @private
vertices_and_edges(Graph) ->
    Vertices = digraph:vertices(Graph),
    Edges = lists:map(
        fun(Edge) ->
            {_E, V1, V2, _Label} = digraph:edge(Graph, Edge),
            {V1, V2}
        end,
        digraph:edges(Graph)
    ),
    {Vertices, Edges}.

%% @private
node_names([]) ->
    [];
node_names([{Name, _Ip, _Port}|T]) ->
    [Name|node_names(T)].

%% @private
populate_graph(Nodes, Graph) ->
    %% Get bucket name.
    BucketName = bucket_name(),

    lists:foldl(
        fun(Node, OrphanedNodes) ->
            File = prefix(atom_to_list(Node)),
            try
                Result = erlcloud_s3:get_object(BucketName, File),
                Body = proplists:get_value(content, Result, undefined),
                case Body of
                    undefined ->
                        OrphanedNodes;
                    _ ->
                        Membership = binary_to_term(Body),
                        case Membership of
                            [Node] ->
                                add_edges(Node, [], Graph),
                                [Node|OrphanedNodes];
                            _ ->
                                add_edges(Node, Membership, Graph),
                                OrphanedNodes
                        end
                end
            catch
                _:{aws_error, Error} ->
                    add_edges(Node, [], Graph),
                    lager:info("Could not get graph object; ~p", [Error]),
                    OrphanedNodes
            end
        end,
        [],
        Nodes
    ).

%% @private
populate_tree(Root, Nodes, Tree) ->
    DebugTree = debug_get_tree(Root, Nodes),
    lists:foreach(
        fun({Node, Peers}) ->
            case Peers of
                down ->
                    add_edges(Node, [], Tree);
                {Eager, _Lazy} ->
                    add_edges(Node, Eager, Tree)
            end
        end,
        DebugTree
    ).

%% @private
add_edges(Name, Membership, Graph) ->
    %% Add node to graph.
    digraph:add_vertex(Graph, Name),

    lists:foldl(
        fun(N, _) ->
            %% Add node to graph.
            digraph:add_vertex(Graph, N),

            %% Add edge to graph.
            digraph:add_edge(Graph, Name, N)
        end,
        Graph,
        Membership
    ).

-spec debug_get_tree(node(), [node()]) ->
                            [{node(), {ordsets:ordset(node()), ordsets:ordset(node())}}].
debug_get_tree(Root, Nodes) ->
    [begin
         Peers = try plumtree_broadcast:debug_get_peers(Node, Root, 5000)
                 catch _:Error ->
                           lager:info("Call to node ~p to get root tree ~p failed: ~p", [Node, Root, Error]),
                           down
                 end,
         {Node, Peers}
     end || Node <- Nodes].

%% @private
clients(Orchestration) ->
    case Orchestration of
        kubernetes ->
            clients_from_kubernetes(Orchestration);
        mesos ->
            clients_from_marathon(Orchestration)
    end.

%% @private
servers(Orchestration) ->
    case Orchestration of
        kubernetes ->
            servers_from_kubernetes(Orchestration);
        mesos ->
            servers_from_marathon(Orchestration)
    end.

%% @private
clients_from_kubernetes(Orchestration) ->
    EvalTimestamp = lasp_config:get(evaluation_timestamp, 0),
    LabelSelector = "tag%3Dclient,evaluation-timestamp%3D" ++ integer_to_list(EvalTimestamp),
    pods_from_kubernetes(LabelSelector, Orchestration).

%% @private
servers_from_kubernetes(Orchestration) ->
    EvalTimestamp = lasp_config:get(evaluation_timestamp, 0),
    LabelSelector = "tag%3Dserver,evaluation-timestamp%3D" ++ integer_to_list(EvalTimestamp),
    pods_from_kubernetes(LabelSelector, Orchestration).

%% @private
pods_from_kubernetes(LabelSelector, Orchestration) ->
    DecodeFun = fun(Body) -> jsx:decode(Body, [return_maps]) end,

    case get_request(generate_pods_url(LabelSelector), DecodeFun, Orchestration) of
        {ok, PodList} ->
            generate_pod_nodes(PodList);
        Error ->
            _ = lager:info("Invalid Marathon response: ~p", [Error]),
            sets:new()
    end.

%% @private
generate_pods_url(LabelSelector) ->
    APIServer = os:getenv("APISERVER"),
    APIServer ++ "/api/v1/pods?labelSelector=" ++ LabelSelector.

%% @private
generate_pod_nodes(#{<<"items">> := Items}) ->
    case Items of
        null ->
            sets:new();
        _ ->
            Nodes = lists:map(fun(Item) ->
                        #{<<"metadata">> := Metadata} = Item,
                        #{<<"name">> := Name} = Metadata,
                        #{<<"status">> := Status} = Item,
                        #{<<"podIP">> := PodIP} = Status,
                        generate_pod_node(Name, PodIP)
                end, Items),
            sets:from_list(Nodes)
    end.

%% @private
generate_pod_node(Name, Host) ->
    {ok, IPAddress} = inet_parse:address(Host),
    Port = list_to_integer(os:getenv("LASP_SERVICE_PORT_PEER")),
    {list_to_atom(binary_to_list(Name) ++ "@" ++ Host), IPAddress, Port}.
