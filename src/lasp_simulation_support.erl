%% ------------------------------------------------------------------
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
%%

-module(lasp_simulation_support).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("lasp.hrl").

-export([run/3]).

run(Case, Config, Options) ->
    ClientNumber = lasp_config:get(client_number, 3),
    NodeNames = node_list(ClientNumber),

    ct:pal("Running ~p with nodes ~p", [Case, NodeNames]),

    lists:foreach(
        fun(_EvalNumber) ->
            Server = start(
              Case,
              Config,
              [{client_number, ClientNumber},
               {nodes, NodeNames},
               {evaluation_timestamp, timestamp()} | Options]
            ),
            wait_for_completion(Server),
            stop(NodeNames)
        end,
        lists:seq(1, ?EVAL_NUMBER)
    ).

%% @private
start(_Case, _Config, Options) ->
    %% Launch distribution for the test runner.
    ct:pal("Launching Erlang distribution..."),

    os:cmd(os:find_executable("epmd") ++ " -daemon"),
    {ok, Hostname} = inet:gethostname(),
    case net_kernel:start([list_to_atom("runner@" ++ Hostname), shortnames]) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok
    end,

    %% Load sasl.
    application:load(sasl),
    ok = application:set_env(sasl,
                             sasl_error_logger,
                             false),
    application:start(sasl),

    %% Load lager.
    {ok, _} = application:ensure_all_started(lager),

    %% Start all three nodes.
    InitializerFun = fun(Name) ->
                            ct:pal("Starting node: ~p", [Name]),

                            NodeConfig = [{monitor_master, true},
                                          {startup_functions, [{code, set_path, [codepath()]}]}],

                            case ct_slave:start(Name, NodeConfig) of
                                {ok, Node} ->
                                    Node;
                                Error ->
                                    ct:fail(Error)
                            end
                     end,

    NodeNames = proplists:get_value(nodes, Options),
    [Server | _] = Nodes = lists:map(InitializerFun, NodeNames),

    %% Load Lasp on all of the nodes.
    LoaderFun = fun(Node) ->
                            ct:pal("Loading lasp on node: ~p", [Node]),

                            PrivDir = code:priv_dir(?APP),
                            NodeDir = filename:join([PrivDir, "lager", Node]),

                            %% Manually force sasl loading, and disable the logger.
                            ok = rpc:call(Node, application, load, [sasl]),
                            ok = rpc:call(Node, application, set_env,
                                          [sasl, sasl_error_logger, false]),
                            ok = rpc:call(Node, application, start, [sasl]),

                            ok = rpc:call(Node, application, load, [plumtree]),
                            ok = rpc:call(Node, application, load, [partisan]),
                            ok = rpc:call(Node, application, load, [lager]),
                            ok = rpc:call(Node, application, load, [lasp]),
                            ok = rpc:call(Node, application, set_env, [sasl,
                                                                       sasl_error_logger,
                                                                       false]),
                            ok = rpc:call(Node, application, set_env, [lasp,
                                                                       instrumentation,
                                                                       false]),
                            ok = rpc:call(Node, application, set_env, [lager,
                                                                       log_root,
                                                                       NodeDir]),
                            ok = rpc:call(Node, application, set_env, [plumtree,
                                                                       plumtree_data_dir,
                                                                       NodeDir]),
                            ok = rpc:call(Node, application, set_env, [plumtree,
                                                                       peer_service,
                                                                       partisan_peer_service]),
                            ok = rpc:call(Node, application, set_env, [plumtree,
                                                                       broadcast_mods,
                                                                       [lasp_default_broadcast_distribution_backend]]),
                            ok = rpc:call(Node, application, set_env, [lasp,
                                                                       data_root,
                                                                       NodeDir])
                     end,
    lists:map(LoaderFun, Nodes),

    SimulationsSyncInterval = 5000,

    %% Configure Lasp settings.
    ConfigureFun = fun(Node) ->
                        %% Configure extended logging.
                        ok = rpc:call(Node, lasp_config, set,
                                      [extended_logging, true]),

                        %% Configure timers.
                        ok = rpc:call(Node, lasp_config, set,
                                      [aae_interval, SimulationsSyncInterval]),
                        ok = rpc:call(Node, lasp_config, set,
                                      [delta_interval, SimulationsSyncInterval]),

                        %% Configure plumtree AAE interval to be the same.
                        ok = rpc:call(Node, application, set_env,
                                      [plumtree, broadcast_exchange_timer, SimulationsSyncInterval]),

                        %% Configure who should be the server and who's
                        %% the client.
                        Simulation = proplists:get_value(simulation, Options, undefined),
                        case Simulation of
                            ad_counter ->
                                case Node of
                                    Server ->
                                        ok = rpc:call(Node, lasp_config, set,
                                                      [ad_counter_simulation_server, true]);
                                    _ ->
                                        ok = rpc:call(Node, lasp_config, set,
                                                      [ad_counter_simulation_client, true])
                                end;
                            game_tournament ->
                                case Node of
                                    Server ->
                                        ok = rpc:call(Node, lasp_config, set,
                                                      [tournament_simulation_server, true]);
                                    _ ->
                                        ok = rpc:call(Node, lasp_config, set,
                                                      [tournament_simulation_client, true])
                                end
                        end,

                        %% Configure the peer service.
                        PeerService = proplists:get_value(partisan_peer_service_manager, Options),
                        ok = rpc:call(Node, partisan_config, set,
                                      [partisan_peer_service_manager, PeerService]),

                        %% Configure the operational mode.
                        Mode = proplists:get_value(mode, Options),
                        ok = rpc:call(Node, lasp_config, set, [mode, Mode]),

                        %% Configure where code should run.
                        HeavyClient = proplists:get_value(heavy_client, Options, false),
                        ok = rpc:call(Node, lasp_config, set, [heavy_client, HeavyClient]),

                        %% Configure reactive server.
                        ReactiveServer = proplists:get_value(reactive_server, Options, false),
                        ok = rpc:call(Node, lasp_config, set, [reactive_server, ReactiveServer]),

                        %% Configure partitions.
                        PartitionProbability = proplists:get_value(partition_probability, Options, 0),
                        ok = rpc:call(Node, lasp_config, set, [partition_probability, PartitionProbability]),

                        %% Configure broadcast settings.
                        Broadcast = proplists:get_value(broadcast, Options),
                        ok = rpc:call(Node, lasp_config, set,
                                      [broadcast, Broadcast]),

                        %% Configure broadcast settings.
                        Set = proplists:get_value(set, Options),
                        ok = rpc:call(Node, lasp_config, set, [set, Set]),

                        %% Configure simulation.
                        Simulation = proplists:get_value(simulation, Options),
                        ok = rpc:call(Node, lasp_config, set,
                                      [simulation, Simulation]),

                        %% Configure evaluation timestamp.
                        EvalTimestamp = proplists:get_value(evaluation_timestamp, Options),
                        ok = rpc:call(Node, lasp_config, set,
                                      [evaluation_timestamp, EvalTimestamp]),

                        %% Configure instrumentation.
                        ok = rpc:call(Node, lasp_config, set,
                                      [instrumentation, true]),

                        %% Configure client number.
                        ClientNumber = proplists:get_value(client_number, Options),
                        ok = rpc:call(Node, lasp_config, set,
                                      [client_number, ClientNumber]),

                        %% Configure evaluation identifier.
                        EvalIdentifier = proplists:get_value(evaluation_identifier, Options),
                        RealEvalIdentifier = atom_to_list(EvalIdentifier)
                                          ++ "_" ++ integer_to_list(ClientNumber)
                                          ++ "_" ++ integer_to_list(PartitionProbability),
                        ok = rpc:call(Node, lasp_config, set,
                                      [evaluation_identifier, list_to_atom(RealEvalIdentifier)]),

                        %% Configure max impression number
                        MaxImpressions = 10,
                        ok = rpc:call(Node, lasp_config, set,
                                      [max_impressions, MaxImpressions])
                   end,
    lists:map(ConfigureFun, Nodes),

    ct:pal("Starting lasp."),

    StartFun = fun(Node) ->
                        %% Start lasp.
                        {ok, _} = rpc:call(Node, application, ensure_all_started, [lasp])
                   end,
    lists:map(StartFun, Nodes),

    ct:pal("Custering nodes..."),
    lists:map(fun(Node) -> cluster(Node, Nodes) end, Nodes),

    ct:pal("Lasp fully initialized."),

    Server.

%% @private
%%
%% We have to cluster each node with all other nodes to compute the
%% correct overlay: for instance, sometimes you'll want to establish a
%% client/server topology, which requires all nodes talk to every other
%% node to correctly compute the overlay.
%%
cluster(Node, Nodes) when is_list(Nodes) ->
    lists:map(fun(OtherNode) -> cluster(Node, OtherNode) end, Nodes -- [Node]);
cluster(Node, OtherNode) ->
    PeerPort = rpc:call(OtherNode,
                        partisan_config,
                        get,
                        [peer_port, ?PEER_PORT]),
    ct:pal("Joining node: ~p to ~p at port ~p", [Node, OtherNode, PeerPort]),
    ok = rpc:call(Node,
                  lasp_peer_service,
                  join,
                  [{OtherNode, {127, 0, 0, 1}, PeerPort}]).

%% @private
stop(Nodes) ->
    StopFun = fun(Node) ->
        case ct_slave:stop(Node) of
            {ok, _} ->
                ok;
            Error ->
                ct:fail(Error)
        end
    end,
    lists:map(StopFun, Nodes),
    ok.

%% @private
wait_for_completion(Server) ->
    ct:pal("Waiting for simulation to end"),
    case lasp_support:wait_until(fun() ->
                SimulationEnd = rpc:call(Server, lasp_config, get, [simulation_end, false]),
                SimulationEnd == true
        end, 60*10, ?STATUS_INTERVAL) of
        ok ->
            ct:pal("Simulation ended with success");
        Error ->
            ct:fail("Simulation failed: ~p", [Error])
    end.

%% @private
codepath() ->
    lists:filter(fun filelib:is_dir/1, code:get_path()).

%% @private
timestamp() ->
    {Mega, Sec, _Micro} = erlang:timestamp(),
    Mega * 1000000 + Sec.

%% @private
node_list(ClientNumber) ->
    Clients = client_list(ClientNumber),
    [server | Clients].

%% @private
client_list(0) -> [];
client_list(N) -> lists:append(client_list(N - 1), [list_to_atom("client_" ++ integer_to_list(N))]).

