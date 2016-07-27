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
%%

-module(lasp_simulation_support).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("lasp.hrl").

-export([run/3]).
-export([push_logs/0]).

run(Case, Config, Options) ->
    lists:foreach(
        fun(_EvalNumber) ->
            Nodes = start(
              Case,
              Config,
              [{evaluation_timestamp, timestamp()} | Options]
            ),
            wait_for_completion(Nodes),
            stop(Nodes)
        end,
        lists:seq(1, ?EVAL_NUMBER)
    ).

push_logs() ->
    ok.

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
    [First|_] = Nodes = lists:map(InitializerFun, ?CT_SLAVES),

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

    %% Configure Lasp settings.
    ConfigureFun = fun(Node) ->
                        %% Configure timers.
                        ok = rpc:call(Node, lasp_config, set,
                                      [aae_interval, ?AAE_INTERVAL]),

                        %% Configure plumtree AAE interval to be the same.
                        ok = rpc:call(Node, application, set_env,
                                      [plumtree, broadcast_exchange_timer, ?AAE_INTERVAL]),

                        %% Configure number of impressions.
                        ok = rpc:call(Node, lasp_config, set,
                                      [simulation_event_number, ?IMPRESSION_NUMBER]),

                        %% Configure who should be the server and who's
                        %% the client.
                        Simulation = proplists:get_value(simulation, Options),

                        case Simulation of
                            ad_counter ->
                                case Node of
                                    First ->
                                        ok = rpc:call(Node, lasp_config, set,
                                                      [ad_counter_simulation_server, true]);
                                    _ ->
                                        ok = rpc:call(Node, lasp_config, set,
                                                      [ad_counter_simulation_client, true])
                                end;
                            music_festival ->
                                case Node of
                                    First ->
                                        ok = rpc:call(Node, lasp_config, set,
                                                      [music_festival_simulation_server, true]);
                                    _ ->
                                        ok = rpc:call(Node, lasp_config, set,
                                                      [music_festival_simulation_client, true])
                                end
                        end,

                        %% Configure the peer service.
                        PeerService = proplists:get_value(partisan_peer_service_manager, Options),
                        ok = rpc:call(Node, partisan_config, set,
                                      [partisan_peer_service_manager, PeerService]),

                        %% Configure the operational mode.
                        Mode = proplists:get_value(mode, Options),
                        ok = rpc:call(Node, lasp_config, set, [mode, Mode]),

                        %% Configure broadcast settings.
                        Broadcast = proplists:get_value(broadcast, Options),
                        ok = rpc:call(Node, lasp_config, set,
                                      [broadcast, Broadcast]),

                        %% Configure broadcast settings.
                        Set = proplists:get_value(set, Options),
                        ok = rpc:call(Node, lasp_config, set, [set, Set]),

                        %% Configure evaluation identifier.
                        EvalIdentifier = proplists:get_value(evaluation_identifier, Options),
                        ok = rpc:call(Node, lasp_config, set,
                                      [evaluation_identifier, {Simulation, EvalIdentifier}]),

                        %% Configure evaluation timestamp.
                        EvalTimestamp = proplists:get_value(evaluation_timestamp, Options),
                        ok = rpc:call(Node, lasp_config, set,
                                      [evaluation_timestamp, EvalTimestamp]),

                        %% Configure instrumentation.
                        ok = rpc:call(Node, lasp_config, set,
                                      [instrumentation, true])
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

    Nodes.

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
stop(_Nodes) ->
    StopFun = fun(Node) ->
        case ct_slave:stop(Node) of
            {ok, _} ->
                ok;
            Error ->
                ct:fail(Error)
        end
    end,
    lists:map(StopFun, ?CT_SLAVES),
    ok.

%% @private
wait_for_completion([Server | _] = _Nodes) ->
    case lasp_support:wait_until(fun() ->
                Convergence = rpc:call(Server, lasp_config, get, [convergence, false]),
                ct:pal("Waiting for convergence: ~p", [Convergence]),
                Convergence == true
        end, 60*4, ?CONVERGENCE_INTERVAL) of
        ok ->
            ct:pal("Convergence reached!");
        Error ->
            ct:fail("Convergence not reached: ~p", [Error])
    end.

%% @private
codepath() ->
    lists:filter(fun filelib:is_dir/1, code:get_path()).

%% @private
timestamp() ->
    {Mega, Sec, _Micro} = erlang:timestamp(),
    Mega * 1000000 + Sec.
