%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Helium Systems, Inc.  All Rights Reserved.
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

-module(lasp_support).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("lasp.hrl").

-export([runner_node/0,
         get_cluster_members/1,
         pmap/2,
         wait_until/3,
         wait_until_left/2,
         wait_until_joined/2,
         wait_until_offline/1,
         wait_until_disconnected/2,
         wait_until_connected/2,
         nodelist/0,
         start_node/3,
         start_and_join_node/3,
         start_nodes/2,
         stop_nodes/2,
         stop_runner/0,
         start_runner/0,
         puniform/1,
         join_to/2,
         partition_cluster/2,
         heal_cluster/2,
         load_lasp/3,
         start_lasp/2,
         push_logs/1]).

-define(EXCHANGE_TIMER, 120).

puniform(Range) ->
    erlang:phash2(erlang:statistics(io), Range) + 1.

get_cluster_members(Node) ->
    {Node, {ok, Res}} = {Node, rpc:call(Node, lasp_peer_service, members, [])},
    Res.

pmap(F, L) ->
    Parent = self(),
    lists:foldl(
        fun(X, N) ->
                spawn_link(fun() ->
                            Parent ! {pmap, N, F(X)}
                    end),
                N+1
        end, 0, L),
    L2 = [receive {pmap, N, R} -> {N, R} end || _ <- L],
    {_, L3} = lists:unzip(lists:keysort(1, L2)),
    L3.

wait_until(Fun, Retry, Delay) when Retry > 0 ->
    Res = Fun(),
    case Res of
        true ->
            ok;
        _ when Retry == 1 ->
            {fail, Res};
        _ ->
            timer:sleep(Delay),
            wait_until(Fun, Retry-1, Delay)
    end.

wait_until_left(Nodes, LeavingNode) ->
    wait_until(fun() ->
                lists:all(fun(X) -> X == true end,
                          pmap(fun(Node) ->
                                not
                                lists:member(LeavingNode,
                                             get_cluster_members(Node))
                        end, Nodes))
        end, 60*2, 500).

wait_until_joined(Nodes, ExpectedCluster) ->
    Manager = lasp_peer_service:manager(),
    case Manager of
        %% Naively wait until the active views across all nodes sum to
        %% the complete set of nodes; note: this is *good enough* for
        %% the test and doesn't guarantee that the cluster is fully
        %% connected.
        %%
        partisan_hyparview_peer_service_manager ->
            wait_until(fun() ->
                        lists:sort(ExpectedCluster) ==
                        lists:usort(lists:flatten(pmap(fun(Node) ->
                                        get_cluster_members(Node)
                                end, Nodes)))
                end, 60*2, 500);
        _ ->
            wait_until(fun() ->
                        lists:all(fun(X) -> X == true end,
                                  pmap(fun(Node) ->
                                        lists:sort(ExpectedCluster) ==
                                        lists:sort(get_cluster_members(Node))
                                end, Nodes))
                end, 60*2, 500)
    end,
    ok.

wait_until_offline(Node) ->
    wait_until(fun() ->
                pang == net_adm:ping(Node)
        end, 60*2, 500).

wait_until_disconnected(Node1, Node2) ->
    wait_until(fun() ->
                pang == rpc:call(Node1, net_adm, ping, [Node2])
        end, 60*2, 500).

wait_until_connected(Node1, Node2) ->
    wait_until(fun() ->
                pong == rpc:call(Node1, net_adm, ping, [Node2])
        end, 60*2, 500).

start_and_join_node(Name, Config, Case) ->
    Node = start_node(Name, Config, Case),
    RunnerNode = runner_node(),
    {ok, Members} = lasp_peer_service:members(),
    join_to(Node, RunnerNode),
    Nodes = Members ++ [Node],
    wait_until_joined(Nodes, Nodes),
    Node.

start_node(Name, Config, Case) ->
    CodePath = lists:filter(fun filelib:is_dir/1, code:get_path()),
    %% have the slave nodes monitor the runner node, so they can't outlive it
    NodeConfig = [ {monitor_master, true},
                   {startup_functions, [ {code, set_path, [CodePath]} ]}],

    Node = start_slave(Name, NodeConfig, Case),
    load_lasp(Node, Config, Case),
    start_lasp(Node, Config).

partition_cluster(ANodes, BNodes) ->
    pmap(fun({Node1, Node2}) ->
                true = rpc:call(Node1, erlang, set_cookie, [Node2, canttouchthis]),
                true = rpc:call(Node1, erlang, disconnect_node, [Node2]),
                ok = wait_until_disconnected(Node1, Node2)
        end,
         [{Node1, Node2} || Node1 <- ANodes, Node2 <- BNodes]),
    ok.

heal_cluster(ANodes, BNodes) ->
    GoodCookie = erlang:get_cookie(),
    pmap(fun({Node1, Node2}) ->
                true = rpc:call(Node1, erlang, set_cookie, [Node2, GoodCookie]),
                ok = wait_until_connected(Node1, Node2)
        end,
         [{Node1, Node2} || Node1 <- ANodes, Node2 <- BNodes]),
    ok.

nodelist() ->
    lists:map(fun(X) ->
                      list_to_atom("node-" ++ integer_to_list(X))
              end, lists:seq(1, lasp_config:get(client_number, 3))).

start_nodes(Case, Config0) ->
    StartedConfig = lists:foldl(fun(N, Config) ->
                                Node = lasp_support:start_node(N, Config, Case),
                                Started0 = proplists:get_value(started, Config, []),
                                Started = [Node|Started0],
                                Config1 = proplists:delete(started, Config0),
                                [{started, Started}|Config1]
                        end, Config0, lasp_support:nodelist()),
    Nodes = proplists:get_value(started, StartedConfig),
    % ct:pal("Nodes: ~p", [Nodes]),

    RunnerNode = runner_node(),
    % ct:pal("RunnerNode: ~p", [RunnerNode]),

    %% Attempt to join all nodes in the cluster.
    lists:foreach(fun(N) ->
                        join_to(N, RunnerNode)
                  end, Nodes),

    %% Consider the runner part of the cluster.
    Nodes1 = [RunnerNode|Nodes],
    % ct:pal("Nodes1: ~p", [Nodes1]),

    %% Sleep until application is fully started.
    %% @todo: Change to a wait_until, eventually.
    timer:sleep(60),

    %% Wait until convergence.
    ok = lasp_support:wait_until_joined(Nodes1, Nodes1),
    % ct:pal("Cluster converged."),

    {ok, _} = ct_cover:add_nodes(Nodes1),

    Nodes1.

runner_node() ->
    {ok, Hostname} = inet:gethostname(),
    list_to_atom("runner@"++Hostname).

stop_nodes(_Case, _Config) ->
    %% Multi-node race condition protection, where if we don't wait for
    %% all nodes to stop delivering messages, one might arrive during
    %% shutdown and trigger an exception, sigh.
    timer:sleep(5000),

    lasp_support:pmap(fun(Node) -> ct_slave:stop(Node) end, lasp_support:nodelist()),

    ok.

start_runner() ->
    %% Launch distribution for the test runner.
    os:cmd(os:find_executable("epmd") ++ " -daemon"),
    {ok, Hostname} = inet:gethostname(),
    case net_kernel:start([list_to_atom("runner@" ++ Hostname), shortnames]) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok
    end,

    %% Manually force sasl loading, and disable the logger.
    application:load(sasl),
    ok = application:set_env(sasl,
                             sasl_error_logger,
                             false),
    application:start(sasl),

    case application:load(lasp) of
        {error, {already_loaded, lasp}} ->
            ok;
        ok ->
            ok;
        Reason ->
            lager:info("Received unexpected error: ~p", [Reason]),
            exit(Reason)
    end,

    ok = application:set_env(plumtree,
                             peer_service,
                             partisan_peer_service),
    ok = application:set_env(plumtree,
                             broadcast_exchange_timer,
                             ?EXCHANGE_TIMER),
    ok = application:set_env(plumtree,
                             broadcast_mods,
                             [lasp_default_broadcast_distribution_backend]),

    {ok, _} = application:ensure_all_started(lasp),

    ok.

stop_runner() ->
    application:stop(lasp).

join_to(N, RunnerNode) ->
    PeerPort = rpc:call(N,
                        partisan_config,
                        get,
                        [peer_port, ?PEER_PORT]),
    ct:pal("Joining node: ~p to ~p at port ~p",
           [N, RunnerNode, PeerPort]),
    ok = rpc:call(RunnerNode,
                  lasp_peer_service,
                  join,
                  [{N, {127, 0, 0, 1}, PeerPort}]).

load_lasp(Node, Config, Case) ->
    PrivDir = proplists:get_value(priv_dir, Config),
    NodeDir = filename:join([PrivDir, Node, Case]),

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
                                               broadcast_exchange_timer,
                                               ?EXCHANGE_TIMER]),
    ok = rpc:call(Node, application, set_env, [plumtree,
                                               broadcast_mods,
                                               [lasp_default_broadcast_distribution_backend]]),
    ok = rpc:call(Node, application, set_env, [lasp,
                                               data_root,
                                               NodeDir]).

start_lasp(Node, Config) ->
    try
        {ok, _} = rpc:call(Node, application, ensure_all_started, [lasp])
    catch
        _:Error ->
            lager:info("Node initialization for ~p failed: ~p", [Node, Error]),
            lists:foreach(fun(N) ->
                                  WebPort = rpc:call(N,
                                                     lasp_config,
                                                     get,
                                                     [web_port, undefined]),
                                  PeerPort = rpc:call(N,
                                                      lasp_config,
                                                      get,
                                                      [peer_port, undefined]),
                                  lager:info("Node: ~p PeerPort: ~p WebPort ~p", [N, PeerPort, WebPort])
                          end, proplists:get_value(started, Config) ++ [Node]),
            ct:fail(can_not_initialize_node)
    end,
    ok = wait_until(fun() ->
                    case rpc:call(Node, lasp_peer_service, members, []) of
                        {ok, _Res} -> true;
                        _ -> false
                    end
            end, 60, 500),
    Node.

start_slave(Name, NodeConfig, _Case) ->
    case ct_slave:start(Name, NodeConfig) of
        {ok, Node} ->
            Node;
        {error, already_started, _Node} ->
            case ct_slave:stop(Name) of
                {ok, _} ->
                    case ct_slave:start(Name, NodeConfig) of
                        {ok, Node} ->
                            Node;
                        Error ->
                            ct:fail(Error)
                    end;
                Error ->
                    ct:fail(Error)
            end
    end.

push_logs(LogFiles) ->
    DCOS = os:getenv("DCOS", "false"),
    LOGS = os:getenv("LOGS", "s3"),

    case DCOS of
        "false" ->
            ok;
        _ ->
            lager:info("Will push logs"),
            case LOGS of
                "s3" ->
                    %% Configure erlcloud.
                    S3Host = "s3.amazonaws.com",
                    AccessKeyId = os:getenv("AWS_ACCESS_KEY_ID"),
                    SecretAccessKey = os:getenv("AWS_SECRET_ACCESS_KEY"),
                    erlcloud_s3:configure(AccessKeyId, SecretAccessKey, S3Host),

                    BucketName = "lasp-instrumentation-logs",
                    %% Create S3 bucket.
                    try
                        lager:info("Creating bucket: ~p", [BucketName]),
                        ok = erlcloud_s3:create_bucket(BucketName)
                    catch
                        _:{aws_error, Error} ->
                        lager:info("Bucket creation failed: ~p", [Error]),
                        ok
                    end,

                    %% Store logs on S3.
                    lists:foreach(
                        fun({FilePath, S3Id}) ->
                            Lines = read_file(FilePath),
                            Logs = lists:foldl(
                                fun(Line, Acc) ->
                                    Acc ++ Line
                                end,
                                "",
                                Lines
                            ),
                            erlcloud_s3:put_object(BucketName, S3Id, list_to_binary(Logs))
                        end,
                        LogFiles
                    )
            end
    end.

%% @private
read_file(FilePath) ->
    {ok, FileDescriptor} = file:open(FilePath, [read]),
    Lines = read_lines(FilePath, FileDescriptor),
    Lines.

%% @private
read_lines(FilePath, FileDescriptor) ->
    case io:get_line(FileDescriptor, '') of
        eof ->
            [];
        {error, Error} ->
            lager:warning("Error while reading line from file ~p. Error: ~p", [FilePath, Error]),
            [];
        Line ->
            [Line | read_lines(FilePath, FileDescriptor)]
    end.
