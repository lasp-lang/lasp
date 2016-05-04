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

-export([get_cluster_members/1,
         pmap/2,
         wait_until/3,
         wait_until_left/2,
         wait_until_joined/2,
         wait_until_offline/1,
         wait_until_disconnected/2,
         wait_until_connected/2,
         nodelist/0,
         start_node/3,
         start_nodes/2,
         stop_nodes/2,
         stop_runner/0,
         start_runner/0,
         puniform/1,
         partition_cluster/2,
         heal_cluster/2]).

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
    wait_until(fun() ->
                lists:all(fun(X) -> X == true end,
                          pmap(fun(Node) ->
                                lists:sort(ExpectedCluster) ==
                                lists:sort(get_cluster_members(Node))
                        end, Nodes))
        end, 60*2, 500).

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

start_node(Name, Config, Case) ->
    CodePath = lists:filter(fun filelib:is_dir/1, code:get_path()),
    %% have the slave nodes monitor the runner node, so they can't outlive it
    NodeConfig = [ {monitor_master, true},
                   {startup_functions, [ {code, set_path, [CodePath]} ]}],

    case ct_slave:start(Name, NodeConfig) of
        {ok, Node} ->
            PrivDir = proplists:get_value(priv_dir, Config),
            NodeDir = filename:join([PrivDir, Node, Case]),
            WebPort = web_port(Name),
            ok = rpc:call(Node, application, load, [plumtree]),
            ok = rpc:call(Node, application, load, [partisan]),
            ok = rpc:call(Node, application, load, [lager]),
            ok = rpc:call(Node, application, load, [lasp]),
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
                                                       120]),
            ok = rpc:call(Node, application, set_env, [plumtree,
                                                       broadcast_mods,
                                                       [lasp_plumtree_broadcast_distribution_backend]]),
            ok = rpc:call(Node, application, set_env, [lasp,
                                                       data_root,
                                                       NodeDir]),
            ok = rpc:call(Node, application, set_env, [lasp,
                                                       web_port,
                                                       WebPort]),
            {ok, _} = rpc:call(Node, application, ensure_all_started, [lasp]),
            ok = wait_until(fun() ->
                            case rpc:call(Node, lasp_peer_service, members, []) of
                                {ok, _Res} -> true;
                                _ -> false
                            end
                    end, 60, 500),
            Node;
        {error, already_started, Node} ->
            ct_slave:stop(Name),
            wait_until_offline(Node),
            start_node(Name, Config, Case)
    end.

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

web_port(_) ->
    random:uniform(5000) + 1024.

nodelist() ->
    lists:map(fun(X) ->
                      list_to_atom("node-" ++ integer_to_list(X))
              end, lists:seq(1, ?NUM_NODES)).

start_nodes(Case, Config) ->
    Nodes = lasp_support:pmap(fun(N) -> lasp_support:start_node(N, Config, Case) end, lasp_support:nodelist()),
    ct:pal("Nodes: ~p", [Nodes]),

    RunnerNode = runner_node(),
    ct:pal("RunnerNode: ~p", [RunnerNode]),

    %% Attempt to join all nodes in the cluster.
    lists:foreach(fun(N) ->
                        lager:info("Locating node: ~p", [N]),
                        PeerPort = rpc:call(N,
                                            partisan_config,
                                            get,
                                            [peer_port, ?PEER_PORT]),
                        ct:pal("Joining node: ~p to ~p at port ~p",
                               [N, RunnerNode, PeerPort]),
                        ok = rpc:call(RunnerNode,
                                      lasp_peer_service,
                                      join,
                                      [{N, {127, 0, 0, 1}, PeerPort}])
                  end, Nodes),

    %% Consider the runner part of the cluster.
    Nodes1 = [RunnerNode|Nodes],
    ct:pal("Nodes1: ~p", [Nodes1]),

    %% Sleep until application is fully started.
    %% @todo: Change to a wait_until, eventually.
    timer:sleep(60),

    %% Wait until convergence.
    ok = lasp_support:wait_until_joined(Nodes1, Nodes1),
    ct:pal("Cluster converged."),

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
    case application:load(lasp) of
        {error, {already_loaded, lasp}} ->
            ok;
        ok ->
            ok;
        Reason ->
            lager:info("Received unexpected error: ~p", [Reason]),
            exit(Reason)
    end,
    ok = application:set_env(plumtree, peer_service, partisan_peer_service),
    ok = application:set_env(plumtree, broadcast_exchange_timer, 120),
    ok = application:set_env(plumtree, broadcast_mods, [lasp_plumtree_broadcast_distribution_backend]),
    ok = application:set_env(lasp, instrumentation, true),
    {ok, _} = application:ensure_all_started(lasp),
    ok.

stop_runner() ->
    application:stop(lasp),
    application:stop(plumtree),
    application:stop(partisan),
    application:stop(lager).
