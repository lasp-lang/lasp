%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
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

%% @doc Helpers for remotely running code.

-module(lasp_test_helpers).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-export([load/1,
         wait_for_cluster/1]).

-export([build_clusters/1]).

%% @doc Ensure cluster is properly configured.
wait_for_cluster(Nodes) ->
    lager:info("Waiting for transfers to complete."),
    ok = rt:wait_until_transfers_complete(Nodes),
    lager:info("Transfers complete.").

%% @doc Remotely load test code on a series of nodes.
load(Nodes) when is_list(Nodes) ->
    _ = [ok = load(Node) || Node <- Nodes],
    ok;
load(Node) ->
    TestGlob = rt_config:get(tests_to_remote_load, undefined),
    case TestGlob of
        undefined ->
            ok;
        TestGlob ->
            Tests = filelib:wildcard(TestGlob),
            [ok = remote_compile_and_load(Node, Test) || Test <- Tests],
            ok
    end.

%% @doc Remotely compile and load a test on a given node.
remote_compile_and_load(Node, F) ->
    {ok, _, Bin} = rpc:call(Node, compile, file,
                            [F, [binary,
                                 {parse_transform, lager_transform}]]),
    ModName = list_to_atom(filename:basename(F, ".erl")),
    {module, _} = rpc:call(Node, code, load_binary, [ModName, F, Bin]),
    ok.

-include_lib("eunit/include/eunit.hrl").

%% @doc Build a series of cluster.
build_clusters(Settings) ->
    Clusters = rt:deploy_clusters(Settings),
    [begin
         ok = join_cluster(Nodes),
         lager:info("Cluster built: ~p", [Nodes])
     end || Nodes <- Clusters],
    Clusters.

%% @private
join_cluster(Nodes) ->
    PeerService = lasp_peer_service:peer_service(),

    case PeerService of
        lasp_riak_core_peer_service ->
            %% Ensure each node owns 100% of it's own ring
            [?assertEqual([Node], rt:owners_according_to(Node)) || Node <- Nodes],

            %% Join nodes
            [Node1|OtherNodes] = Nodes,
            case OtherNodes of
                [] ->
                    %% no other nodes, nothing to join/plan/commit
                    ok;
                _ ->
                    %% ok do a staged join and then commit it, this eliminates the
                    %% large amount of redundant handoff done in a sequential join
                    [rt:staged_join(Node, Node1) || Node <- OtherNodes],
                     rt:plan_and_commit(Node1),
                     rt:try_nodes_ready(Nodes, 3, 500)
            end,

            %% Wait until nodes are ready.
            ?assertEqual(ok, rt:wait_until_nodes_ready(Nodes)),

            %% Ensure each node owns a portion of the ring
            rt:wait_until_nodes_agree_about_ownership(Nodes),
            ?assertEqual(ok, rt:wait_until_no_pending_changes(Nodes)),
            ok;
        Unknown ->
            lager:error("Unknown peer service: ~p", [Unknown]),
            {error, unknown_peer_service}
    end.
