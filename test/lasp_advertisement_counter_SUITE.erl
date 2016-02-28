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

-module(lasp_advertisement_counter_SUITE).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

%% common_test callbacks
-export([%% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0]).

%% tests
-compile([export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

%% ===================================================================
%% common_test callbacks
%% ===================================================================

init_per_suite(_Config) ->
    lager:start(),

    %% this might help, might not...
    os:cmd(os:find_executable("epmd")++" -daemon"),
    {ok, Hostname} = inet:gethostname(),
    case net_kernel:start([list_to_atom("runner@"++Hostname), shortnames]) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,
    lager:info("node name ~p", [node()]),

    %% Start Lasp on the runner and enable instrumentation.
    ok = application:load(lasp),
    ok = application:set_env(lasp, instrumentation, true),
    {ok, _} = application:ensure_all_started(lasp),

    _Config.

end_per_suite(_Config) ->
    application:stop(lasp),
    application:stop(lager),
    _Config.

init_per_testcase(Case, Config) ->
    Nodes = lasp_test_utils:pmap(fun(N) -> lasp_test_utils:start_node(N, Config, Case) end, [jaguar, shadow, thorn, pyros]),
    ct:pal("Nodes: ~p", [Nodes]),

    RunnerNode = runner_node(),
    ct:pal("RunnerNode: ~p", [RunnerNode]),

    %% Attempt to join all nodes in the cluster.
    lists:foreach(fun(N) ->
                        ct:pal("Joining node: ~p to ~p", [N, RunnerNode]),
                        ok = rpc:call(RunnerNode, lasp_peer_service, join, [N])
                  end, Nodes),

    %% Consider the runner part of the cluster.
    Nodes1 = [RunnerNode|Nodes],
    ct:pal("Nodes1: ~p", [Nodes1]),

    %% Sleep until application is fully started.
    %% @todo: Change to a wait_until, eventually.
    timer:sleep(60),

    %% Wait until convergence.
    ok = lasp_test_utils:wait_until_joined(Nodes1, Nodes1),
    ct:pal("Cluster converged."),

    {ok, _} = ct_cover:add_nodes(Nodes1),
    [{nodes, Nodes1}|Config].

end_per_testcase(_, _Config) ->
    timer:sleep(1000), %% @todo: Travis related teardown race condition.
    lasp_test_utils:pmap(fun(Node) -> ct_slave:stop(Node) end, [jaguar, shadow, thorn, pyros]),
    ok.

runner_node() ->
    {ok, Hostname} = inet:gethostname(),
    list_to_atom("runner@"++Hostname).

all() ->
    [
        setup_test,
        minimal_test,
        minimal_delta_test
    ].

%% ===================================================================
%% tests
%% ===================================================================

setup_test(_Config) ->
    ok.

minimal_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    {ok, _} = lasp_simulation:run(lasp_advertisement_counter,
                                  [Nodes, false, lasp_orset, lasp_gcounter, 100, 100, 10]),
    ok.

minimal_delta_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    {ok, _} = lasp_simulation:run(lasp_advertisement_counter,
                                  [Nodes, true, lasp_orset, lasp_gcounter, 100, 100, 10]),
    ok.
