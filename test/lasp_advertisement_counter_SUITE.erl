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
    %% Start Lasp on the runner and enable instrumentation.
    lasp_support:start_runner(),

    _Config.

end_per_suite(_Config) ->
    %% Stop Lasp on the runner.
    lasp_support:stop_runner(),

    _Config.

init_per_testcase(Case, Config) ->
    %% Runner must start and stop in between test runs as well, to
    %% ensure that we clear the membership list (otherwise, we could
    %% delete the data on disk, but this is cleaner.)
    lasp_support:start_runner(),

    Nodes = lasp_support:start_nodes(Case, Config),

    [{nodes, Nodes}|Config].

end_per_testcase(Case, Config) ->
    lasp_support:stop_nodes(Case, Config),

    %% Runner must start and stop in between test runs as well, to
    %% ensure that we clear the membership list (otherwise, we could
    %% delete the data on disk, but this is cleaner.)
    lasp_support:stop_runner(),

    %% Generate transmission plot
    lasp_plot_gen:generate_plot().

all() -> [
          state_based_with_aae,
          state_based_with_aae_and_tree,
          delta_based_with_aae
          ].

%% ===================================================================
%% tests
%% ===================================================================

state_based_with_aae(Config) ->
    configure(Config, [{mode, state_based},
                       {broadcast, false},
                       {evaluation_identifier, state_based_with_aae}]),
    wait_for_completion(),
    ok.

state_based_with_aae_and_tree(Config) ->
    configure(Config, [{mode, state_based},
                       {broadcast, true},
                       {evaluation_identifier, state_based_with_aae_and_tree}]),
    wait_for_completion(),
    ok.

delta_based_with_aae(Config) ->
    configure(Config, [{mode, delta_based},
                       {broadcast, false},
                       {evaluation_identifier, delta_based_with_aae}]),
    wait_for_completion(),
    ok.

%% ===================================================================
%% Internal functions
%% ===================================================================

configure(Config, Options) ->
    lager:info("Configuring nodes; options: ~p", [Options]),
    Nodes = proplists:get_value(nodes, Config),

    lager:info("Enabling ad client simulation on all nodes."),
    lists:foreach(fun(Node) ->
                        ok = rpc:call(Node, lasp_config, set,
                                      [ad_counter_simulation_client, true])
                  end, Nodes),

    lager:info("Setting evaluation identifier on all nodes."),
    Identifier = lasp_config:get(evaluation_identifier, undefined),
    lists:foreach(fun(Node) ->
                        ok = rpc:call(Node, lasp_config, set,
                                      [evaluation_identifier, Identifier])
                  end, Nodes),


    lager:info("Enabling ad server simulation on local node."),
    ok = lasp_config:set(ad_counter_simulation_server, true),

    lager:info("Enabling instrumentation."),
    ok = lasp_config:set(instrumentation, true),

    lager:info("Enabling instrumentation on all nodes."),
    lists:foreach(fun(Node) ->
                        ok = rpc:call(Node, lasp_config, set,
                                      [instrumentation, true])
                  end, Nodes),

    Mode = proplists:get_value(mode, Options),

    lager:info("Enabling mode locally: ~p.", [Mode]),
    ok = lasp_config:set(mode, Mode),

    lager:info("Enabling mode on all nodes: ~p", [Mode]),
    lists:foreach(fun(Node) ->
                        ok = rpc:call(Node, lasp_config, set,
                                      [mode, Mode])
                  end, Nodes),

    Broadcast = proplists:get_value(broadcast, Options),

    lager:info("Enabling broadcast locally: ~p.", [broadcast]),
    ok = lasp_config:set(broadcast, Broadcast),

    lager:info("Enabling broadcast on all nodes: ~p", [Broadcast]),
    lists:foreach(fun(Node) ->
                        ok = rpc:call(Node, lasp_config, set,
                                      [broadcast, Broadcast])
                  end, Nodes),

    lager:info("Restarting Lasp on all nodes."),
    lists:foreach(fun(Node) ->
                        lager:info("Restarting ~p and re-joining...", [Node]),
                        ok = rpc:call(Node, application, stop, [lasp]),
                        {ok, _} = rpc:call(Node, application, ensure_all_started,
                                           [lasp]),
                        RunnerNode = lasp_support:runner_node(),
                        lasp_support:join_to(Node, RunnerNode),
                        timer:sleep(4000),
                        {ok, Members} = rpc:call(Node, lasp_peer_service, members, []),
                        {ok, LocalMembers} = lasp_peer_service:members(),
                        lager:info("* Members; ~p", [Members]),
                        lager:info("* LocalMembers; ~p", [LocalMembers])
                  end, Nodes),

    ok.

wait_for_completion() ->
    timer:sleep(20000).
