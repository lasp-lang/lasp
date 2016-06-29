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

all() ->
    [
     state_based_with_aae,
     state_based_with_aae_and_tree,
     delta_based_with_aae
    ].

%% ===================================================================
%% tests
%% ===================================================================

-define(EVAL_NUMBER, 3).
-define(EVAL_TIME, 20000).

state_based_with_aae(Config) ->
    run(Config, [{mode, state_based},
                 {broadcast, false},
                 {evaluation_identifier, state_based_with_aae}]),
    ok.

state_based_with_aae_and_tree(Config) ->
    run(Config, [{mode, state_based},
                 {broadcast, true},
                 {evaluation_identifier, state_based_with_aae_and_tree}]),
    ok.

delta_based_with_aae(Config) ->
    run(Config, [{mode, delta_based},
                 {broadcast, false},
                 {evaluation_identifier, delta_based_with_aae}]),
    ok.

%% ===================================================================
%% Internal functions
%% ===================================================================

run(Config, Options) ->
    lists:foreach(
        fun(EvalNumber) ->
            configure(
              Config,
              [{evaluation_number, EvalNumber} | Options]
            ),
            wait_for_completion()
        end,
        lists:seq(1, ?EVAL_NUMBER)
    ).

configure(Config, Options) ->
    lager:info("Configuring nodes; options: ~p", [Options]),
    Nodes = proplists:get_value(nodes, Config),

    lager:info("Enabling ad client simulation on all nodes."),
    lists:foreach(fun(Node) ->
                        ok = rpc:call(Node, lasp_config, set,
                                      [ad_counter_simulation_client, true])
                  end, Nodes),

    lager:info("Enabling ad server simulation on local node."),
    ok = lasp_config:set(ad_counter_simulation_server, true),

    %% Enabling instrumentation
    lager:info("Enabling instrumentation locally."),
    ok = lasp_config:set(instrumentation, true),

    lager:info("Enabling instrumentation on all nodes."),
    lists:foreach(fun(Node) ->
                        ok = rpc:call(Node, lasp_config, set,
                                      [instrumentation, true])
                  end, Nodes),


    %% Setting mode
    Mode = proplists:get_value(mode, Options),

    lager:info("Setting mode locally: ~p.", [Mode]),
    ok = lasp_config:set(mode, Mode),

    lager:info("Setting mode on all nodes: ~p", [Mode]),
    lists:foreach(fun(Node) ->
                        ok = rpc:call(Node, lasp_config, set,
                                      [mode, Mode])
                  end, Nodes),


    %% Setting broadcast
    Broadcast = proplists:get_value(broadcast, Options),

    lager:info("Setting broadcast locally: ~p.", [Broadcast]),
    ok = lasp_config:set(broadcast, Broadcast),

    lager:info("Setting broadcast on all nodes: ~p", [Broadcast]),
    lists:foreach(fun(Node) ->
                        ok = rpc:call(Node, lasp_config, set,
                                      [broadcast, Broadcast])
                  end, Nodes),


    %% Setting evaluation identifier
    EvalIdentifier = proplists:get_value(evaluation_identifier, Options),

    lager:info("Setting evaluation identifier locally: ~p.", [EvalIdentifier]),
    ok = lasp_config:set(evaluation_identifier, EvalIdentifier),

    lager:info("Setting evaluation identifier on all nodes: ~p", [EvalIdentifier]),
    lists:foreach(fun(Node) ->
                        ok = rpc:call(Node, lasp_config, set,
                                      [evaluation_identifier, EvalIdentifier])
                  end, Nodes),


    %% Setting evaluation number
    EvalNumber = proplists:get_value(evaluation_number, Options),

    lager:info("Setting evaluation number locally: ~p.", [EvalNumber]),
    ok = lasp_config:set(evaluation_number, EvalNumber),

    lager:info("Setting evaluation number on all nodes: ~p", [EvalNumber]),
    lists:foreach(fun(Node) ->
                        ok = rpc:call(Node, lasp_config, set,
                                      [evaluation_number, EvalNumber])
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
    timer:sleep(?EVAL_TIME).
