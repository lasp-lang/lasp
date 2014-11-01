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

%% @doc Global programs test.

-module(derflow_global_programs_test).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-export([test/1]).

-ifdef(TEST).

-export([confirm/0]).

-define(HARNESS, (rt_config:get(rt_harness))).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    [Nodes] = rt:build_clusters([1]),
    lager:info("Nodes: ~p", [Nodes]),
    Node = hd(Nodes),

    ok = rt:wait_until_ring_converged(Nodes),

    lager:info("Remotely loading code on node ~p", [Node]),
    ok = derflow_test_helpers:load(Nodes),
    lager:info("Remote code loading complete."),

    ok = derflow_test_helpers:wait_for_cluster(Nodes),

    TestPaths = rt_config:get(test_paths, undefined),
    Program = hd(TestPaths) ++ "/../derflow_example_program.erl",
    lager:info("Program is: ~p", [Program]),

    lager:info("Remotely executing the test."),
    ?assertEqual([], rpc:call(Node, ?MODULE, test, [Program])),

    pass.

-endif.

test(Program) ->
    lager:info("Registering program from the test."),

    ok = derflow:register(derflow_example_program, Program, global),

    lager:info("Executing program from the test."),

    {ok, Result} = derflow:execute(derflow_example_program, global),

    Result.
