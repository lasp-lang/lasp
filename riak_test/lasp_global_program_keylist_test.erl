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

%% @doc Global keylist program test.

-module(lasp_global_program_keylist_test).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-export([test/0]).

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
    ok = lasp_test_helpers:load(Nodes),
    lager:info("Remote code loading complete."),

    ok = lasp_test_helpers:wait_for_cluster(Nodes),

    lager:info("Remotely executing the test."),
    ?assertEqual([1,2], rpc:call(Node, ?MODULE, test, [])),

    pass.

-endif.

test() ->
    %% Put one value.
    lager:info("Processing one value."),
    ok = lasp:process(lasp_example_keylist_program, global, 1, put, 0, node()),

    %% Put one value.
    lager:info("Processing second value."),
    ok = lasp:process(lasp_example_keylist_program, global, 2, put, 0, node()),

    lager:info("Executing program from the test."),
    {ok, Result} = lasp:execute(lasp_example_keylist_program, global),

    Result.
