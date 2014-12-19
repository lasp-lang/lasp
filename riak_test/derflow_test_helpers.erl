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

%% @doc Helpers for remotely running derpflow code.

-module(derpflow_test_helpers).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-export([load/1,
         wait_for_cluster/1]).

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
    lager:info("Test glob is: ~p", [TestGlob]),
    case TestGlob of
        undefined ->
            ok;
        TestGlob ->
            Tests = filelib:wildcard(TestGlob),
            lager:info("Found the following tests: ~p", [Tests]),
            [ok = remote_compile_and_load(Node, Test) || Test <- Tests],
            ok
    end.

%% @doc Remotely compile and load a test on a given node.
remote_compile_and_load(Node, F) ->
    lager:info("Compiling and loading file ~s on node ~s", [F, Node]),
    {ok, _, Bin} = rpc:call(Node, compile, file,
                            [F, [binary,
                                 {parse_transform, lager_transform}]]),
    ModName = list_to_atom(filename:basename(F, ".erl")),
    {module, _} = rpc:call(Node, code, load_binary, [ModName, F, Bin]),
    ok.
