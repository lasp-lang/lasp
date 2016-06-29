%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Junghun Yoo.  All Rights Reserved.
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

-module(lasp_sql_query_SUITE).
-author("Junghun Yoo <junghun.yoo@cs.ox.ac.uk>").

%% common_test callbacks
-export([%% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0]).

-export([is_equal_inter/2,
         get_inter_elem/2]).

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
    lasp_support:stop_runner().

all() ->
    [
     simple_query_test,
     simple_join_query_test
    ].

%% ===================================================================
%% tests
%% ===================================================================

-define(SET, orset).

%%-type attribute_name() :: term().
%%-type element() :: {attribute_name(), term()}.
%%-type tuple() :: {sqltuple, [element()]}.
%%-type table() :: ?SET(tuple()).

-define(ATTR1ST, a1st).
-define(ATTR2ND, a2nd).
-define(ATTR3RD, a3rd).
-define(ATTR4TH, a4th).

is_equal_inter({sqltuple, [{?ATTR1ST, _Attr1L},
                           {?ATTR2ND, _Attr2L},
                           {?ATTR3RD, Attr3L}]},
               {sqltuple, [{?ATTR1ST, _Attr1R},
                           {?ATTR3RD, Attr3R},
                           {?ATTR4TH, _Attr4R}]}) ->
    Attr3L == Attr3R;
is_equal_inter({sqltuple, _FieldsL}, {sqltuple, _FieldsR}) ->
    false.

get_inter_elem({sqltuple, [{?ATTR1ST, Attr1L},
                           {?ATTR2ND, Attr2L},
                           {?ATTR3RD, Attr3L}]},
               {sqltuple, [{?ATTR1ST, Attr1R},
                           {?ATTR3RD, Attr3R},
                           {?ATTR4TH, Attr4R}]}) ->
    {sqltuple, [{?ATTR1ST, Attr1L}, {?ATTR2ND, Attr2L}, {?ATTR3RD, Attr3L},
                {?ATTR1ST, Attr1R}, {?ATTR3RD, Attr3R}, {?ATTR4TH, Attr4R}]};
get_inter_elem({sqltuple, _FieldsL}, {sqltuple, _FieldsR}) ->
    {sqltuple, []}.

%% @doc Simple query test.
simple_query_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    %% Set the delta_mode to true for all nodes.
    lists:foreach(fun(Node) ->
                        ok = rpc:call(Node, lasp_config, set, [mode, delta_based])
                  end, Nodes),
    %% Set the incremental_computation_mode to true for all nodes.
    lists:foreach(fun(Node) ->
                        ok = rpc:call(Node, lasp_config, set,
                                      [incremental_computation_mode, true])
                  end, Nodes),

    %% Enable deltas.
    ok = lasp_config:set(mode, delta_based),

    ?assertMatch(delta_based, lasp_config:get(mode, state_based)),

    %% Enable incremental computation.
    ok = lasp_config:set(incremental_computation_mode, true),

    ?assertMatch(true, lasp_config:get(incremental_computation_mode, false)),

    lager:info("test start!"),

    {ok, {Table1, _, _, _}} = lasp:declare(?SET),

    ?assertMatch({ok, _},
                 lasp:update(Table1, {add_all, [{sqltuple, [{?ATTR1ST, 1},
                                                            {?ATTR2ND, 2},
                                                            {?ATTR3RD, 3}]},
                                                {sqltuple, [{?ATTR1ST, 2},
                                                            {?ATTR2ND, 4},
                                                            {?ATTR3RD, 6}]},
                                                {sqltuple, [{?ATTR1ST, 3},
                                                            {?ATTR2ND, 6},
                                                            {?ATTR3RD, 9}]},
                                                {sqltuple, [{?ATTR1ST, 4},
                                                            {?ATTR2ND, 8},
                                                            {?ATTR3RD, 12}]},
                                                {sqltuple, [{?ATTR1ST, 5},
                                                            {?ATTR2ND, 10},
                                                            {?ATTR3RD, 15}]}]}, a)),

    timer:sleep(4000),

    lager:info("query start!"),

    %% SELECT ?ATTR1ST,?ATTR3RD FROM Table1 WHERE ?ATTR2ND > 5
    {ok, {Temp1, _, _, _}} = lasp:declare(?SET),
    Function = fun({sqltuple, [{?ATTR1ST, _Attr1},
                               {?ATTR2ND, Attr2},
                               {?ATTR3RD, _Attr3}]}) ->
                       Attr2 > 5
               end,
    ?assertMatch(ok, lasp:filter(Table1, Function, Temp1)),
    timer:sleep(4000),
    {ok, {Result, _, _, _}} = lasp:declare(?SET),
    Function2 = fun({sqltuple, [{?ATTR1ST, Attr1},
                                {?ATTR2ND, _Attr2},
                                {?ATTR3RD, Attr3}]}) ->
                       {sqltuple, [{?ATTR1ST, Attr1}, {?ATTR3RD, Attr3}]}
                end,
    ?assertMatch(ok, lasp:map(Temp1, Function2, Result)),

    timer:sleep(4000),

    lager:info("wait the result!"),

    {ok, {_, _, _, ResultV1}} = lasp:read(Result, {strict, undefined}),
    ?assertEqual(sets:from_list([{sqltuple, [{?ATTR1ST, 3}, {?ATTR3RD, 9}]},
                                 {sqltuple, [{?ATTR1ST, 4}, {?ATTR3RD, 12}]},
                                 {sqltuple, [{?ATTR1ST, 5}, {?ATTR3RD, 15}]}]),
                 lasp_type:query(?SET, ResultV1)),

    lager:info("add some tuples!"),

    ?assertMatch({ok, _},
                 lasp:update(Table1, {add_all, [{sqltuple, [{?ATTR1ST, 6},
                                                            {?ATTR2ND, 3},
                                                            {?ATTR3RD, 18}]},
                                                {sqltuple, [{?ATTR1ST, 7},
                                                            {?ATTR2ND, 12},
                                                            {?ATTR3RD, 21}]}]}, a)),

    timer:sleep(4000),

    {ok, {_, _, _, ResultV2}} = lasp:read(Result, {strict, ResultV1}),
    ?assertEqual(sets:from_list([{sqltuple, [{?ATTR1ST, 3}, {?ATTR3RD, 9}]},
                                 {sqltuple, [{?ATTR1ST, 4}, {?ATTR3RD, 12}]},
                                 {sqltuple, [{?ATTR1ST, 5}, {?ATTR3RD, 15}]},
                                 {sqltuple, [{?ATTR1ST, 7}, {?ATTR3RD, 21}]}]),
                 lasp_type:query(?SET, ResultV2)),

    lager:info("remove a tuple!"),

    ?assertMatch({ok, _},
                 lasp:update(Table1,
                             {rmv, {sqltuple, [{?ATTR1ST, 1},
                                               {?ATTR2ND, 2},
                                               {?ATTR3RD, 3}]}},
                             a)),

    timer:sleep(4000),

    {ok, {_, _, _, ResultV3}} = lasp:read(Result, ResultV2),
    ?assertEqual(sets:from_list([{sqltuple, [{?ATTR1ST, 3}, {?ATTR3RD, 9}]},
                                 {sqltuple, [{?ATTR1ST, 4}, {?ATTR3RD, 12}]},
                                 {sqltuple, [{?ATTR1ST, 5}, {?ATTR3RD, 15}]},
                                 {sqltuple, [{?ATTR1ST, 7}, {?ATTR3RD, 21}]}]),
                 lasp_type:query(?SET, ResultV3)),

    lager:info("remove another tuple!"),

    ?assertMatch({ok, _},
                 lasp:update(Table1,
                             {rmv, {sqltuple, [{?ATTR1ST, 3},
                                               {?ATTR2ND, 6},
                                               {?ATTR3RD, 9}]}},
                             a)),

    timer:sleep(4000),

    {ok, {_, _, _, ResultV4}} = lasp:read(Result, {strict, ResultV2}),
    ?assertEqual(sets:from_list([{sqltuple, [{?ATTR1ST, 4}, {?ATTR3RD, 12}]},
                                 {sqltuple, [{?ATTR1ST, 5}, {?ATTR3RD, 15}]},
                                 {sqltuple, [{?ATTR1ST, 7}, {?ATTR3RD, 21}]}]),
                 lasp_type:query(?SET, ResultV4)),

    lager:info("test end!"),

    ok.

%% @doc Simple join query test.
simple_join_query_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    %% Set the delta_mode to true for all nodes.
    lists:foreach(fun(Node) ->
                        ok = rpc:call(Node, lasp_config, set, [mode, delta_based])
                  end, Nodes),
    %% Set the incremental_computation_mode to true for all nodes.
    lists:foreach(fun(Node) ->
                        ok = rpc:call(Node, lasp_config, set,
                                      [incremental_computation_mode, true])
                  end, Nodes),

    %% Enable deltas.
    ok = lasp_config:set(mode, delta_based),

    ?assertMatch(delta_based, lasp_config:get(mode, state_based)),

    %% Enable incremental computation.
    ok = lasp_config:set(incremental_computation_mode, true),

    ?assertMatch(true, lasp_config:get(incremental_computation_mode, false)),

    lager:info("test start!"),

    {ok, {Table1, _, _, _}} = lasp:declare(?SET),

    ?assertMatch({ok, _},
                 lasp:update(Table1, {add_all, [{sqltuple, [{?ATTR1ST, 1},
                                                            {?ATTR2ND, 2},
                                                            {?ATTR3RD, 3}]},
                                                {sqltuple, [{?ATTR1ST, 2},
                                                            {?ATTR2ND, 4},
                                                            {?ATTR3RD, 6}]},
                                                {sqltuple, [{?ATTR1ST, 3},
                                                            {?ATTR2ND, 6},
                                                            {?ATTR3RD, 9}]},
                                                {sqltuple, [{?ATTR1ST, 4},
                                                            {?ATTR2ND, 8},
                                                            {?ATTR3RD, 12}]},
                                                {sqltuple, [{?ATTR1ST, 5},
                                                            {?ATTR2ND, 10},
                                                            {?ATTR3RD, 15}]}]}, a)),

    timer:sleep(4000),

    {ok, {Table2, _, _, _}} = lasp:declare(?SET),

    ?assertMatch({ok, _},
                 lasp:update(Table2, {add_all, [{sqltuple, [{?ATTR1ST, 1},
                                                            {?ATTR3RD, 3},
                                                            {?ATTR4TH, 4}]},
                                                {sqltuple, [{?ATTR1ST, 2},
                                                            {?ATTR3RD, 9},
                                                            {?ATTR4TH, 8}]},
                                                {sqltuple, [{?ATTR1ST, 3},
                                                            {?ATTR3RD, 15},
                                                            {?ATTR4TH, 12}]},
                                                {sqltuple, [{?ATTR1ST, 4},
                                                            {?ATTR3RD, 21},
                                                            {?ATTR4TH, 16}]},
                                                {sqltuple, [{?ATTR1ST, 5},
                                                            {?ATTR3RD, 6},
                                                            {?ATTR4TH, 20}]}]}, a)),

    timer:sleep(4000),

    lager:info("query start!"),

    %% SELECT * FROM Table1 JOIN Table2 ON Table1.ATTR3RD = Table2.ATTR3RD
    {ok, {Result, _, _, _}} = lasp:declare(?SET),
    ?assertMatch(ok, lasp:intersection(Table1, Table2, Result)),

    timer:sleep(4000),

    lager:info("wait the result!"),

    {ok, {_, _, _, ResultV1}} = lasp:read(Result, {strict, undefined}),
    ?assertEqual(sets:from_list(
                   [{sqltuple, [{?ATTR1ST, 1}, {?ATTR2ND, 2}, {?ATTR3RD, 3},
                                {?ATTR1ST, 1}, {?ATTR3RD, 3}, {?ATTR4TH, 4}]},
                    {sqltuple, [{?ATTR1ST, 2}, {?ATTR2ND, 4}, {?ATTR3RD, 6},
                                {?ATTR1ST, 5}, {?ATTR3RD, 6}, {?ATTR4TH, 20}]},
                    {sqltuple, [{?ATTR1ST, 3}, {?ATTR2ND, 6}, {?ATTR3RD, 9},
                                {?ATTR1ST, 2}, {?ATTR3RD, 9}, {?ATTR4TH, 8}]},
                    {sqltuple, [{?ATTR1ST, 5}, {?ATTR2ND, 10}, {?ATTR3RD, 15},
                                {?ATTR1ST, 3}, {?ATTR3RD, 15}, {?ATTR4TH, 12}]}]),
                 lasp_type:query(?SET, ResultV1)),

    lager:info("test end!"),

    ok.
