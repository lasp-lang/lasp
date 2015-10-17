%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Christopher Meiklejohn.  All Rights Reserved.
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
%% @doc Top-K CRDT.
%%
%% @reference Navalho, D., Duarte, S., Pregucia, N.
%%            "A Study of CRDTs that do computations"
%%            http://dl.acm.org/citation.cfm?id=2745948
%%
-module(lasp_top_k_var).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("lasp.hrl").

-behaviour(riak_dt).

-export([new/0,
         value/1,
         update/3,
         merge/2,
         equal/2,
         to_binary/1,
         from_binary/1,
         value/2,
         precondition_context/1,
         stats/1,
         stat/2]).
-export([update/4,
         parent_clock/2]).
-export([to_binary/2]).
-export([to_version/2]).

-export_type([top_k_var/0, binary_top_k_var/0, top_k_var_op/0]).
-type k() :: non_neg_integer().
-type top_k_var() :: {k(), orddict:orddict()}.
-type binary_top_k_var() :: binary().
-type top_k_var_op() :: {set, term(), term()}.

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% EQC API
-ifdef(EQC).
-export([gen_op/0]).
-endif.

%% @doc Return a new top-k variable; assumes with no arguments top-1.
-spec new() -> top_k_var().
new() ->
    {1, orddict:new()}.

%% @doc Update the CRDT with a given key/value pair.
-spec update(top_k_var_op(), actor(), top_k_var()) -> {ok, top_k_var()}.
update({set, Key, Value}, _Actor, {K, Var0}) ->
    UpdateFun = fun(Value0) ->
                        max(Value, Value0)
                end,
    Var = orddict:update(Key, UpdateFun, Value, Var0),
    {ok, enforce_k({K, Var})}.

%% @doc Determine if two ordered dictionaries are equivalent.
-spec equal(top_k_var(), top_k_var()) -> boolean().
equal({K, Orddict1}, {K, Orddict2}) ->
    orddict:to_list(Orddict1) =:= orddict:to_list(Orddict2);
equal(_, _) ->
    false.

%% @doc Merge function for two top-k CRDTs.
%% @todo Make much more efficient.
-spec merge(top_k_var(), top_k_var()) -> top_k_var().
merge({K, A}, {K, B}) ->
    MergeFun = fun(_, AValue, BValue) -> max(AValue, BValue) end,
    Merged = orddict:merge(MergeFun, A, B),
    enforce_k({K, Merged}).

%% @doc Return the value of the top-K CRDT.
-spec value(top_k_var()) -> orddict:orddict().
value({_, Dict}) ->
    Dict.

-spec value(any(), top_k_var()) -> orddict:orddict().
value(_, Dict) ->
    value(Dict).

%% @doc Enforce K after update or merge.
%% @todo Optimize.
-spec enforce_k(top_k_var()) -> top_k_var().
enforce_k({K, Dict}) ->
    Unsorted = orddict:to_list(Dict),
    SortFun = fun({K1, V1}, {K2, V2}) -> V2 =< V1 andalso K1 =< K2 end,
    Sorted = lists:sort(SortFun, Unsorted),
    Truncated = lists:sublist(Sorted, K),
    {K, orddict:from_list(Truncated)}.

-spec precondition_context(top_k_var()) -> top_k_var().
precondition_context(Var) ->
    Var.

-spec stats(top_k_var()) -> [].
stats(_) ->
    [].

-spec stat(atom(), top_k_var()) -> undefined.
stat(_, _) -> undefined.

-include_lib("riak_dt/include/riak_dt_tags.hrl").
-define(DT_TOP_K_VAR_TAG, 101).
-define(TAG, ?DT_TOP_K_VAR_TAG).
-define(V1_VERS, 1).

-spec to_binary(top_k_var()) -> binary_top_k_var().
to_binary(IVar) ->
    <<?TAG:8/integer, ?V1_VERS:8/integer, (riak_dt:to_binary(IVar))/binary>>.

-spec to_binary(Vers :: pos_integer(), top_k_var()) -> {ok, binary_top_k_var()} | ?UNSUPPORTED_VERSION.
to_binary(1, Set) ->
    {ok, to_binary(Set)};
to_binary(Vers, _Set) ->
    ?UNSUPPORTED_VERSION(Vers).

-spec from_binary(binary_top_k_var()) -> {ok, top_k_var()} | ?UNSUPPORTED_VERSION | ?INVALID_BINARY.
from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>) ->
    riak_dt:from_binary(Bin);
from_binary(<<?TAG:8/integer, Vers:8/integer, _Bin/binary>>) ->
    ?UNSUPPORTED_VERSION(Vers);
from_binary(_B) ->
    ?INVALID_BINARY.

-spec to_version(pos_integer(), top_k_var()) -> top_k_var().
to_version(_Version, IVar) ->
    IVar.

-spec parent_clock(riak_dt_vclock:vclock(), top_k_var()) -> top_k_var().
parent_clock(_Clock, IVar) ->
    IVar.

-spec update(top_k_var_op(), actor(), top_k_var(), riak_dt:context()) -> {ok, top_k_var()}.
update(Op, Actor, ORDict, _Ctx) ->
    update(Op, Actor, ORDict).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

basic_test() ->
    A1 = lasp_top_k_var:new(),
    B1 = lasp_top_k_var:new(),
    {ok, A2} = lasp_top_k_var:update({set, chris, 1}, undefined, A1),
    {ok, B2} = lasp_top_k_var:update({set, chris, 100}, undefined, B1),
    A3 = lasp_top_k_var:merge(A2, B2),
    {ok, A4} = lasp_top_k_var:update({set, jordan, 500}, undefined, A3),
    {ok, A5} = lasp_top_k_var:update({set, chris, 50}, undefined, A4),
    Value = orddict:to_list(lasp_top_k_var:value(A5)),
    ?assertEqual([{jordan, 500}], Value).

-endif.
