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

-module(lasp_bounded_lww_set).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("lasp.hrl").

-behaviour(riak_dt).
-behaviour(lasp_dt).

-export([new/1]).
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

-export_type([bounded_lww_set/0,
              binary_bounded_lww_set/0,
              bounded_lww_set_op/0]).
-type k() :: non_neg_integer().
-type bounded_lww_set() :: {k(), orddict:orddict()}.
-type binary_bounded_lww_set() :: binary().
-type bounded_lww_set_op() :: {add, term()}.

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% @doc Return a new set; assume bound 1.
-spec new() -> bounded_lww_set().
new() ->
    new([1]).

%% @doc Return a new set.
-spec new([non_neg_integer()]) -> bounded_lww_set().
new([K]) ->
    {K, orddict:new()}.

% %% @doc Update the CRDT with a given key/value pair.
-spec update(bounded_lww_set_op(), actor(), bounded_lww_set()) ->
    {ok, bounded_lww_set()}.
update({add, Value}, _Actor, {K, Set0}) ->
    Key = timestamp(erlang:timestamp()),
    UpdateFun = fun(_) -> Value end,
    Set = orddict:update({Key, Value}, UpdateFun, false, Set0),
    {ok, enforce_k({K, Set})}.

%% @doc Determine if two ordered dictionaries are equivalent.
-spec equal(bounded_lww_set(), bounded_lww_set()) -> boolean().
equal({K, Orddict1}, {K, Orddict2}) ->
    orddict:to_list(Orddict1) =:= orddict:to_list(Orddict2);
equal(_, _) ->
    false.

%% @doc Merge function for two CRDTs.
%% @todo Make much more efficient.
-spec merge(bounded_lww_set(), bounded_lww_set()) -> bounded_lww_set().
merge({K, A}, {K, B}) ->
    MergeFun = fun(_, AValue, BValue) -> max(AValue, BValue) end,
    Merged = orddict:merge(MergeFun, A, B),
    enforce_k({K, Merged}).

%% @doc Return the value of the CRDT.
-spec value(bounded_lww_set()) -> [term()].
value({_, Dict}) ->
    lists:foldl(fun({{_K, V}, D}, Acc) ->
                        case D of
                            true ->
                                Acc;
                            false ->
                                Acc ++ [V]
                        end
                end, [], orddict:to_list(Dict)).

-spec value(any(), bounded_lww_set()) -> [term()].
value(_, Dict) ->
    value(Dict).

%% @doc Enforce K after update or merge.
%% @todo Optimize.
-spec enforce_k(bounded_lww_set()) -> bounded_lww_set().
enforce_k({K, Dict}) ->
    Unsorted = orddict:to_list(Dict),
    SortFun = fun({K1, V1}, {K2, V2}) -> V2 =< V1 andalso K1 =< K2 end,
    Sorted = lists:sort(SortFun, Unsorted),
    %% @todo: Wrong.
    Keep = lists:sublist(Sorted, K),
    Remove = [{K1, V1, true} || {K1, V1, _} <- lists:nthtail(K, Sorted)],
    {K, orddict:from_list(Keep ++ Remove)}.

-spec precondition_context(bounded_lww_set()) -> bounded_lww_set().
precondition_context(Var) ->
    Var.

-spec stats(bounded_lww_set()) -> [].
stats(_) ->
    [].

-spec stat(atom(), bounded_lww_set()) -> undefined.
stat(_, _) -> undefined.

-include_lib("riak_dt/include/riak_dt_tags.hrl").
-define(DT_BOUNDED_LWW_SET_TAG, 103).
-define(TAG, ?DT_BOUNDED_LWW_SET_TAG).
-define(V1_VERS, 1).

-spec to_binary(bounded_lww_set()) -> binary_bounded_lww_set().
to_binary(IVar) ->
    <<?TAG:8/integer, ?V1_VERS:8/integer, (riak_dt:to_binary(IVar))/binary>>.

-spec to_binary(Vers :: pos_integer(), bounded_lww_set()) ->
    {ok, binary_bounded_lww_set()} | ?UNSUPPORTED_VERSION.
to_binary(1, Set) ->
    {ok, to_binary(Set)};
to_binary(Vers, _Set) ->
    ?UNSUPPORTED_VERSION(Vers).

-spec from_binary(binary_bounded_lww_set()) ->
    {ok, bounded_lww_set()} | ?UNSUPPORTED_VERSION | ?INVALID_BINARY.
from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>) ->
    riak_dt:from_binary(Bin);
from_binary(<<?TAG:8/integer, Vers:8/integer, _Bin/binary>>) ->
    ?UNSUPPORTED_VERSION(Vers);
from_binary(_B) ->
    ?INVALID_BINARY.

-spec to_version(pos_integer(), bounded_lww_set()) -> bounded_lww_set().
to_version(_Version, IVar) ->
    IVar.

-spec parent_clock(riak_dt_vclock:vclock(), bounded_lww_set()) ->
    bounded_lww_set().
parent_clock(_Clock, IVar) ->
    IVar.

-spec update(bounded_lww_set_op(), actor(), bounded_lww_set(),
             riak_dt:context()) -> {ok, bounded_lww_set()}.
update(Op, Actor, ORDict, _Ctx) ->
    update(Op, Actor, ORDict).

%% ===================================================================
%% EUnit tests
%% ===================================================================

-ifdef(TEST).

basic_test() ->
    A1 = ?MODULE:new(),
    B1 = ?MODULE:new(),
    {ok, A2} = ?MODULE:update({add, chris}, undefined, A1),
    {ok, B2} = ?MODULE:update({add, chris}, undefined, B1),
    A3 = ?MODULE:merge(A2, B2),
    {ok, A4} = ?MODULE:update({add, jordan}, undefined, A3),
    {ok, A5} = ?MODULE:update({add, chris}, undefined, A4),
    Value = orddict:to_list(?MODULE:value(A5)),
    ?assertEqual([chris], Value).

-endif.

%% @private
timestamp({Mega, Secs, Micro}) ->
        Mega * 1000 * 1000 * 1000 * 1000 + Secs * 1000 * 1000 + Micro.
