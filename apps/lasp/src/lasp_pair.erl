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

-module(lasp_pair).
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

-export_type([pair/0, binary_pair/0, pair_op/0]).
-type pair() :: {{type(), term()}, {type(), term()}}.
-type binary_pair() :: binary().
-type pair_value() :: {term(), term()}.

%% @todo: Find a way to make this more significant.
-type pair_op() :: {fst, term()} | {snd, term()}.

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% @doc Return a new pair.
-spec new() -> pair().
new() ->
    new([lasp_ivar, lasp_ivar]).

%% @doc Return a new pair.
-spec new([crdt()]) -> pair().
new([Fst, Snd]) ->
    {{Fst, Fst:new()}, {Snd, Snd:new()}}.

%% @doc Update the CRDT with a given key/value pair.
-spec update(pair_op(), actor(), pair()) -> {ok, pair()}.
update({fst, Update}, Actor, {{FstType, Fst}, {SndType, Snd}}) ->
    case FstType:update(Update, Actor, Fst) of
        {ok, Value} ->
            {ok, {{FstType, Value}, {SndType, Snd}}};
        Error ->
            Error
    end;
update({snd, Update}, Actor, {{FstType, Fst}, {SndType, Snd}}) ->
    case SndType:update(Update, Actor, Snd) of
        {ok, Value} ->
            {ok, {{FstType, Fst}, {SndType, Value}}};
        Error ->
            Error
    end.

%% @doc Determine if pairs are equivalent.
-spec equal(pair(), pair()) -> boolean().
equal({{FstType, Fst1}, {SndType, Snd1}},
      {{FstType, Fst2}, {SndType, Snd2}}) ->
    FstType:equal(Fst1, Fst2) andalso SndType:equal(Snd1, Snd2);
equal(_, _) ->
    false.

%% @doc Merge function for two pairs.
-spec merge(pair(), pair()) -> pair().
merge({{FstType, Fst1}, {SndType, Snd1}},
      {{FstType, Fst2}, {SndType, Snd2}}) ->
    {{FstType, FstType:merge(Fst1, Fst2)}, {SndType, SndType:merge(Snd1, Snd2)}}.

%% @doc Return the value of the top-K CRDT.
-spec value(pair()) -> pair_value().
value({{FstType, Fst}, {SndType, Snd}}) ->
    {FstType:value(Fst), SndType:value(Snd)}.

-spec value(any(), pair()) -> pair_value().
value(_, Pair) ->
    value(Pair).

-spec precondition_context(pair()) -> pair().
precondition_context(Var) ->
    Var.

-spec stats(pair()) -> [].
stats(_) ->
    [].

-spec stat(atom(), pair()) -> undefined.
stat(_, _) -> undefined.

-include_lib("riak_dt/include/riak_dt_tags.hrl").
-define(DT_PAIR_TAG, 102).
-define(TAG, ?DT_PAIR_TAG).
-define(V1_VERS, 1).

-spec to_binary(pair()) -> binary_pair().
to_binary(IVar) ->
    <<?TAG:8/integer, ?V1_VERS:8/integer, (riak_dt:to_binary(IVar))/binary>>.

-spec to_binary(Vers :: pos_integer(), pair()) ->
    {ok, binary_pair()} | ?UNSUPPORTED_VERSION.
to_binary(1, Set) ->
    {ok, to_binary(Set)};
to_binary(Vers, _Set) ->
    ?UNSUPPORTED_VERSION(Vers).

-spec from_binary(binary_pair()) ->
    {ok, pair()} | ?UNSUPPORTED_VERSION | ?INVALID_BINARY.
from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>) ->
    riak_dt:from_binary(Bin);
from_binary(<<?TAG:8/integer, Vers:8/integer, _Bin/binary>>) ->
    ?UNSUPPORTED_VERSION(Vers);
from_binary(_B) ->
    ?INVALID_BINARY.

-spec to_version(pos_integer(), pair()) -> pair().
to_version(_Version, Pair) ->
    Pair.

-spec parent_clock(riak_dt_vclock:vclock(), pair()) -> pair().
parent_clock(_Clock, Pair) ->
    Pair.

-spec update(pair_op(), actor(), pair(),
             riak_dt:context()) -> {ok, pair()}.
update(Op, Actor, Pair, _Ctx) ->
    update(Op, Actor, Pair).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

basic_test() ->
    A1 = lasp_pair:new([lasp_pncounter, lasp_pncounter]),
    {ok, A2} = lasp_pair:update({fst, increment}, undefined, A1),

    B1 = lasp_pair:new([lasp_pncounter, lasp_pncounter]),
    {ok, B2} = lasp_pair:update({snd, decrement}, undefined, B1),

    A3 = lasp_pair:merge(A2, B2),
    B3 = lasp_pair:merge(B2, A2),

    Value = orddict:to_list(lasp_pair:value(A3)),
    ?assertEqual({1, -1}, Value),

    ?assert(lasp_pair:equal(A3, B3)),

    ok.

-endif.
