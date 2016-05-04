%% -------------------------------------------------------------------
%%
%% riak_dt_lwwreg: A DVVSet based last-write-wins register
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc
%% An LWW Register CRDT.
%%
%% @reference Marc Shapiro, Nuno Preguiça, Carlos Baquero, Marek Zawirski (2011) A comprehensive study of
%% Convergent and Commutative Replicated Data Types. http://hal.upmc.fr/inria-00555588/
%%
%% @end

-module(lasp_lwwreg).
-behaviour(riak_dt).
-behaviour(lasp_dt).

-export([new/0, value/1, value/2, update/3, merge/2,
         equal/2, to_binary/1, from_binary/1, stats/1, stat/2]).
-export([parent_clock/2, update/4]).
-export([to_binary/2]).
-export([to_version/2]).

-export([new/1, update_delta/3]).

%% EQC API
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-export([gen_op/0, gen_op/1, update_expected/3, eqc_state_value/1, init_state/0, generate/0]).
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([lwwreg/0, lwwreg_op/0]).

-opaque lwwreg() :: {term(), non_neg_integer()}.

-type lwwreg_op() :: {assign, term(), non_neg_integer()}  | {assign, term()}.

-type lww_q() :: timestamp.

%% @doc Create a new, empty `lwwreg()'
-spec new() -> lwwreg().
new() ->
    {<<>>, 0}.

-spec new(list()) -> lwwreg().
new(_) ->
    new().

-spec parent_clock(riak_dt_vclock:vclock(), lwwreg()) -> lwwreg().
parent_clock(_Clock, Reg) ->
    Reg.

%% @doc The single total value of a `gcounter()'.
-spec value(lwwreg()) -> term().
value({Value, _TS}) ->
    Value.

%% @doc query for this `lwwreg()'.
%% `timestamp' is the only query option.
-spec value(lww_q(), lwwreg()) -> non_neg_integer().
value(timestamp, {_V, TS}) ->
    TS.

%% @doc Assign a `Value' to the `lwwreg()'
%% associating the update with time `TS'
-spec update(lwwreg_op(), term(), lwwreg()) ->
                    {ok, lwwreg()}.
update({assign, Value, TS}, _Actor, {_OldVal, OldTS}) when is_integer(TS), TS > 0, TS >= OldTS ->
    {ok, {Value, TS}};
update({assign, _Value, _TS}, _Actor, OldLWWReg) ->
    {ok, OldLWWReg};
%% For when users don't provide timestamps
%% don't think it is a good idea to mix server and client timestamps
update({assign, Value}, _Actor, {OldVal, OldTS}) ->
    MicroEpoch = make_micro_epoch(),
    LWW = case MicroEpoch > OldTS of
              true ->
                  {Value, MicroEpoch};
              false ->
                  {OldVal, OldTS}
          end,
    {ok, LWW}.

update(Op, Actor, Reg, _Ctx) ->
    update(Op, Actor, Reg).

%% @todo Not implemented.
update_delta(Op, Actor, Reg) ->
    update(Op, Actor, Reg).

make_micro_epoch() ->
    {Mega, Sec, Micro} = os:timestamp(),
    (Mega * 1000000 + Sec) * 1000000 + Micro.

%% @doc Merge two `lwwreg()'s to a single `lwwreg()'. This is the Least Upper Bound
%% function described in the literature.
-spec merge(lwwreg(), lwwreg()) -> lwwreg().
merge({Val1, TS1}, {_Val2, TS2}) when TS1 > TS2 ->
    {Val1, TS1};
merge({_Val1, TS1}, {Val2, TS2}) when TS2 > TS1 ->
    {Val2, TS2};
merge(LWWReg1, LWWReg2) when LWWReg1 >= LWWReg2 ->
    LWWReg1;
merge(_LWWReg1, LWWReg2) ->
    LWWReg2.

%% @doc Are two `lwwreg()'s structurally equal? This is not `value/1' equality.
%% Two registers might represent the value `armchair', and not be `equal/2'. Equality here is
%% that both registers contain the same value and timestamp.
-spec equal(lwwreg(), lwwreg()) -> boolean().
equal({Val, TS}, {Val, TS}) ->
    true;
equal(_, _) ->
    false.

-spec stats(lwwreg()) -> [{atom(), number()}].
stats(LWW) ->
    [{value_size, stat(value_size, LWW)}].

-spec stat(atom(), lwwreg()) -> number() | undefined.
stat(value_size, {Value, _}=_LWW) ->
    erlang:external_size(Value);
stat(_, _) -> undefined.

-include_lib("riak_dt/include/riak_dt_tags.hrl").
-define(TAG, ?DT_LWWREG_TAG).
-define(V1_VERS, 1).

%% @doc Encode an effecient binary representation of an `lwwreg()'
-spec to_binary(lwwreg()) -> binary().
to_binary(LWWReg) ->
    <<?TAG:8/integer, ?V1_VERS:8/integer, (riak_dt:to_binary(LWWReg))/binary>>.

-spec to_binary(Vers :: pos_integer(), lwwreg()) -> {ok, binary()} | ?UNSUPPORTED_VERSION.
to_binary(1, LWW) ->
    {ok, to_binary(LWW)};
to_binary(Vers, _LWW) ->
    ?UNSUPPORTED_VERSION(Vers).

%% @doc Decode binary `lwwreg()'
-spec from_binary(binary()) -> {ok, lwwreg()} | ?UNSUPPORTED_VERSION | ?INVALID_BINARY.
from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>) ->
    {ok, riak_dt:from_binary(Bin)};
from_binary(<<?TAG:8/integer, Vers:8/integer, _Bin/binary>>) ->
    ?UNSUPPORTED_VERSION(Vers);
from_binary(_B) ->
    ?INVALID_BINARY.

-spec to_version(pos_integer(), lwwreg()) -> lwwreg().
to_version(_Version, LWW) ->
    LWW.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).
eqc_value_test_() ->
    crdt_statem_eqc:run(?MODULE, 1000).

%% EQC generator
generate() ->
    ?LET({Op, Actor}, {gen_op(), char()},
         begin
             {ok, Lww} = riak_dt_lwwreg:update(Op, Actor, riak_dt_lwwreg:new()),
             Lww
         end).

init_state() ->
    {<<>>, 0}.

gen_op(_Size) ->
    gen_op().

gen_op() ->
    ?LET(TS, largeint(), {assign, binary(), abs(TS)}).

update_expected(_ID, {assign, Val, TS}, {OldVal, OldTS}) ->
    case TS >= OldTS of
        true ->
            {Val, TS};
        false ->
            {OldVal, OldTS}
    end;
update_expected(_ID, _Op, Prev) ->
    Prev.

eqc_state_value({Val, _TS}) ->
    Val.
-endif.

new_test() ->
    ?assertEqual({<<>>, 0}, new()).

value_test() ->
    Val1 = "the rain in spain falls mainly on the plane",
    LWWREG1 = {Val1, 19090},
    LWWREG2 = new(),
    ?assertEqual(Val1, value(LWWREG1)),
    ?assertEqual(<<>>, value(LWWREG2)).

update_assign_test() ->
    LWW0 = new(),
    {ok, LWW1} = update({assign, value1, 2}, actor1, LWW0),
    {ok, LWW2} = update({assign, value0, 1}, actor1, LWW1),
    ?assertEqual({value1, 2}, LWW2),
    {ok, LWW3} = update({assign, value2, 3}, actor1, LWW2),
    ?assertEqual({value2, 3}, LWW3).

update_assign_ts_test() ->
    LWW0 = new(),
    {ok, LWW1} = update({assign, value0}, actr, LWW0),
    {ok, LWW2} = update({assign, value1}, actr, LWW1),
    ?assertMatch({value1, _}, LWW2).

merge_test() ->
    LWW1 = {old_value, 3},
    LWW2 = {new_value, 4},
    ?assertEqual({<<>>, 0}, merge(new(), new())),
    ?assertEqual({new_value, 4}, merge(LWW1, LWW2)),
    ?assertEqual({new_value, 4}, merge(LWW2, LWW1)).

equal_test() ->
    LWW1 = {value1, 1000},
    LWW2 = {value1, 1000},
    LWW3 = {value1, 1001},
    LWW4 = {value2, 1000},
    ?assertNot(equal(LWW1, LWW3)),
    ?assert(equal(LWW1, LWW2)),
    ?assertNot(equal(LWW4, LWW1)).

roundtrip_bin_test() ->
    LWW = new(),
    {ok, LWW1} = update({assign, 2}, a1, LWW),
    {ok, LWW2} = update({assign, 4}, a2, LWW1),
    {ok, LWW3} = update({assign, 89}, a3, LWW2),
    {ok, LWW4} = update({assign, <<"this is a binary">>}, a4, LWW3),
    Bin = to_binary(LWW4),
    {ok, Decoded} = from_binary(Bin),
    ?assert(equal(LWW4, Decoded)).

query_test() ->
    LWW = new(),
    {ok, LWW1} = update({assign, value, 100}, a1, LWW),
    ?assertEqual(100, value(timestamp, LWW1)).

stat_test() ->
    LWW = new(),
    {ok, LWW1} = update({assign, <<"abcd">>}, 1, LWW),
    ?assertEqual([{value_size, 11}], stats(LWW)),
    ?assertEqual([{value_size, 15}], stats(LWW1)),
    ?assertEqual(15, stat(value_size, LWW1)),
    ?assertEqual(undefined, stat(actor_count, LWW1)).
-endif.
