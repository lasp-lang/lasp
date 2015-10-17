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
%%
%% @doc Single-assignment variable.
%%
%%      This module allows us to remove the single-assignment code path
%%      and formailize everything as lattice variables.
%%

-module(lasp_ivar).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

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

-export_type([ivar/0, binary_ivar/0, ivar_op/0]).
-type ivar() :: term().
-type binary_ivar() :: <<_:16,_:_*8>>.
-type ivar_op() :: {set, term()}.

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

%% @doc Create a new single-assignment variable.
-spec new() -> undefined.
new() ->
    undefined.

%% @doc Set the value of a single-assignment variable.
update({set, Value}, _Actor, undefined) ->
    {ok, Value}.

%% @doc Single assignment merge; undefined for two bound variables.
-spec merge(ivar(), ivar()) -> ivar().
merge(A, undefined) ->
    A;
merge(undefined, B) ->
    B;
merge(A, A) ->
    A.

%% @doc Test for equality.
equal(A, A) ->
    true;
equal(_, _) ->
    false.

-spec value(ivar()) -> ivar().
value(IVar) ->
    IVar.

-spec value(any(), ivar()) -> ivar().
value(_, IVar) ->
    value(IVar).

-spec precondition_context(ivar()) -> ivar().
precondition_context(IVar) ->
    IVar.

-spec stats(ivar()) -> [].
stats(_) ->
    [].

-spec stat(atom(), ivar()) -> undefined.
stat(_, _) -> undefined.

-include_lib("riak_dt/include/riak_dt_tags.hrl").
-define(DT_IVAR_TAG, 100).
-define(TAG, ?DT_IVAR_TAG).
-define(V1_VERS, 1).

-spec to_binary(ivar()) -> binary_ivar().
to_binary(IVar) ->
    <<?TAG:8/integer, ?V1_VERS:8/integer, (riak_dt:to_binary(IVar))/binary>>.

-spec to_binary(Vers :: pos_integer(), ivar()) -> {ok, binary_ivar()} | ?UNSUPPORTED_VERSION.
to_binary(1, Set) ->
    {ok, to_binary(Set)};
to_binary(Vers, _Set) ->
    ?UNSUPPORTED_VERSION(Vers).

-spec from_binary(binary_ivar()) -> {ok, ivar()} | ?UNSUPPORTED_VERSION | ?INVALID_BINARY.
from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>) ->
    riak_dt:from_binary(Bin);
from_binary(<<?TAG:8/integer, Vers:8/integer, _Bin/binary>>) ->
    ?UNSUPPORTED_VERSION(Vers);
from_binary(_B) ->
    ?INVALID_BINARY.

-spec to_version(pos_integer(), ivar()) -> ivar().
to_version(_Version, IVar) ->
    IVar.

-spec parent_clock(riak_dt_vclock:vclock(), ivar()) -> ivar().
parent_clock(_Clock, IVar) ->
    IVar.

-spec update(ivar_op(), actor(), ivar(), riak_dt:context()) -> {ok, ivar()}.
update(Op, Actor, ORDict, _Ctx) ->
    update(Op, Actor, ORDict).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).

%% EQC generator
gen_op() ->
    oneof([{set, int()}]).

-endif.

-endif.
