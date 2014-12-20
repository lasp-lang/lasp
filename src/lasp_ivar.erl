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

-export([new/0, update/3, merge/2]).

%% EQC API
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
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
-spec merge(term(), term()) -> term().
merge(A, undefined) ->
    A;
merge(undefined, B) ->
    B;
merge(A, A) ->
    A.

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
