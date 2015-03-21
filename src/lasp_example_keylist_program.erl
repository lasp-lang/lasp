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

-module(lasp_example_keylist_program).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-behavior(lasp_program).

-export([init/1,
         process/5,
         execute/2,
         value/1,
         merge/1,
         sum/1]).

-record(state, {type, id, previous}).

-define(CORE, lasp_core).
-define(TYPE, riak_dt_gset).

%% @doc Initialize an or-set as an accumulator.
init(Store) ->
    {ok, Id} = ?CORE:declare(?TYPE, Store),
    {ok, #state{id=Id}}.

%% @doc Notification from the system of an event.
process(Object, _Reason, Idx, #state{id=Id}=State, Store) ->
    {ok, _} = ?CORE:update(Id, {add, Object}, Idx, Store),
    {ok, State}.

%% @doc Return the result.
execute(#state{id=Id, previous=Previous}, Store) ->
    {ok, {_, _, Value}} = ?CORE:read(Id, Previous, Store),
    {ok, Value}.

%% @doc Return the result from a merged response
value(Merged) ->
    {ok, ?TYPE:value(Merged)}.

%% @doc Given a series of outputs, take each one and merge it.
merge(Outputs) ->
    Value = ?TYPE:new(),
    Merged = lists:foldl(fun(X, Acc) -> ?TYPE:merge(X, Acc) end, Value, Outputs),
    {ok, Merged}.

%% @doc Computing a sum accorss nodes is the same as as performing the
%%      merge of outputs between a replica, when dealing with the
%%      set.
sum(Outputs) ->
    Value = ?TYPE:new(),
    Sum = lists:foldl(fun(X, Acc) -> X ++ Acc end, Value, Outputs),
    {ok, Sum}.
