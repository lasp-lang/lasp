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

-module(lasp_type).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("lasp.hrl").

-export([new/1, update/4, merge/3, value/2, query/2]).

%% @doc Initialize a new variable for a given type.
new(Type) ->
    case Type of
        {T, Args} ->
            T:new(Args);
        T ->
            T:new()
    end.

%% @doc Use the proper type for performing an update.
update(Type, Operation, Actor, Value) ->
    case Type of
        {T, _Args} ->
            case mochiglobal:get(delta_mode, false) of
                true ->
                    T:update_delta(Operation, Actor, Value);
                false ->
                    T:update(Operation, Actor, Value)
            end;
        T ->
            case mochiglobal:get(delta_mode, false) of
                true ->
                    T:update_delta(Operation, Actor, Value);
                false ->
                    T:update(Operation, Actor, Value)
            end
    end.

%% @doc Call the correct merge function for a given type.
merge(Type, Value0, Value) ->
    case Type of
        {T, _Args} ->
            T:merge(Value0, Value);
        T ->
            T:merge(Value0, Value)
    end.

%% @doc Return the value of a CRDT.
value(Type, Value) ->
    case Type of
        {T, _Args} ->
            T:value(Value);
        T ->
            T:value(Value)
    end.

%% @doc Return the current value of a CRDT.
query(Type, Id) ->
    {ok, {_, _, _, Value}} = lasp:read(Id, undefined),
    case Type of
        {T, _Args} ->
            T:value(Value);
        T ->
            T:value(Value)
    end.
