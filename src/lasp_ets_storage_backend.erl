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

-module(lasp_ets_storage_backend).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-include("lasp.hrl").

-export([start/1,
         put/3,
         get/2]).

-behaviour(lasp_storage_backend).

%% @doc Initialize the backend.
-spec start(atom()) -> {ok, atom()} | {error, atom()}.
start(Store) ->
    try
        Store = ets:new(Store, [set,
                                named_table,
                                public,
                                {write_concurrency, true}]),
        {ok, Store}
    catch
        _:Reason ->
            lager:info("Backend initialization failed!"),
            {error, Reason}
    end.

%% @doc Write a record to the backend.
-spec put(store(), id(), variable()) -> ok.
put(Store, Id, Record) ->
    true = ets:insert(Store, {Id, Record}),
    ok.

%% @doc Retrieve a record from the backend.
-spec get(store(), id()) -> {ok, variable()} | {error, not_found} | {error, atom()}.
get(Store, Id) ->
    case ets:lookup(Store, Id) of
        [{_Key, Record}] ->
            {ok, Record};
        [] ->
            {error, not_found}
    end.
