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

-module(lasp_storage_backend).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-include("lasp.hrl").

-callback start(atom())-> {ok, store()} | ignore | {error, term()}.

-callback put(store(), id(), variable()) -> ok | {error, atom()}.

-callback get(store(), id()) ->
    {ok, variable()} | {error, not_found} | {error, atom()}.

-callback update(store(), id(), function()) ->
    {ok, any()} | error | {error, atom()}.

-callback update_all(store(), function()) ->
    {ok, any()} | error | {error, atom()}.

-callback fold(store(), function(), term()) -> {ok, term()}.

-callback reset(store()) -> ok.
