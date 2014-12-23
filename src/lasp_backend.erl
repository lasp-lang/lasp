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

-module(lasp_backend).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-include("lasp.hrl").

-callback next(id(), store()) -> {ok, id()}.
-callback next(id(), store(), function()) -> {ok, id()}.

-callback read(id(), store()) -> {ok, type(), value(), id()}.
-callback read(id(), value(), store()) -> {ok, type(), value(), id()}.
-callback read(id(), value(), store(), pid(), function(), function()) ->
    {ok, type(), value(), id()}.

-callback fetch(id(), id(), pid(), store()) -> {ok, id()}.
-callback fetch(id(), id(), pid(), store(), function(), function(),
                function(), function()) -> term().

-callback reply_fetch(id(), pid(), #dv{}, store()) -> {ok, id()}.

-callback declare(store()) -> {ok, id()}.
-callback declare(type(), store()) -> {ok, id()}.
-callback declare(id(), type(), store()) -> {ok, id()}.

-callback bind_to(id(), value(), store(), function(), pid()) -> any().

-callback bind(id(), value(), store()) -> {ok, id()}.
-callback bind(id(), value(), store(), function(), function()) -> {ok, id()}.

-callback is_det(id(), store()) -> {ok, bound()}.

-callback thread(module(), func(), args(), store()) -> {ok, pid()}.

-callback next_key(undefined | id(), type(), store()) -> id().

-callback write(type(), value(), id(), id(), store()) -> ok.
-callback write(type(), value(), id(), id(), store(), function()) -> ok.

-callback wait_needed(id(), store()) -> {ok, threshold()}.
-callback wait_needed(id(), threshold(), store()) -> {ok, threshold()}.
-callback wait_needed(id(), threshold(), store(), pid(), function(), function()) -> {ok, threshold()}.

-callback notify_value(id(), value(), store(), function()) -> ok.
-callback notify_all(function(), list(#dv{}), value()) -> ok.

-callback reply_to_all(list(pid() | pending_threshold()), term()) ->
    {ok, list(pending_threshold())}.
-callback reply_to_all(list(pid() | pending_threshold()),
                       list(pid() | pending_threshold()),
                       term()) ->
    {ok, list(pending_threshold())}.

-callback filter(id(), function(), id(), store()) -> {ok, pid()}.
-callback filter(id(), function(), id(), store(), function(), function()) -> {ok, pid()}.

-callback fold(store(), id(), function(), id(), function(), function()) -> function().
