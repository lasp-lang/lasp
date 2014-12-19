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

-module(derpflow_program).

-type crdt() :: riak_dt:crdt().

-callback init() -> {ok, crdt()} | {error, atom()}.
-callback execute(crdt()) -> {ok, crdt()} | {error, atom()}.
-callback execute(crdt(), term()) -> {ok, crdt()} | {error, atom()}.
-callback merge(list(crdt())) -> {ok, crdt()} | {error, atom()}.
