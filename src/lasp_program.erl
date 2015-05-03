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

-module(lasp_program).

-include("lasp.hrl").

%% Mutations

%% @doc Initialize the program.  Perform whatever initial configuration
%%      is required.
-callback init(store()) -> {ok, state()}.

%% @doc Given a notification from the underlying system about an object
%%      having been put, handed off, or deleted, notify all programs that
%%      need to be notified.
-callback process(object(), reason(), actor(), state(), store()) -> {ok, state()}.

%% @doc Return the current result of a given program.
-callback execute(state(), store()) -> {value | stream, output()}.

%% Pure Functions

%% @doc Return the type of the CRDT returned by the application.
-callback type() -> type().

%% @doc Given a value to be returned to the user when executing,
%%      possibly filter some component of it.
-callback value(output()) -> output().
