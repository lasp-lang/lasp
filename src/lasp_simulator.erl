%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Christopher S. Meiklejohn.  All Rights Reserved.
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

-module(lasp_simulator).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-export([run/1]).

%% @doc Prototype new simulator harness.
%%
%% @clippy Hey, it looks like you're writing a State monad here!  Would
%%         you like some help?
%%
run(Module) ->
    {ok, State} = Module:init(),

    %% Launch client processes.
    {ok, State1} = Module:clients(State),

    %% Initialize simulation.
    {ok, State2} = Module:simulate(State1),

    %% Wait until we receive num events.
    {ok, State3} = Module:wait(State2),

    %% Terminate all clients.
    {ok, State4} = Module:terminate(State3),

    %% Finish and summarize.
    {ok, State5} = Module:summarize(State4),

    {ok, State5}.
