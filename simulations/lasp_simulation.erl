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

-module(lasp_simulation).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-type state() :: any().

%% Initialize the Lasp application.
%%
%% This callback is designed to have a majority of the Lasp
%% computational graph defined and return any state information needed
%% to run the simulation.
%%
-callback init([any()]) -> {ok, state()}.

%% Initialize a series of clients that will perform computations and
%% receive messages from the simulator.
%%
-callback clients(state()) -> {ok, state()}.

%% Simulate clients actions by sending a message to each of the clients
%% randomly.
%%
-callback simulate(state()) -> {ok, state()}.

%% Wait until all clients process all messages.
%%
%% Normally, this should be done by sending a message to the harness for
%% each messages processed, so we don't have to rely on iteratively
%% inspecting mailboxes.
%%
-callback wait(state()) -> {ok, state()}.

%% Terminate all client processes.
-callback terminate(state()) -> {ok, state()}.

%% Perform any summarization needed.
-callback summarize(state()) -> {ok, state()}.

-export([run/2]).

%% @doc Prototype new simulator harness.
%%
%% @clippy Hey, it looks like you're writing a State monad here!  Would
%%         you like some help?
%%
run(Module, Args) ->
    Pid = self(),

    spawn_link(fun() ->
                    _ = lager:info("Initializing simulation!"),
                    {ok, State} = Module:init(Args),

                    %% Unfortunately, we have to wait for the cluster to stabilize, else
                    %% some of the clients running at other node will get not_found
                    %% operations.
                    _ = lager:info("Waiting for cluster to stabilize..."),
                    timer:sleep(2000),

                    %% Launch client processes.
                    _ = lager:info("Launching clients!"),
                    {ok, State1} = Module:clients(State),

                    %% Initialize simulation.
                    _ = lager:info("Running simulation!"),
                    {ok, State2} = Module:simulate(State1),

                    %% Wait until we receive num events.
                    _ = lager:info("Waiting for event generation to complete!"),
                    {ok, State3} = Module:wait(State2),

                    %% Terminate all clients.
                    _ = lager:info("Terminating clients!"),
                    {ok, State4} = Module:terminate(State3),

                    %% Finish and summarize.
                    _ = lager:info("Summarizing results!"),
                    {ok, Term} = Module:summarize(State4),

                    _ = lager:info("Processes before termination: ~p",
                                   [length(processes())]),

                    Pid ! {ok, Term}
               end),
    receive
        {ok, Term} ->
            _ = lager:info("Processes after termination: ~p",
                           [length(processes())]),
            {ok, Term}
    end.
