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

-module(gen_flow).
-author('Christopher Meiklejohn <christopher.meiklejohn@gmail.com>').

%% API
-export([start_link/2]).

%% Callbacks
-export([start/3]).

%%%===================================================================
%%% Behaviour
%%%===================================================================

-type state() :: term().

-callback init(list(term())) -> {ok, state()}.
-callback read(state()) -> {ok, [function()], state()}.
-callback process(list(term()), state()) -> {ok, state()}.

%%%===================================================================
%%% API
%%%===================================================================

start_link(Module, Args) ->
    Pid = proc_lib:start_link(?MODULE, start, [self(), Module, Args]),
    {ok, Pid}.

%%%===================================================================
%%% Callbacks
%%%===================================================================

%% @doc TODO
start(Parent, Module, Args) ->
    %% Initialize state.
    {ok, State} = case Module:init(Args) of
        {ok, InitState} ->
            proc_lib:init_ack(Parent, {ok, self()}),
            {ok, InitState};
        {error, Reason} ->
            exit(Reason)
    end,
    loop(Module, State, orddict:new()).

%% @doc TODO
loop(Module, State0, Cache0) ->
    %% Get self.
    Self = self(),

    %% Gather the read functions.
    {ok, ReadFuns, ReadState} = Module:read(State0),

    %% Initialize bottom values in orddict.
    DefaultedCache = lists:foldl(fun(X, C) ->
                case orddict:find(X, C) of
                    error ->
                        orddict:store(X, undefined, C);
                    {ok, _} ->
                        C
                end
        end, Cache0, lists:seq(1, length(ReadFuns))),

    %% For each readfun, spawn a linked process to request values.
    lists:foreach(fun(X) ->
            ReadFun = lists:nth(X, ReadFuns),
            CachedValue = orddict:fetch(X, DefaultedCache),
            spawn_link(fun() ->
                            Value = ReadFun(CachedValue),
                            Self ! {ok, X, Value}
                    end)
        end, lists:seq(1, length(ReadFuns))),

    %% Wait for responses.
    receive
        {ok, X, V} ->
            %% Update cache.
            Cache = orddict:store(X, V, DefaultedCache),

            %% Get current values from cache.
            RealizedCache = [Value || {_, Value}
                                      <- orddict:to_list(Cache)],

            %% Call process function.
            {ok, State} = Module:process(RealizedCache, ReadState),

            %% Wait.
            loop(Module, State, Cache)
    end.
