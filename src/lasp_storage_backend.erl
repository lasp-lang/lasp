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

%% Behavior
-callback start_link(atom())-> 
    {ok, store()} | ignore | {error, term()}.

-callback put(store(), id(), variable()) -> 
    ok | {error, atom()}.

-callback get(store(), id()) ->
    {ok, variable()} | {error, not_found} | {error, atom()}.

-callback update(store(), id(), function()) ->
    {ok, any()} | error | {error, atom()}.

-callback update_all(store(), function()) ->
    {ok, any()} | error | {error, atom()}.

-callback fold(store(), function(), term()) -> 
    {ok, term()}.

-callback reset(store()) -> ok.

-behaviour(gen_server).

%% API
-export([do/2, 
         start_link/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {backend, store}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start and link to calling process.
start_link(Identifier) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Identifier], []).

%% @doc Execute a function against the backend.
do(Function, Args) ->
    gen_server:call(?MODULE, {do, Function, Args}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
init([Identifier]) ->
    %% Select the appropriate backend.
    Backend = backend(),

    %% Start the storage backend.
    case Backend:start_link(Identifier) of
        {ok, StorePid} ->
            {ok, #state{backend=Backend, store=StorePid}};
        {error, {already_started, StorePid}} ->
            {ok, #state{backend=Backend, store=StorePid}};
        {error, Reason} ->
            _ = lager:error("Failed to initialize backend ~p: ~p", [Backend, Reason]),
            {stop, Reason}
    end.

%% @private
handle_call({do, Function, Args}, _From, #state{backend=Backend}=State) ->
    Result = erlang:apply(Backend, Function, Args),
    {reply, Result, State};

handle_call(Msg, _From, State) ->
    _ = lager:warning("Unhandled call messages: ~p", [Msg]),
    {reply, ok, State}.

%% @private
handle_cast(Msg, State) ->
    _ = lager:warning("Unhandled cast messages: ~p", [Msg]),
    {noreply, State}.

%% @private
handle_info(Msg, State) ->
    _ = lager:warning("Unhandled info messages: ~p", [Msg]),
    {noreply, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-ifdef(TEST).

%% @doc Use the memory backend for tests.
backend() ->
    lasp_ets_storage_backend.

-else.

%% @doc Use configured backend.
backend() ->
    lasp_config:get(storage_backend, lasp_ets_storage_backend).

-endif.