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
-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% lasp_storage_backend callbacks
-export([put/3,
         update/3,
         update_all/2,
         get/2,
         reset/1,
         fold/3]).

-record(state, {backend, store}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start and link to calling process.
start_link(Identifier) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Identifier], []).

%%%===================================================================
%%% lasp_storage_backend callbacks
%%%===================================================================

%% @doc Write a record to the backend.
put(Ref, Id, Record) ->
    gen_server:call(Ref, {put, Id, Record}, ?TIMEOUT).

%% @doc In-place update given a mutation function.
update(Ref, Id, Function) ->
    % lager:info("~p in the update for id ~p and ref ~p", [?MODULE, Id, Ref]),
    Result = gen_server:call(Ref, {update, Id, Function}, ?TIMEOUT),
    % lager:info("~p out of the update with ref ~p and result ~p", [?MODULE, Ref, Result]),
    % Trace = try throw(42) catch 42 -> erlang:get_stacktrace() end,
    % lager:info("=> trace: ~p", [Trace]) ,
    Result.

%% @doc Update all objects given a mutation function.
update_all(Ref, Function) ->
    % lager:info("~p in the update_all", [?MODULE]),
    Result = gen_server:call(Ref, {update_all, Function}, ?TIMEOUT),
    % lager:info("~p out of the update_all", [?MODULE]),
    Result.

%% @doc Retrieve a record from the backend.
get(Ref, Id) ->
    % lager:info("~p in the get", [?MODULE]),
    Result = gen_server:call(Ref, {get, Id}, ?TIMEOUT),
    % lager:info("~p out of the get", [?MODULE]),
    Result.

%% @doc Fold operation.
fold(Ref, Function, Acc) ->
    lager:info("~p in the fold with ref ~p and timeout ~p", [?MODULE, Ref, ?TIMEOUT]),
    Result = gen_server:call(Ref, {fold, Function, Acc}, ?TIMEOUT),
    % lager:info("~p out of the fold with ref ~p", [?MODULE, Ref]),
    Result.

%% @doc Reset all application state.
reset(Ref) ->
    % lager:info("~p in the reset", [?MODULE]),
    Result = gen_server:call(Ref, reset, ?TIMEOUT),
    Result.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
init([Identifier]) ->
    %% Select the appropriate backend.
    Backend = backend(),

    %% Schedule waiting threads pruning.
    schedule_waiting_threads_pruning(),

    %% Start the storage backend.
    case Backend:start_link(Identifier) of
        {ok, StorePid} ->
            _ = lager:info("Backend ~p initialized: ~p", [Backend, StorePid]),
            {ok, #state{backend=Backend, store=StorePid}};
        {error, {already_started, StorePid}} ->
            _ = lager:info("Backend ~p initialized: ~p", [Backend, StorePid]),
            {ok, #state{backend=Backend, store=StorePid}};
        {error, Reason} ->
            _ = lager:error("Failed to initialize backend ~p: ~p", [Backend, Reason]),
            {stop, Reason}
    end.

%% Proxy calls to the storage instance.
handle_call({get, Id}, _From, #state{store=Store}=State) ->
    Result = gen_server:call(Store, {get, Id}, ?TIMEOUT),
    {reply, Result, State};
handle_call({put, Id, Record}, _From, #state{store=Store}=State) ->
    Result = gen_server:call(Store, {put, Id, Record}, ?TIMEOUT),
    {reply, Result, State};
handle_call({update, Id, Function}, _From, #state{store=Store}=State) ->
    Result = gen_server:call(Store, {update, Id, Function}, ?TIMEOUT),
    {reply, Result, State};
handle_call({update_all, Function}, _From, #state{store=Store}=State) ->
    Result = gen_server:call(Store, {update_all, Function}, ?TIMEOUT),
    {reply, Result, State};
handle_call({fold, Function, Acc0}, _From, #state{store=Store}=State) ->
    % lager:info("at the inner fold of ~p", [?MODULE]),
    Result = gen_server:call(Store, {fold, Function, Acc0}, ?TIMEOUT),
    % lager:info("exiting the inner fold of ~p", [?MODULE]),
    {reply, Result, State};
handle_call(reset, _From, #state{store=Store}=State) ->
    Result = gen_server:call(Store, reset, ?TIMEOUT),
    {reply, Result, State};

handle_call(Msg, _From, State) ->
    _ = lager:warning("Unhandled call messages: ~p", [Msg]),
    {reply, ok, State}.

%% @private
handle_cast(Msg, State) ->
    _ = lager:warning("Unhandled cast messages: ~p", [Msg]),
    {noreply, State}.

%% @private
handle_info(waiting_threads_pruning, #state{store=Store}=State) ->
    Mutator = fun({Id, #dv{waiting_threads=WaitingThreads0}=Value}) ->
        GCFun = fun({_, _, From, _, _}) ->
            case is_pid(From) of
                true ->
                    is_process_alive(From);
                false ->
                    true
            end
        end,
        WaitingThreads = lists:filter(GCFun, WaitingThreads0),
        {Value#dv{waiting_threads=WaitingThreads}, Id}
    end,
    gen_server:call(Store, {update_all, Mutator}, ?TIMEOUT),

    %% Schedule next message.
    schedule_waiting_threads_pruning(),

    {noreply, State};

handle_info(Msg, State) ->
    _ = lager:warning("Unhandled info messages at module ~p: ~p", [?MODULE, Msg]),
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

%% @private
schedule_waiting_threads_pruning() ->
    timer:send_after(10000, waiting_threads_pruning).