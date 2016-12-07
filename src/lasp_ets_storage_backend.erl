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

-behaviour(gen_server).
-behaviour(lasp_storage_backend).

-include("lasp.hrl").

%% lasp_storage_backend callbacks
-export([start/1,
         put/3,
         update/3,
         update_all/2, %% @todo add this function to behaviour lasp_storage_backend
         get/2,
         reset/1,
         fold/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% reference tpe
-type ref() :: atom().

%% State record
-record(state, {ref :: ref()}).

%%%===================================================================
%%% lasp_storage_backend callbacks
%%%===================================================================

%% @doc Start and link to calling process.
-spec start(atom())-> {ok, pid()}.
start(Identifier) ->
    gen_server:start_link({local, ?MODULE},
                          ?MODULE,
                          [Identifier],
                          []).

%% @doc Write a record to the backend.
-spec put(ref(), id(), variable()) -> ok | {error, atom()}.
put(Ref, Id, Record) ->
    gen_server:call(Ref, {put, Id, Record}, infinity).

%% @doc In-place update given a mutation function.
-spec update(ref(), id(), function()) -> {ok, any()} | error |
                                         {error, atom()}.
update(Ref, Id, Function) ->
    gen_server:call(Ref, {update, Id, Function}, infinity).

%% @doc Update all objects given a mutation function.
-spec update_all(ref(), function()) -> {ok, term()}.
update_all(Ref, Function) ->
    gen_server:call(Ref, {update_all, Function}, infinity).

%% @doc Retrieve a record from the backend.
-spec get(ref(), id()) -> {ok, variable()} | {error, not_found} |
                          {error, atom()}.
get(Ref, Id) ->
    gen_server:call(Ref, {get, Id}, infinity).

%% @doc Fold operation.
-spec fold(store(), function(), term()) -> {ok, term()}.
fold(Ref, Function, Acc) ->
    gen_server:call(Ref, {fold, Function, Acc}, infinity).

%% @doc Reset all application state.
-spec reset(store()) -> ok.
reset(Ref) ->
    gen_server:call(Ref, reset, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
init([Identifier]) ->
    try
        Identifier = ets:new(Identifier, [ordered_set,
                                          named_table,
                                          public]),
        {ok, #state{ref=Identifier}}
    catch
        _:Reason ->
            _ = lager:info("Backend initialization failed!"),
            {stop, Reason}
    end.

%% @private
handle_call({get, Id}, _From, #state{ref=Ref}=State) ->
    Result = do_get(Ref, Id),
    {reply, Result, State};
handle_call({put, Id, Record}, _From, #state{ref=Ref}=State) ->
    Result = do_put(Ref, Id, Record),
    {reply, Result, State};
handle_call({update, Id, Function}, _From, #state{ref=Ref}=State) ->
    Result = case do_get(Ref, Id) of
        {ok, Value} ->
            {NewValue, InnerResult} = Function(Value),
            case do_put(Ref, Id, NewValue) of
                ok ->
                    InnerResult
            end;
        Error ->
            Error
    end,
    {reply, Result, State};
handle_call({update_all, Function}, _From, #state{ref=Ref}=State) ->
    Result = ets:foldl(
        fun({Id, _}=Value, Acc) ->
            {NewValue, InnerResult} = Function(Value),
            case do_put(Ref, Id, NewValue) of
                ok ->
                    Acc ++ [InnerResult]
            end
        end,
        [],
        Ref
    ),
    {reply, {ok, Result}, State};
handle_call({fold, Function, Acc0}, _From, #state{ref=Ref}=State) ->
    Acc1 = ets:foldl(Function, Acc0, Ref),
    {reply, {ok, Acc1}, State};
handle_call(reset, _From, #state{ref=Ref}=State) ->
    true = ets:delete_all_objects(Ref),
    {reply, ok, State};
handle_call(Msg, _From, State) ->
    _ = lager:warning("Unhandled messages: ~p", [Msg]),
    {reply, ok, State}.

%% @private
handle_cast(Msg, State) ->
    _ = lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
handle_info(Msg, State) ->
    _ = lager:warning("Unhandled messages: ~p", [Msg]),
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

%% @doc Retrieve a record from the backend.
-spec do_get(ref(), id()) -> {ok, variable()} | {error, not_found} |
                             {error, atom()}.
do_get(Ref, Id) ->
    case ets:lookup(Ref, Id) of
        [{_Key, Record}] ->
            {ok, Record};
        [] ->
            {error, not_found}
    end.

%% @doc Write a record to the backend.
-spec do_put(ref(), id(), variable()) -> ok.
do_put(Ref, Id, Record) ->
    true = ets:insert(Ref, {Id, Record}),
    ok.
