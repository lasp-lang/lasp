%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Christopher Meiklejohn.  All Rights Reserved.
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

-module(lasp_dets_storage_backend).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).
-behaviour(lasp_storage_backend).

-include("lasp.hrl").

%% lasp_storage_backend callbacks
-export([start/1,
         put/3,
         update/3,
         get/2,
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
-spec start(atom())-> {ok, atom()}.
start(Identifier) ->
    {ok, _Pid} = gen_server:start_link({local, Identifier},
                                       ?MODULE,
                                       [Identifier],
                                       []),
    {ok, Identifier}.

%% @doc Write a record to the backend.
-spec put(ref(), id(), variable()) -> ok | {error, atom()}.
put(Ref, Id, Record) ->
    gen_server:call(Ref, {put, Id, Record}, infinity).

%% @doc In-place update given a mutation function.
-spec update(ref(), id(), function()) -> {ok, any()} | error |
                                         {error, atom()}.
update(Ref, Id, Function) ->
    gen_server:call(Ref, {update, Id, Function}, infinity).

%% @doc Retrieve a record from the backend.
-spec get(ref(), id()) -> {ok, variable()} | {error, not_found} |
                          {error, atom()}.
get(Ref, Id) ->
    gen_server:call(Ref, {get, Id}, infinity).

%% @doc Fold operation.
-spec fold(store(), function(), term()) -> {ok, term()}.
fold(Ref, Function, Acc) ->
    gen_server:call(Ref, {fold, Function, Acc}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
init([Identifier]) ->
    try
        Config = app_helper:get_env(?APP),
        File = filename:join(app_helper:get_prop_or_env(data_root,
                                                        Config,
                                                        dets),
                                atom_to_list(Identifier)),
        case dets:open_file(Identifier, [{file, File}]) of
            {ok, Identifier} ->
                {ok, #state{ref=Identifier}};
            {error, Error} ->
                {stop, Error}
        end
    catch
        _:Reason ->
            lager:info("Backend initialization failed!"),
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
handle_call({fold, Function, Acc0}, _From, #state{ref=Ref}=State) ->
    Acc1 = dets:foldl(Function, Acc0, Ref),
    {reply, {ok, Acc1}, State};
handle_call(Msg, _From, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {reply, ok, State}.

%% @private
handle_cast(Msg, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
handle_info(Msg, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
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
    case dets:lookup(Ref, Id) of
        [{_Key, Record}] ->
            {ok, Record};
        [] ->
            {error, not_found}
    end.

%% @doc Write a record to the backend.
-spec do_put(ref(), id(), variable()) -> ok.
do_put(Ref, Id, Record) ->
    ok = dets:insert(Ref, {Id, Record}),
    ok.
