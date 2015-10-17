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

-module(lasp_hanoidb_storage_backend).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).
-behaviour(lasp_storage_backend).

-include("lasp.hrl").

%% lasp_storage_backend callbacks
-export([start/1,
         put/3,
         update/3,
         get/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% reference type
-type ref() :: any().

%% State record
-record(state, {ref :: ref()}).

%%%===================================================================
%%% lasp_storage_backend callbacks
%%%===================================================================

%% @doc Start and link to calling process.
-spec start(atom())-> {ok, pid()} | ignore | {error, term()}.
start(Identifier) ->
    gen_server:start_link({local, Identifier}, ?MODULE, [Identifier], []).

%% @doc Write a record to the backend.
-spec put(ref(), id(), variable()) -> ok | {error, atom()}.
put(Ref, Id, Record) ->
    gen_server:call(Ref, {put, Id, Record}, infinity).

%% @doc In-place update given a mutation function.
-spec update(ref(), id(), function()) -> {ok, any()} | error | {error, atom()}.
update(Ref, Id, Function) ->
    gen_server:call(Ref, {update, Id, Function}, infinity).

%% @doc Retrieve a record from the backend.
-spec get(ref(), id()) -> {ok, variable()} | {error, not_found} | {error, atom()}.
get(Ref, Id) ->
    gen_server:call(Ref, {get, Id}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
init([Identifier]) ->
    %% Get the data root directory
    Config = app_helper:get_env(?APP),
    DataDir = filename:join(app_helper:get_prop_or_env(data_root, Config, hanoidb),
                            atom_to_list(Identifier)),

    %% Ensure directory.
    ok = filelib:ensure_dir(filename:join(DataDir, "hanoidb")),

    case hanoidb:open(DataDir, []) of
        {ok, Ref} ->
            {ok, #state{ref=Ref}};
        {error, Reason} ->
            lager:info("Failed to open backend: ~p", [Reason]),
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
                    InnerResult;
                Error ->
                    Error
            end;
        Error ->
            Error
    end,
    {reply, Result, State};
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

%% @doc Encoding of object to binary before write.
encode(X) ->
    term_to_binary(X).

%% @doc Decoding of object to binary after read.
decode(X) ->
    binary_to_term(X).

do_get(Ref, Id) ->
    StorageKey = encode(Id),
    case hanoidb:get(Ref, StorageKey) of
        {ok, Value} ->
            {ok, decode(Value)};
        not_found ->
            {error, not_found};
        {error, Reason} ->
            lager:info("Error reading object; id: ~p, reason: ~p",
                       [Id, Reason]),
            {error, Reason}
    end.

do_put(Ref, Id, Record) ->
    StorageKey = encode(Id),
    StorageValue = encode(Record),
    Updates = [{put, StorageKey, StorageValue}],
    case hanoidb:transact(Ref, Updates) of
        ok ->
            ok;
        {error, Reason} ->
            lager:info("Error writing object; id: ~p, reason: ~p",
                       [Id, Reason]),
            {error, Reason}
    end.
