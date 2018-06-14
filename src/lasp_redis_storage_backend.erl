%% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 Christopher S. Meiklejohn.  All Rights Reserved.
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

-module(lasp_redis_storage_backend).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).
-behaviour(lasp_storage_backend).

-include("lasp.hrl").

%% lasp_storage_backend callbacks
-export([start_link/1,
         put/3,
         update/3,
         update_all/2,
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

%% reference type
-type eredis() :: term().
-type prefix() :: atom().

%% State record
-record(state, {eredis, prefix}).

%%%===================================================================
%%% lasp_storage_backend callbacks
%%%===================================================================

%% @doc Start and link to calling process.
-spec start_link(atom())-> {ok, pid()}.
start_link(Prefix) ->
    gen_server:start_link({local, ?MODULE},
                          ?MODULE,
                          [Prefix],
                          []).

%% @doc Write a record to the backend.
-spec put(eredis(), id(), variable()) -> ok | {error, atom()}.
put(Eredis, Id, Record) ->
    gen_server:call(Eredis, {put, Id, Record}, ?TIMEOUT).

%% @doc In-place update given a mutation function.
-spec update(eredis(), id(), function()) -> {ok, any()} | error |
                                         {error, atom()}.
update(Eredis, Id, Function) ->
    gen_server:call(Eredis, {update, Id, Function}, ?TIMEOUT).

%% @doc Update all objects given a mutation function.
-spec update_all(eredis(), function()) -> {ok, term()}.
update_all(Eredis, Function) ->
    gen_server:call(Eredis, {update_all, Function}, ?TIMEOUT).

%% @doc Retrieve a record from the backend.
-spec get(eredis(), id()) -> {ok, variable()} | {error, not_found} |
                          {error, atom()}.
get(Eredis, Id) ->
    gen_server:call(Eredis, {get, Id}, ?TIMEOUT).

%% @doc Fold operation.
-spec fold(store(), function(), term()) -> {ok, term()}.
fold(Eredis, Function, Acc) ->
    gen_server:call(Eredis, {fold, Function, Acc}, ?TIMEOUT).

%% @doc Reset all application state.
-spec reset(store()) -> ok.
reset(Eredis) ->
    gen_server:call(Eredis, reset, ?TIMEOUT).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
init([Prefix]) ->
    RedisHost = os:getenv("REDIS_SERVICE_HOST", "127.0.0.1"),
    RedisPort = os:getenv("REDIS_SERVICE_PORT", "6379"),
    Result = eredis:start_link(RedisHost, list_to_integer(RedisPort)),
    case Result of
        {ok, C} ->
            {ok, #state{eredis=C, prefix=Prefix}};
        Error ->
            lager:error("Error connecting to redis for workflow management: ~p",
                        [Error]),
            {stop, Error}
    end.

%% @private
handle_call({get, Id}, _From, #state{eredis=Eredis, prefix=Prefix}=State) ->
    Result = do_get(Eredis, Prefix, Id),
    {reply, Result, State};
handle_call({put, Id, Record}, _From, #state{eredis=Eredis, prefix=Prefix}=State) ->
    Result = do_put(Eredis, Id, Prefix, Record),
    {reply, Result, State};
handle_call({update, Id, Function}, _From, #state{eredis=Eredis, prefix=Prefix}=State) ->
    Result = case do_get(Eredis, Prefix, Id) of
        {ok, Value} ->
            {NewValue, InnerResult} = Function({Id, Value}),
            case do_put(Eredis, Prefix, Id, NewValue) of
                ok ->
                    InnerResult
            end;
        Error ->
            Error
    end,
    {reply, Result, State};
handle_call({update_all, Function}, _From, #state{eredis=Eredis, prefix=Prefix}=State) ->
    {ok, Objects} = eredis:q(Eredis, ["KEYS", prefix(Prefix, "*")]),
    Result = lists:foldl(
        fun(Id, Acc) ->
            {ok, Value} = do_get(Eredis, Prefix, Id),
            {NewValue, InnerResult} = Function({Id, Value}),
            case do_put(Eredis, Prefix, Id, NewValue) of
                ok ->
                    Acc ++ [InnerResult]
            end
        end,
        [],
        Objects
    ),
    {reply, {ok, Result}, State};
handle_call({fold, Function, Acc0}, _From, #state{eredis=Eredis, prefix=Prefix}=State) ->
    {ok, Objects} = eredis:q(Eredis, ["KEYS", prefix(Prefix, "*")]),
    Acc1 = lists:foldl(
        fun(Id, Acc) ->
            {ok, Value} = do_get(Eredis, Prefix, Id),
            Function({Id, Value}, Acc)
        end,
        Acc0,
        Objects
    ),
    {reply, {ok, Acc1}, State};
handle_call(reset, _From, #state{eredis=Eredis}=State) ->
    eredis:q(Eredis, ["FLUSHALL"]),
    {reply, ok, State};
handle_call(Msg, _From, State) ->
    lager:warning("Unhandled call messages at module ~p: ~p", [?MODULE, Msg]),
    {reply, ok, State}.

%% @private
handle_cast(Msg, State) ->
    lager:warning("Unhandled cast messages at module ~p: ~p", [?MODULE, Msg]),
    {noreply, State}.

%% @private
handle_info(Msg, State) ->
    lager:warning("Unhandled info messages at module ~p: ~p", [?MODULE, Msg]),
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
-spec do_get(eredis(), prefix(), id()) -> {ok, variable()} | {error, not_found} |
                             {error, atom()}.
do_get(Eredis, Prefix, Id) ->
    {ok, Result} = eredis:q(Eredis, ["GET", prefix(Prefix, Id)]),
    {ok, decode(Result)}.

%% @doc Write a record to the backend.
-spec do_put(eredis(), prefix(), id(), variable()) -> ok.
do_put(Eredis, Prefix, Id, Record) ->
    {ok, <<"OK">>} = eredis:q(Eredis, ["SET", prefix(Prefix, Id), encode(Record)]),
    ok.

%% @private
encode(Record) ->
    binary_to_term(Record).

%% @private
decode(Record) ->
    term_to_binary(Record).

%% @private
prefix(Prefix, Id) ->
    prefix(Prefix) ++ "/" ++ Id.

%% @private
prefix(Prefix) ->
    "data/" ++ atom_to_list(Prefix).