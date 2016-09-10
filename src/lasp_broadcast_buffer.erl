%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Christopher S. Meiklejohn.  All Rights Reserved.
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

-module(lasp_broadcast_buffer).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).

-define(BROADCAST_INTERVAL, 30000).

%% API
-export([start_link/0,
         buffer/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("lasp.hrl").

%% State record.
-record(state, {buffer}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start and link to calling process.
-spec start_link()-> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec buffer(term()) -> ok | error().
buffer(Payload) ->
    gen_server:cast(?MODULE, {buffer, Payload}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([term()]) -> {ok, #state{}}.
init([]) ->
    %% Schedule periodic broadcasts.
    schedule_broadcast(),

    %% Build a buffer.
    Buffer = dict:new(),

    {ok, #state{buffer=Buffer}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.

%% @private
handle_call(Msg, _From, State) ->
    _ = lager:warning("Unhandled messages: ~p", [Msg]),
    {reply, ok, State}.

%% @private
-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.

handle_cast({buffer, {Id, _Type, _Metadata, _Value} = Payload},
            #state{buffer=Buffer0}=State) ->
    lasp_logger:extended("Buffering update for ~p", [Id]),

    Buffer = case lasp_config:get(broadcast, false) of
        true ->
            %% Buffer latest update for that state.
            dict:store(Id, Payload, Buffer0);
        false ->
            Buffer0
    end,

    {noreply, State#state{buffer=Buffer}};

handle_cast(Msg, State) ->
    _ = lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
handle_info(perform_broadcast, #state{buffer=Buffer0}=State) ->
    lager:info("Flushing broadcast buffer."),

    Backend = lasp_config:get(distribution_backend,
                              ?DEFAULT_DISTRIBUTION_BACKEND),

    %% Call broadcast for latest buffered updates.
    dict:fold(fun(Id, Payload, ok) ->
                      lasp_logger:extended("Flushing update ~p", [Id]),
                      ok = Backend:broadcast(Payload),
                      lasp_logger:extended("Finished."),
                      ok
              end, ok, Buffer0),

    %% Reset state.
    Buffer = dict:new(),

    %% Reschedule broadcast.
    schedule_broadcast(),

    lager:info("Flushing broadcast buffer complete."),

    {noreply, State#state{buffer=Buffer}};

handle_info(Msg, State) ->
    _ = lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec terminate(term(), #state{}) -> term().
terminate(_Reason, _State) ->
    ok.

%% @private
-spec code_change(term() | {down, term()}, #state{}, term()) -> {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
schedule_broadcast() ->
    case lasp_config:get(broadcast, false) of
        true ->
            timer:send_after(?BROADCAST_INTERVAL, perform_broadcast);
        false ->
            ok
    end.
