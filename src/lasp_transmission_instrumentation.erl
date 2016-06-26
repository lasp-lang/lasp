%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Christopher Meiklejohn.  All Rights Reserved.
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

-module(lasp_transmission_instrumentation).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).

%% API
-export([start_link/1,
         start/3,
         stop/1,
         log/5]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("lasp.hrl").

%% State record.
-record(state, {entity,
                tref,
                size_per_type=orddict:new(),
                clients=0,
                lines="",
                clock=0,
                status=init,
                filename}).

-define(INTERVAL, 1000). %% 1 second.

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start and link to calling process.
-spec start_link(list(term()))-> {ok, pid()} | ignore | {error, term()}.
start_link(Entity) ->
    gen_server:start_link({global, {?MODULE, Entity}}, ?MODULE, [Entity], []).

-spec log(term(), term(), term(), pos_integer(), node()) -> ok | error().
log(Entity, Type, Payload, PeerCount, Node) ->
    gen_server:call({global, {?MODULE, Entity}}, {log, Type, Payload, PeerCount, Node}, infinity).

-spec start(term(), list(), pos_integer()) -> ok | error().
start(Entity, Filename, Clients) ->
    gen_server:call({global, {?MODULE, Entity}}, {start, Filename, Clients}, infinity).

-spec stop(term()) -> ok | error().
stop(Entity) ->
    gen_server:call({global, {?MODULE, Entity}}, stop, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([term()]) -> {ok, #state{}}.
init([Entity]) ->
    Line = io_lib:format("Type,Seconds,MegaBytes,MeanMegaBytesPerClient\n", []),
    {ok, #state{entity=Entity, lines=Line}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.

handle_call({log, Type, Payload, PeerCount, _Node}, _From, #state{entity=_Entity, size_per_type=Map0}=State) ->
    Size = termsize(Payload) * PeerCount,
    Current = case orddict:find(Type, Map0) of
        {ok, Value} ->
            Value;
        error ->
            0
    end,
    Map = orddict:store(Type, Current + Size, Map0),
    {reply, ok, State#state{size_per_type=Map}};

handle_call({start, Filename, Clients}, _From, #state{entity=Entity}=State) ->
    {ok, TRef} = start_timer(),
    _ = lager:info("Instrumentation timer for ~p enabled!", [Entity]),
    {reply, ok, State#state{tref=TRef, clock=0, clients=Clients,
                            filename=Filename, status=running, size_per_type=orddict:new(),
                            lines = []}};

handle_call(stop, _From, #state{entity=Entity, lines=Lines0, clock=Clock0,
                                clients=Clients, size_per_type=Map,
                                filename=Filename, tref=TRef}=State) ->
    {ok, cancel} = timer:cancel(TRef),
    {ok, Clock, Lines} = record(Clock0, Map, Clients, Filename, Lines0),
    _ = lager:info("Instrumentation timer for ~p disabled!", [Entity]),
    {reply, ok, State#state{tref=undefined, clock=Clock, lines=Lines}};

%% @private
handle_call(Msg, _From, State) ->
    _ = lager:warning("Unhandled messages: ~p", [Msg]),
    {reply, ok, State}.

%% @private
-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
handle_cast(Msg, State) ->
    _ = lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info(record, #state{filename=Filename, clients=Clients,
                           size_per_type=Map, clock=Clock0, status=running,
                           lines=Lines0}=State) ->
    lager:info("RECORD"),

    {ok, TRef} = start_timer(),
    {ok, Clock, Lines} = record(Clock0, Map, Clients, Filename, Lines0),
    {noreply, State#state{tref=TRef, clock=Clock, lines=Lines}};

handle_info(Msg, State) ->
    _ = lager:warning("INFO Unhandled messages: ~p~n~p", [Msg, State]),
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
termsize(Term) ->
    erts_debug:flat_size(Term) * erlang:system_info(wordsize).

%% @private
start_timer() ->
    timer:send_after(?INTERVAL, record).

%% @private
filename(Filename) ->
    Root = code:priv_dir(?APP),
    Root ++ "/logs/" ++ Filename.

%% @private
megasize(Size) ->
    KiloSize = Size / 1024,
    MegaSize = KiloSize / 1024,
    MegaSize.

%% @private
clock(Clock) ->
    Clock / 1000.

%% @private
record(Clock0, Map, Clients, Filename, Lines0) ->
    lager:info("RECORD"),
    Clock = Clock0 + ?INTERVAL,
    Lines = orddict:fold(
        fun(Type, Size, Acc) ->
            Line = io_lib:format("~w,~w,~w,~w\n",
                                 [Type,
                                  clock(Clock),
                                  megasize(Size),
                                  megasize(Size) / Clients]),
            lager:info("LINE: ~p~n~n~n", [Line]),
            Acc ++ Line
        end,
        Lines0,
        Map
    ),
    ok = file:write_file(filename(Filename), Lines),
    {ok, Clock, Lines}.

