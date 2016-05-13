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

-module(lasp_divergence_instrumentation).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).

%% API
-export([start_link/0,
         start/2,
         stop/0,
         buffer/2,
         flush/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("lasp.hrl").

%% State record.
-record(state, {tref,
                events=0,
                lines="",
                clock=0,
                clients=0,
                total=0,
                total_dec=0,
                status=init,
                filename}).

-define(INTERVAL, 10000). %% 10 seconds.

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start and link to calling process.
-spec start_link()-> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({global, ?MODULE}, ?MODULE, [], []).

-spec buffer(integer(), node()) -> ok | error().
buffer(Number, Node) ->
    gen_server:call({global, ?MODULE}, {buffer, Number, Node}, infinity).

-spec flush(integer(), node()) -> ok | error().
flush(Number, Node) ->
    gen_server:call({global, ?MODULE}, {flush, Number, Node}, infinity).

-spec start(list(), pos_integer()) -> ok | error().
start(Filename, Clients) ->
    gen_server:call({global, ?MODULE}, {start, Filename, Clients}, infinity).

-spec stop()-> ok | error().
stop() ->
    gen_server:call({global, ?MODULE}, stop, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([term()]) -> {ok, #state{}}.
init([]) ->
    Line = io_lib:format("Seconds,Divergence,MeanDivergencePerClient\n", []),
    Line2 = io_lib:format("0,0,0\n", []),
    {ok, #state{lines=Line ++ Line2}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.

handle_call({buffer, Events, _Node}, _From, #state{events=Events0, total=Total0}=State) ->
    {reply, ok, State#state{events=Events0 + Events, total=Total0 + Events}};

handle_call({flush, Events, _Node}, _From, #state{events=Events0,
                                                  total_dec=TotalDec0}=State) ->
    {reply, ok, State#state{events=Events0 - Events, total_dec=TotalDec0 + Events}};

handle_call({start, Filename, Clients}, _From, State) ->
    {ok, TRef} = start_timer(),
    {reply, ok, State#state{tref=TRef, clients=Clients, clock=0, total=0, total_dec=0,
                            filename=Filename, status=running, events=0,
                            lines = []}};

handle_call(stop, _From, #state{lines=Lines0, clock=Clock0, events=Events,
                                total_dec=TotalDec, total=Total,
                                clients=Clients, filename=Filename,
                                tref=TRef}=State) ->
    {ok, cancel} = timer:cancel(TRef),
    {ok, Clock, Lines} = record(Clock0, Events, Clients, Filename, Lines0),
    _ = lager:info("Total events seen: ~p", [Total]),
    _ = lager:info("Total decrements seen: ~p", [TotalDec]),
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
                           events=Events, clock=Clock0, status=running,
                           lines=Lines0}=State) ->
    {ok, TRef} = start_timer(),
    {ok, Clock, Lines} = record(Clock0, Events, Clients, Filename, Lines0),
    {noreply, State#state{tref=TRef, clock=Clock, lines=Lines}};

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
start_timer() ->
    timer:send_after(?INTERVAL, record).

%% @private
filename(Filename) ->
    Root = code:priv_dir(?APP),
    Root ++ "/logs/" ++ Filename.

%% @private
clock(Clock) ->
    Clock / 1000.

%% @private
record(Clock0, Events, Clients, Filename, Lines0) ->
    Clock = Clock0 + ?INTERVAL,
    Line = io_lib:format("~w,~w,~w\n",
                         [clock(Clock), Events, Events / Clients]),
    Lines = Lines0 ++ Line,
    ok = file:write_file(filename(Filename), Lines),
    {ok, Clock, Lines}.

