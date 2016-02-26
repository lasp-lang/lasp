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
         log/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("lasp.hrl").

%% State record.
-record(state, {type,
                size=0,
                clients=0,
                lines="",
                clock=0,
                status=init,
                filename}).

-define(INTERVAL, 10000).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start and link to calling process.
-spec start_link(list(term()))-> {ok, pid()} | ignore | {error, term()}.
start_link(Type) ->
    gen_server:start_link({global, {?MODULE, Type}}, ?MODULE, [Type], []).

-spec log(term(), term(), node()) -> ok | error().
log(Type, Term, Node) ->
    gen_server:call({global, {?MODULE, Type}}, {log, Term, Node}, infinity).

-spec start(term(), list(), pos_integer()) -> ok | error().
start(Type, Filename, Clients) ->
    gen_server:call({global, {?MODULE, Type}}, {start, Filename, Clients}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([term()]) -> {ok, #state{}}.
init([Type]) ->
    Line = io_lib:format("Seconds,MegaBytes,MeanMegaBytesPerClient\n", []),
    Line2 = io_lib:format("0,0,0\n", []),
    {ok, #state{type=Type, lines=Line ++ Line2}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.

handle_call({log, Term, _Node}, _From, #state{type=_Type, size=Size0}=State) ->
    Size = termsize(Term),
    %% lager:info("Instrumentation: type ~p received ~p bytes from node ~p", [Type, Size, Node]),
    {reply, ok, State#state{size=Size0 + Size}};

handle_call({start, Filename, Clients}, _From, State) ->
    start_timer(),
    {reply, ok, State#state{clock=0, clients=Clients, filename=Filename, status=running}};

%% @private
handle_call(Msg, _From, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {reply, ok, State}.

%% @private
-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
handle_cast(Msg, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info(record, #state{type=Type, filename=Filename, clients=Clients,
                           size=Size, clock=Clock0, status=running, lines=Lines0}=State) ->
    start_timer(),
    Clock = Clock0 + ?INTERVAL,
    Line = io_lib:format("~w,~w,~w\n",
                         [clock(Clock),
                          megasize(Size),
                          megasize(Size) / Clients]),
    Lines = Lines0 ++ Line,
    ok = file:write_file(filename(Filename, Type), Lines),
    lager:info("Instrumentation: type ~p wrote ~p", [Type, Filename]),
    {noreply, State#state{clock=Clock, lines=Lines}};

handle_info(Msg, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
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
filename(Filename, Type) ->
    Root = code:priv_dir(?APP),
    Root ++ "/logs/lasp_transmission_instrumentation-" ++ atom_to_list(Type) ++ "-" ++ Filename ++ ".csv".

%% @private
megasize(Size) ->
    KiloSize = Size / 1024,
    MegaSize = KiloSize / 1024,
    MegaSize.

%% @private
clock(Clock) ->
    Clock / 1000.
