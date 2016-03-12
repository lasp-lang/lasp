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

-module(lasp_read_latency_instrumentation).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).

%% API
-export([start_link/0,
         sample/2,
         start/2,
         stop/0]).

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
                clock,
                timestamp,
                lines="",
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

-spec sample(float(), node()) -> ok | error().
sample(Number, Node) ->
    gen_server:call({global, ?MODULE}, {sample, Number, Node}, infinity).

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
    Line = io_lib:format("Seconds,Milliseconds\n", []),
    Line2 = io_lib:format("0,0\n", []),
    {ok, #state{lines=Line ++ Line2}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.

handle_call({sample, _Milliseconds, _Node}, _From, #state{status=init}=State) ->
    %% Don't log anything if we haven't started the instrumentation.
    {reply, ok, State};
handle_call({sample, Milliseconds, _Node},
            _From,
            #state{lines=Lines0, status=running, timestamp=Timestamp0}=State) ->
    Now = os:timestamp(),
    Diff = (timer:now_diff(Now, Timestamp0) / 1000) / 1000,
    Line = io_lib:format("~w,~w\n", [Diff, Milliseconds]),
    Lines = Lines0 ++ Line,
    {reply, ok, State#state{lines=Lines}};

handle_call({start, Filename, _Clients}, _From, State) ->
    {ok, TRef} = start_timer(),
    Timestamp = os:timestamp(),
    {reply, ok, State#state{tref=TRef, clock=0, timestamp=Timestamp,
                            filename=Filename, status=running}};

handle_call(stop, _From, #state{tref=TRef, filename=Filename,
                                lines=Lines0}=State) ->
    {ok, cancel} = timer:cancel(TRef),
    ok = file:write_file(filename(Filename), Lines0),
    {reply, ok, State};

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
handle_info(record, #state{filename=Filename, status=running,
                           clock=Clock0, lines=Lines0}=State) ->
    {ok, TRef} = start_timer(),
    ok = file:write_file(filename(Filename), Lines0),
    {noreply, State#state{tref=TRef, clock=Clock0 + ?INTERVAL}};

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
start_timer() ->
    timer:send_after(?INTERVAL, record).

%% @private
filename(Filename) ->
    Root = code:priv_dir(?APP),
    Root ++ "/logs/" ++ Filename.
