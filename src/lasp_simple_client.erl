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

-module(lasp_simple_client).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com").

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("lasp.hrl").

%% State record.
-record(state, {actor, events}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start and link to calling process.
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link(?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([term()]) -> {ok, #state{}}.
init([]) ->
    lager:info("Simple client initialized."),

    %% Generate actor identifier.
    Actor = node(),

    %% Schedule event.
    schedule_event(),

    {ok, #state{actor=Actor, events=0}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.
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
handle_info(event, #state{actor=Actor,
                          events=Events0}=State) ->
    lasp_marathon_simulations:log_message_queue_size("event"),

    Events1 = Events0 + 1,
    Element = atom_to_list(Actor) ++ "###" + integer_to_list(Events1),

    lasp:update(?SIMPLE_BAG, {add, Element}, Actor),

    {ok, Value} = lasp:query(?SIMPLE_BAG),
    lager:info("Events done: ~p, Events seen: ~p. Node: ~p", [Events1, sets:size(Value), Actor]),

    case Events1 == max_events() of
        true ->
            lager:info("All events done. Node: ~p", [Actor]),

            %% Update Simulation Status Instance
            lasp:update(?SIM_STATUS_ID, {apply, Actor, {fst, true}}, Actor),
            log_convergence(),
            schedule_check_simulation_end();
        false ->
            schedule_event()
    end,

    {noreply, State#state{events=Events1}};

handle_info(check_simulation_end, #state{actor=Actor}=State) ->
    lasp_marathon_simulations:log_message_queue_size("check_simulation_end"),

    %% A simulation ends for clients when all clients have
    %% done all events (first component of the map in
    %% the simulation status instance is true for all clients)
    {ok, AllEventsAndLogs} = lasp:query(?SIM_STATUS_ID),

    NodesWithAllEvents = lists:filter(
        fun({_Node, {AllEvents, _LogsPushed}}) ->
            AllEvents
        end,
        AllEventsAndLogs
    ),

    case length(NodesWithAllEvents) == client_number() of
        true ->
            lager:info("All nodes did all events. Node ~p", [Actor]),
            lasp_instrumentation:stop(),
            lasp_support:push_logs(),
            lasp:update(?SIM_STATUS_ID, {apply, Actor, {snd, true}}, Actor);
        false ->
            schedule_check_simulation_end()
    end,

    {noreply, State};

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
schedule_event() ->
    timer:send_after(?EVENT_INTERVAL, event).

%% @private
schedule_check_simulation_end() ->
    timer:send_after(?STATUS_INTERVAL, check_simulation_end).

%% @private
client_number() ->
    lasp_config:get(client_number, 3).

%% @private
log_convergence() ->
    case lasp_config:get(instrumentation, false) of
        true ->
            lasp_instrumentation:convergence();
        false ->
            ok
    end.

%% @private
max_events() ->
    lasp_config:get(max_events, ?MAX_EVENTS_DEFAULT).
