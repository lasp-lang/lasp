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

-module(lasp_divergence_client).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

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
    lager:info("Divergence client initialized."),

    %% Generate actor identifier.
    Actor = node(),

    %% Schedule event.
    schedule_event(),

    %% Create variable.
    {ok, _} = lasp:declare(?SIMPLE_COUNTER, ?GCOUNTER_TYPE),

    %% Configure invariant.
    Threshold = {value, max_events()},

    EnforceFun = fun() ->
                         lager:info("Threshold exceeded!"),
                         lasp_config:set(events_generated, true)
                 end,

    lager:info("Configuring invariant for threshold: ~p", [Threshold]),
    spawn_link(fun() ->
                       lasp:invariant(?SIMPLE_COUNTER, Threshold, EnforceFun)
               end),
    lager:info("Configured."),

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
handle_info(event, #state{actor=Actor, events=Events0}=State) ->
    lasp_marathon_simulations:log_message_queue_size("event"),

    LocalEvents = case lasp_workflow:is_task_completed(convergence, 1) of
        true ->
            Events1 = Events0 + 1,

            Element = atom_to_list(Actor),

            perform_update(Element, Actor, Events1),

            case max_events_reached(Events1) of
                true ->
                    Value = lasp:query(?SIMPLE_COUNTER),
                    lager:info("All events done, counter is now: ~p.
                               Node: ~p", [Value, Actor]),

                    %% Update Simulation Status Instance
                    lasp_workflow:task_completed(events, node()),
                    log_convergence(),
                    schedule_check_simulation_end();
                false ->
                    schedule_event()
            end,
            Events1;
        false ->
            schedule_event(),
            Events0
    end,

    {noreply, State#state{events=LocalEvents}};

handle_info(check_simulation_end, #state{actor=Actor}=State) ->
    lasp_marathon_simulations:log_message_queue_size("check_simulation_end"),

    case lasp_workflow:is_task_completed(events) of
        true ->
            lager:info("All nodes did all events. Node ~p", [Actor]),
            case lasp_workflow:is_task_completed(anti_entropy, 1) of
                true ->
                    lasp_instrumentation:stop(),
                    lasp_support:push_logs(),
                    lasp_workflow:task_completed(logs, node());
                false ->
                    schedule_check_simulation_end()
            end;
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

%% @private
max_events_reached(Events) ->
    max_events() == Events orelse lasp_config:get(events_generated, false).

%% @private
perform_update(Element, Actor, Events1) ->
    UniqueElement = Element ++ integer_to_list(Events1),

    case lasp_config:get(throughput_type, gset) of
        twopset ->
            lasp:update(?SIMPLE_TWOPSET, {add, UniqueElement}, Actor);
        boolean ->
            lasp:update(?SIMPLE_BOOLEAN, true, Actor);
        gset ->
            lasp:update(?SIMPLE_BAG, {add, Element}, Actor);
        awset_ps ->
            lasp:update(?PROVENANCE_SET, {add, UniqueElement}, Actor);
        gcounter ->
            lasp:update(?SIMPLE_COUNTER, increment, Actor)
    end.
