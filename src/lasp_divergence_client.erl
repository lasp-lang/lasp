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
-record(state, {actor, events, batch_start, batch_events}).

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
    Threshold = {value, max_events() * client_number()},

    EnforceFun = fun() ->
                         lager:info("Threshold exceeded!"),
                         lasp_config:set(events_generated, true)
                 end,

    lager:info("Configuring invariant for threshold: ~p", [Threshold]),
    spawn_link(fun() ->
                       lasp:invariant(?SIMPLE_COUNTER, Threshold, EnforceFun)
               end),
    lager:info("Configured."),

    {ok, #state{actor=Actor,
                events=0,
                batch_start=undefined,
                batch_events=0}}.

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
                          batch_start=BatchStart0,
                          batch_events=BatchEvents0,
                          events=Events0}=State) ->
    lasp_marathon_simulations:log_message_queue_size("event"),

    %% Start the batch for every event, if it isn't started yet.
    BatchStart1 = case BatchStart0 of
                    undefined ->
                        erlang:timestamp();
                     _ ->
                        BatchStart0
                 end,

    {LocalEvents, BatchStart, BatchEvents} = case lasp_workflow:is_task_completed(convergence, 1) of
        true ->
            Events1 = Events0 + 1,

            %% If we hit the batch size, restart the batch.
            {BatchStart2, BatchEvents1} = case BatchEvents0 + 1 == ?BATCH_EVENTS of
                                              true ->
                                                  BatchEnd = erlang:timestamp(),

                                                  lager:info("Events done: ~p, Batch finished!  ~p, Node: ~p",
                                                             [Events1, ?BATCH_EVENTS, Actor]),

                                                  log_batch(BatchStart1, BatchEnd, ?BATCH_EVENTS),
                                                  {undefined, 0};
                                              false ->
                                                  {BatchStart1, BatchEvents0 + 1}
                                          end,

            Element = atom_to_list(Actor),

            {Duration, _} = timer:tc(fun() ->
                                            perform_update(Element, Actor, Events1)
                                     end),
            log_event(Duration),

            case max_events_reached() of
                true ->
                    Value = lasp:query(?SIMPLE_COUNTER),
                    lager:info("All events done, counter is now: ~p.
                               Node: ~p", [Value, Actor]),

                    case BatchStart2 of
                        undefined ->
                            lager:info("Simulation finished at batch."),
                            ok;
                        _ ->
                            FinalBatchEnd = erlang:timestamp(),
                            lager:info("Batch in progress: ~p", [BatchEvents1]),
                            log_batch(BatchStart2, FinalBatchEnd, BatchEvents1),
                            lager:info("Logging final batch: ~p", [BatchEvents1]),
                            ok
                    end,

                    %% Update Simulation Status Instance
                    lasp_workflow:task_completed(events, node()),
                    log_convergence(),
                    log_event_number(Events1),
                    schedule_check_simulation_end();
                false ->
                    schedule_event()
            end,
            {Events1, BatchStart2, BatchEvents1};
        false ->
            schedule_event(),
            {Events0, undefined, 0}
    end,

    {noreply, State#state{batch_events=BatchEvents,
                          batch_start=BatchStart,
                          events=LocalEvents}};

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
log_batch(Start, End, Events) ->
    case lasp_config:get(instrumentation, false) of
        true ->
            lasp_instrumentation:batch(Start, End, Events);
        false ->
            ok
    end.

%% @private
log_event_number(Events) ->
    case lasp_config:get(instrumentation, false) of
        true ->
            lasp_instrumentation:event_number(Events);
        false ->
            ok
    end.

%% @private
log_event(Duration) ->
    case lasp_config:get(event_logging, false) of
        true ->
            lasp_instrumentation:event(Duration);
        false ->
            ok
    end.

%% @private
max_events() ->
    lasp_config:get(max_events, ?MAX_EVENTS_DEFAULT).

%% @private
client_number() ->
    lasp_config:get(client_number, 0).

%% @private
max_events_reached() ->
    lasp_config:get(events_generated, false).

%% @private
perform_update(Element, Actor, Events1) ->
    UniqueElement = Element ++ integer_to_list(Events1),

    case lasp_config:get(divergence_type, gset) of
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
