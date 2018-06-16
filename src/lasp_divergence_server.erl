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

-module(lasp_divergence_server).
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
-record(state, {}).

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
    lager:info("Divergence server initialized."),

    %% Delay for graph connectedness.
    wait_for_connectedness(),
    lasp_instrumentation:experiment_started(),

    %% Configure invariant.
    Threshold = {value, max_events() * client_number()},

    EnforceFun = fun() ->
                         lager:info("Threshold exceeded!"),
                         lasp_config:set(events_generated, true)
                 end,

    lager:info("Configuring invariant for threshold: ~p", [Threshold]),
    Pid = spawn_link(fun() ->
                       lasp:invariant(?SIMPLE_COUNTER, Threshold, EnforceFun)
               end),
    lager:info("Configured process: ~p", [Pid]),

    %% Track whether simulation has ended or not.
    lasp_config:set(simulation_end, false),

    %% Schedule check simulation end
    schedule_check_simulation_end(),

    %% Mark that the convergence reached.
    lasp_workflow:task_completed(convergence, lasp_support:mynode()),

    {ok, #state{}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.
handle_call(Msg, _From, State) ->
    lager:warning("Unhandled call messages at module ~p: ~p", [?MODULE, Msg]),
    {reply, ok, State}.

%% @private
-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
handle_cast(Msg, State) ->
    lager:warning("Unhandled cast messages at module ~p: ~p", [?MODULE, Msg]),
    {noreply, State}.

handle_info(check_simulation_end, State) ->
    lasp_marathon_simulations:log_message_queue_size("check_simulation_end"),

    {ok, NodesWithAllEvents} = lasp_workflow:task_progress(events),
    {ok, NodesWithLogsPushed} = lasp_workflow:task_progress(logs),

    lager:info("Checking for simulation end: ~p nodes with all events and ~p nodes with logs pushed.",
               [NodesWithAllEvents, NodesWithLogsPushed]),

    case lasp_workflow:is_task_completed(events) of
        true ->
            case lasp_workflow:is_task_completed(anti_entropy, 1) of
                false ->
                    lager:info("Performing anti-entropy pass with all clients."),
                    ObjectFilterFun = fun(_, _) -> true end,
                    ok = lasp_distribution_backend:blocking_sync(ObjectFilterFun),
                    lasp_workflow:task_completed(anti_entropy, lasp_support:mynode());
                true ->
                    ok
            end;
        false ->
            ok
    end,

    case lasp_workflow:is_task_completed(logs) of
        true ->
            log_convergence(),
            log_divergence(),
            lasp_instrumentation:stop(),
            lasp_support:push_logs(),
            lasp_config:set(simulation_end, true),
            stop_simulation();
        false ->
            schedule_check_simulation_end()
    end,

    {noreply, State};

handle_info(Msg, State) ->
    lager:warning("Unhandled info messages at module ~p: ~p", [?MODULE, Msg]),
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
client_number() ->
    lasp_config:get(client_number, 0).

%% @private
log_divergence() ->
    lager:info("Attempting to get divergence count..."),
    {ok, Value} = lasp:query(?SIMPLE_COUNTER),
    lager:info("Count is: ~p", [Value]),

    MaxEvents = max_events() * client_number(),
    Overcounting = Value - MaxEvents,
    OvercountingPercentage = (Overcounting * 100) / MaxEvents,

    lager:info("**** Divergence information."),
    lager:info("**** Value: ~p", [Value]),
    lager:info("**** MaxEvents: ~p", [MaxEvents]),
    lager:info("**** Overcounting: ~p", [Overcounting]),
    lager:info("**** OvercountingPercentage: ~p", [OvercountingPercentage]),

    case lasp_config:get(instrumentation, false) of
        true ->
            lasp_instrumentation:overcounting(OvercountingPercentage);
        false ->
            ok
    end.

%% @private
stop_simulation() ->
    case sprinter:orchestrated() of
        false ->
            ok;
        _ ->
            case sprinter:orchestration() of
                {ok, kubernetes} ->
                    lasp_kubernetes_simulations:stop();
                {ok, mesos} ->
                    lasp_marathon_simulations:stop()
            end
    end.

%% @private
wait_for_connectedness() ->
    case sprinter:orchestrated() of
        false ->
            ok;
        _ ->
            case sprinter_backend:was_connected() of
                {ok, true} ->
                    ok;
                {ok, false} ->
                    timer:sleep(100),
                    wait_for_connectedness()
            end
    end.
