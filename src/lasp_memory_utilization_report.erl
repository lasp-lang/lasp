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

-module(lasp_memory_utilization_report).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).

%% API
-export([start_link/0,
         start_link/1]).

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

%% @doc Same as start_link([]).
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    start_link([]).

%% @doc Start and link to calling process.
-spec start_link(list())-> {ok, pid()} | ignore | {error, term()}.
start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([]) -> {ok, #state{}}.
init([]) ->
    %% Seed the process at initialization.
    rand_compat:seed(erlang:phash2([lasp_support:mynode()]),
                     erlang:monotonic_time(),
                     erlang:unique_integer()),

    %% Schedule reports.
    %% schedule_memory_utilization_report(),

    {ok, #state{}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.

%% @private
handle_call(Msg, _From, State) ->
    _ = lager:warning("Unhandled messages: ~p", [Msg]),
    {reply, ok, State}.

-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.

%% @private
handle_cast(Msg, State) ->
    _ = lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info(memory_utilization_report, State) ->
    lasp_marathon_simulations:log_message_queue_size("memory_utilization_report"),

    %% Log
    memory_utilization_report(),

    %% Schedule report.
    schedule_memory_utilization_report(),

    {noreply, State};

handle_info(Msg, State) ->
    _ = lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec terminate(term(), #state{}) -> term().
terminate(_Reason, _State) ->
    ok.

%% @private
-spec code_change(term() | {down, term()}, #state{}, term()) ->
    {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
schedule_memory_utilization_report() ->
    case lasp_config:get(instrumentation, false) of
        true ->
            timer:send_after(?MEMORY_UTILIZATION_INTERVAL, memory_utilization_report);
        false ->
            ok
    end.

%% @private
memory_utilization_report() ->
    TotalBytes = erlang:memory(total),
    TotalKBytes = TotalBytes / 1024,
    TotalMBytes = TotalKBytes / 1024,
    lasp_logger:extended("\nTOTAL MEMORY ~p bytes, ~p megabytes\n", [TotalBytes, round(TotalMBytes)]),

    ProcessesInfo = [process_info(PID, [current_function, initial_call, memory, registered_name, message_queue_len]) || PID <- processes()],
    Top = 5,
    MemorySorted = lists:sublist(
        lists:reverse(
            ordsets:from_list(
                [{to_mb(M), {get_name(I), CF, IC}} || [{current_function, CF}, {initial_call, IC}, {memory, M}, {registered_name, I}, {message_queue_len, _MQ}] <- ProcessesInfo]
            )
        ),
        Top
    ),

    MQSorted = lists:sublist(
        lists:reverse(
            ordsets:from_list(
                [{MQ, {get_name(I), CF, IC}} || [{current_function, CF}, {initial_call, IC}, {memory, _M}, {registered_name, I}, {message_queue_len, MQ}] <- ProcessesInfo]
            )
        ),
        Top
    ),

    MemoryLog = lists:foldl(
        fun({M, {I, {CFM, CFF, CFA}, {ICM, ICF, ICA}}}, Acc) ->
            Acc ++ atom_to_list(I) ++ ":" ++ integer_to_list(M) ++ ":"
            ++ atom_to_list(CFM) ++ "," ++ atom_to_list(CFF) ++ "," ++
            integer_to_list(CFA) ++ ":" ++
            atom_to_list(ICM) ++ "," ++ atom_to_list(ICF) ++ "," ++
            integer_to_list(ICA) ++ "\n"
        end,
        "",
        MemorySorted
    ),
    lasp_logger:extended("\nMEMORY PROCESSES INFO\n" ++ MemoryLog),

    MQLog = lists:foldl(
        fun({MQ, {I, {CFM, CFF, CFA}, {ICM, ICF, ICA}}}, Acc) ->
            Acc ++ atom_to_list(I) ++ ":" ++ integer_to_list(MQ) ++ ":"
            ++ atom_to_list(CFM) ++ "," ++ atom_to_list(CFF) ++ "," ++
            integer_to_list(CFA) ++ ":" ++
            atom_to_list(ICM) ++ "," ++ atom_to_list(ICF) ++ "," ++
            integer_to_list(ICA) ++ "\n"
        end,
        "",
        MQSorted
    ),
    lasp_logger:extended("\nMQ PROCESSES INFO\n" ++ MQLog),

    ok.

%% @private
get_name([]) -> undefined;
get_name(Name) -> Name.

%% @private
to_mb(Bytes) ->
    KBytes = Bytes / 1024,
    MBytes = KBytes / 1024,
    round(MBytes).
