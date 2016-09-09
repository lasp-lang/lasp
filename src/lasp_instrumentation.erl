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

-module(lasp_instrumentation).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).

%% API
-export([start_link/0,
         transmission/3,
         memory/1,
         overcounting/1,
         convergence/0,
         stop/0,
         log_files/0]).

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
                size_per_type=orddict:new(),
                status=init}).

-define(TRANSMISSION_INTERVAL, 1000). %% 1 second.

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start and link to calling process.
-spec start_link()-> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec transmission(term(), term(), pos_integer()) -> ok | error().
transmission(Type, Payload, PeerCount) ->
    gen_server:call(?MODULE, {transmission, Type, Payload, PeerCount}, infinity).

-spec memory(pos_integer()) -> ok | error().
memory(Size) ->
    gen_server:call(?MODULE, {memory, Size}, infinity).

-spec overcounting(pos_integer()) -> ok | error().
overcounting(Value) ->
    gen_server:call(?MODULE, {overcounting, Value}, infinity).

-spec convergence() -> ok | error().
convergence() ->
    gen_server:call(?MODULE, convergence, infinity).

-spec stop() -> ok | error().
stop() ->
    gen_server:call(?MODULE, stop, infinity).

-spec log_files() -> [{string(), string()}].
log_files() ->
    SimulationId = simulation_id(),

    MainLog = main_log(),
    MainLogS3 = SimulationId ++ "/" ++ main_log_suffix(),

    OtherLogs = case partisan_config:get(tag, undefined) of
        server ->
            OvercountingLog = overcounting_log(),
            OvercountingLogS3 = SimulationId ++ "/" ++ overcounting_log_suffix(),
            [{OvercountingLog, OvercountingLogS3}];
        _ ->
            []
    end,

    [{MainLog, MainLogS3} | OtherLogs].

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([term()]) -> {ok, #state{}}.
init([]) ->
    LogDir = log_dir(),
    filelib:ensure_dir(LogDir),

    Filename = main_log(),
    Line = io_lib:format("Type,Seconds,MegaBytes\n", []),
    write_to_file(Filename, Line),

    {ok, TRef} = start_transmission_timer(),

    _ = lager:info("Instrumentation timer enabled!"),

    {ok, #state{tref=TRef, status=running,
                size_per_type=orddict:new()}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.

handle_call({transmission, Type, Payload, PeerCount}, _From, #state{size_per_type=Map0}=State) ->
    TransmissionType = get_transmission_type(Type),
    Size = termsize(Payload) * PeerCount,
    Current = case orddict:find(TransmissionType, Map0) of
        {ok, Value} ->
            Value;
        error ->
            0
    end,
    Map = orddict:store(TransmissionType, Current + Size, Map0),
    {reply, ok, State#state{size_per_type=Map}};

handle_call({memory, Size}, _From, #state{}=State) ->
    record_memory(Size),
    {reply, ok, State};

handle_call({overcounting, Value}, _From, #state{}=State) ->
    record_overcounting(Value),
    {reply, ok, State};

handle_call(convergence, _From, #state{}=State) ->
    record_convergence(),
    {reply, ok, State};

handle_call(stop, _From, #state{tref=TRef}=State) ->
    {ok, cancel} = timer:cancel(TRef),
    _ = lager:info("Instrumentation timer disabled!"),
    {reply, ok, State#state{tref=undefined}};

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
handle_info(transmission, #state{size_per_type=Map, status=running}=State) ->
    {ok, TRef} = start_transmission_timer(),
    record_transmission(Map),
    {noreply, State#state{tref=TRef}};

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
termsize(Term) ->
    erts_debug:flat_size(Term) * erlang:system_info(wordsize).

%% @private
start_transmission_timer() ->
    timer:send_after(?TRANSMISSION_INTERVAL, transmission).

%% @private
root_eval_dir() ->
    code:priv_dir(?APP) ++ "/evaluation".

%% @private
root_log_dir() ->
    root_eval_dir() ++ "/logs".

%% @private
log_dir() ->
    root_log_dir() ++ "/" ++ simulation_id() ++ "/".

%% @private
simulation_id() ->
    Simulation = lasp_config:get(simulation, undefined),
    LocalOrDCOS = case os:getenv("DCOS", "false") of
        "false" ->
            "local";
        _ ->
            "dcos"
    end,
    EvalIdentifier = lasp_config:get(evaluation_identifier, undefined),
    EvalTimestamp = lasp_config:get(evaluation_timestamp, 0),

    Id = atom_to_list(Simulation) ++ "/"
      ++ LocalOrDCOS ++ "/"
      ++ atom_to_list(EvalIdentifier) ++ "/"
      ++ integer_to_list(EvalTimestamp),
    Id.

%% @private
main_log() ->
    log_dir() ++ main_log_suffix().

%% @private
main_log_suffix() ->
    atom_to_list(node()) ++ ".csv".

%% @private
overcounting_log() ->
    log_dir() ++ overcounting_log_suffix().

%% @private
overcounting_log_suffix() ->
    "overcounting".

%% @private
megasize(Size) ->
    KiloSize = Size / 1024,
    MegaSize = KiloSize / 1024,
    MegaSize.

%% @private
record_transmission(Map) ->
    Filename = main_log(),
    Timestamp = timestamp(),
    Lines = orddict:fold(
        fun(Type, Size, Acc) ->
            Acc ++ get_line(Type, Timestamp, Size)
        end,
        "",
        Map
    ),
    append_to_file(Filename, Lines).

%% @private
record_memory(Size) ->
    Filename = main_log(),
    Timestamp = timestamp(),
    Line = get_line(memory, Timestamp, Size),
    append_to_file(Filename, Line).

%% @private
record_convergence() ->
    Filename = main_log(),
    Timestamp = timestamp(),
    Line = get_line(convergence, Timestamp, 0),
    append_to_file(Filename, Line).

%% @private
record_overcounting(Value) ->
    lager:info("Overcounting ~p%", [Value]),
    Filename = overcounting_log(),
    Line = io_lib:format("~w", [Value]),
    write_to_file(Filename, Line).

%% @private
get_line(Type, Timestamp, Size) ->
    io_lib:format(
        "~w,~w,~w\n",
        [Type, Timestamp, megasize(Size)]
    ).

%% @private
write_to_file(Filename, Line) ->
    write_file(Filename, Line, write).

%% @private
append_to_file(Filename, Line) ->
    write_file(Filename, Line, append).

%% @private
write_file(Filename, Line, Mode) ->
    ok = file:write_file(Filename, Line, [Mode]),
    ok.

%% @private
timestamp() ->
    {Mega, Sec, _Micro} = erlang:timestamp(),
    Mega * 1000000 + Sec.

%% @private
get_transmission_type(aae_send) -> aae_send;
get_transmission_type(broadcast) -> aae_send;
get_transmission_type(delta_send) -> delta_send;
get_transmission_type(delta_ack) -> delta_send.
