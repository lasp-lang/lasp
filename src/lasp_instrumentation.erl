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
         log/3,
         convergence/0,
         stop/0,
         log_file/0]).

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
                status=init,
                filename}).

-define(INTERVAL, 1000). %% 1 second.

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start and link to calling process.
-spec start_link()-> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec log(term(), term(), pos_integer()) -> ok | error().
log(Type, Payload, PeerCount) ->
    gen_server:call(?MODULE, {log, Type, Payload, PeerCount}, infinity).

-spec convergence() -> ok | error().
convergence() ->
    gen_server:call(?MODULE, convergence, infinity).

-spec stop() -> ok | error().
stop() ->
    gen_server:call(?MODULE, stop, infinity).

-spec log_file() -> {string(), string()}.
log_file() ->
    {_DirPath, FilePath, S3Id} = dir_path_filename_and_s3_id(),
    {FilePath, S3Id}.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([term()]) -> {ok, #state{}}.
init([]) ->
    {DirPath, Filename, _S3Id} = dir_path_filename_and_s3_id(),
    filelib:ensure_dir(DirPath),

    Line = io_lib:format("Type,Seconds,MegaBytes\n", []),
    write_to_file(Filename, Line),

    {ok, TRef} = start_timer(),

    _ = lager:info("Instrumentation timer enabled!"),

    {ok, #state{tref=TRef, filename=Filename, status=running,
                size_per_type=orddict:new()}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.

handle_call({log, Type, Payload, PeerCount}, _From, #state{size_per_type=Map0}=State) ->
    LogType = get_log_type(Type),
    Size = termsize(Payload) * PeerCount,
    Current = case orddict:find(LogType, Map0) of
        {ok, Value} ->
            Value;
        error ->
            0
    end,
    Map = orddict:store(LogType, Current + Size, Map0),
    {reply, ok, State#state{size_per_type=Map}};

handle_call(convergence, _From, #state{filename=Filename}=State) ->
    record_convergence(Filename),
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
handle_info(record, #state{filename=Filename, size_per_type=Map,
                           status=running}=State) ->
    {ok, TRef} = start_timer(),
    record(Map, Filename),
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
start_timer() ->
    timer:send_after(?INTERVAL, record).

%% @private
eval_dir() ->
    code:priv_dir(?APP) ++ "/evaluation".

%% @private
log_dir() ->
    eval_dir() ++ "/logs".

%% @private
dir_path_filename_and_s3_id() ->
    Simulation = lasp_config:get(simulation, undefined),
    LocalOrDCOS = case os:getenv("DCOS", "false") of
        "false" ->
            "local";
        _ ->
            "dcos"
    end,
    EvalIdentifier = lasp_config:get(evaluation_identifier, undefined),
    Id = atom_to_list(Simulation) ++ "/"
      ++ LocalOrDCOS ++ "/"
      ++ atom_to_list(EvalIdentifier),
    EvalTimestamp = lasp_config:get(evaluation_timestamp, 0),
    Filename = atom_to_list(node()) ++ ".csv",

    DirPath = log_dir() ++ "/"
        ++ Id ++ "/"
        ++ integer_to_list(EvalTimestamp) ++ "/",
    FilePath = DirPath ++ Filename,
    S3Id = Id ++ "/"
              ++ integer_to_list(EvalTimestamp) ++ "/"
              ++ Filename,
    {DirPath, FilePath, S3Id}.

%% @private
megasize(Size) ->
    KiloSize = Size / 1024,
    MegaSize = KiloSize / 1024,
    MegaSize.

%% @private
record(Map, Filename) ->
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
record_convergence(Filename) ->
    Timestamp = timestamp(),
    Line = get_line(convergence, Timestamp, 0),
    append_to_file(Filename, Line).

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
get_log_type(aae_send) -> aae_send;
get_log_type(broadcast) -> aae_send;
get_log_type(delta_send) -> delta_send;
get_log_type(delta_ack) -> delta_send.
