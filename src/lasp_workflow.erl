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

-module(lasp_workflow).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

%% API
-export([start_link/0,
         start_link/1,
         task_completed/2,
         is_task_completed/1,
         is_task_completed/2,
         task_progress/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% State record.
-record(state, {eredis}).

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

%% @doc Mark a task as completed.
-spec task_completed(atom(), atom()) -> ok.
task_completed(Task, Node) ->
    gen_server:call(?MODULE, {task_completed, Task, Node}, infinity).

%% @doc Determine if a task is completed.
-spec is_task_completed(atom()) -> ok.
is_task_completed(Task) ->
    ClientNumber = lasp_config:get(client_number, 0),
    gen_server:call(?MODULE, {is_task_completed, Task, ClientNumber}, infinity).

%% @doc Determine if a task is completed.
-spec is_task_completed(atom(), non_neg_integer()) -> ok.
is_task_completed(Task, NodeCount) ->
    gen_server:call(?MODULE, {is_task_completed, Task, NodeCount}, infinity).

%% @doc Determine if a task is completed.
-spec task_progress(atom()) -> ok.
task_progress(Task) ->
    gen_server:call(?MODULE, {task_progress, Task}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([]) -> {ok, #state{}}.
init([]) ->
    RedisHost = os:getenv("REDIS_SERVICE_HOST", "127.0.0.1"),
    RedisPort = os:getenv("REDIS_SERVICE_PORT", "6379"),
    Result = eredis:start_link(RedisHost, list_to_integer(RedisPort)),
    case Result of
        {ok, C} ->
            {ok, #state{eredis=C}};
        Error ->
            lager:error("Error connecting to redis for workflow management: ~p",
                        [Error]),
            {stop, Error}
    end.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.

%% @private
handle_call({task_progress, Task}, _From, #state{eredis=Eredis}=State) ->
    {ok, Objects} = eredis:q(Eredis, ["KEYS", prefix(Task, "*")]),
    NumObjects = length(Objects),
    lager:info("Task ~p progress: ~p", [Task, NumObjects]),
    {reply, {ok, NumObjects}, State};
handle_call({is_task_completed, Task, NumNodes}, _From, #state{eredis=Eredis}=State) ->
    {ok, Objects} = eredis:q(Eredis, ["KEYS", prefix(Task, "*")]),
    Result = case length(Objects) of
        NumNodes ->
            lager:info("Task ~p completed on all nodes.", [Task]),
            true;
        Other ->
            lager:info("Task ~p incomplete: only on ~p/~p nodes.",
                       [Task, Other, NumNodes]),
            false
    end,
    {reply, Result, State};
handle_call({task_completed, Task, Node}, _From, #state{eredis=Eredis}=State) ->
    Path = prefix(Task, Node),
    lager:info("Setting ~p to true.", [Path]),
    {ok, <<"OK">>} = eredis:q(Eredis, ["SET", Path, true]),
    {reply, ok, State};

handle_call(Msg, _From, State) ->
    _ = lager:warning("Unhandled messages: ~p", [Msg]),
    {reply, ok, State}.

-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.

%% @private
handle_cast(Msg, State) ->
    _ = lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
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
prefix(Task, Node) when is_atom(Task) ->
    prefix(atom_to_list(Task), Node);
prefix(Task, Node) when is_atom(Node) ->
    prefix(Task, atom_to_list(Node));
prefix(Task, Node) ->
    EvalId = lasp_config:get(evaluation_identifier, undefined),
    EvalTimestamp = lasp_config:get(evaluation_timestamp, 0),
    "workflow" ++ "/" ++ atom_to_list(EvalId) ++ "/" ++ integer_to_list(EvalTimestamp) ++ "/" ++ Task ++ "/" ++ Node.
