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
         task_completed/2]).

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

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([]) -> {ok, #state{}}.
init([]) ->
    RedisHost = os:getenv("REDIS_SERVICE_HOST", "127.0.0.1"),
    RedisPort = os:getenv("REDIS_SERVICE_PORT", "6379"),
    Result = eredis:start_link(RedisHost, list_to_integer(RedisPort)),
    Eredis = case Result of
        {ok, C} ->
            C ;
        {error, Error} ->
            lager:error("Error connecting to redis for workflow management: ~p",
                        [Error]),
            undefined
    end,
    {ok, #state{eredis=Eredis}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.

%% @private
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
prefix(Task, Node) ->
    EvalId = lasp_config:get(evaluation_identifier, undefined),
    EvalTimestamp = lasp_config:get(evaluation_timestamp, 0),
    "workflow" ++ "/" ++ EvalId ++ "/" ++ integer_to_list(EvalTimestamp) ++ "/" ++ Task ++ "/" ++ Node.
