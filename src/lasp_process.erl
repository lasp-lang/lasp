%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
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

-module(lasp_process).
-author('Christopher Meiklejohn <christopher.meiklejohn@gmail.com>').

-behaviour(gen_flow).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_link/1,
         start_dag_link/1,
         start_manual_process/1,
         single_fire_function/4,
         start_single_fire_process/1]).

%% Callbacks
-export([init/1, read/1, process/2]).

%% Types
-type read_fun() :: {atom(), function()}.
-type write_fun() :: {atom(), function()}.

%% Records
-record(state, {read_funs :: [read_fun()],
                trans_fun :: function(),
                write_fun :: write_fun()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link([ReadFuns, TransFun]) ->
    start_manual_process([ReadFuns, TransFun, undefined]).

%% @doc A lasp process that shouldn't be automatically managed by the dag
%%
%%      When using this function, the resulting process isn't tracked by
%%      the runtime. Only useful when we want to manually introduce the
%%      dependencies in the graph, or for functions that aren't currently
%%      supported (fold and stream).
%%
start_manual_process([_ReadFuns, _TransFun, _WriteFun]=Args) ->
    lasp_process_sup:start_child([nodag | Args]).

%% @todo rename to start_link once all functions are tracked
start_dag_link(Args) ->
    start_tracked_process(undefined, Args).

%% @doc Starts a single-fire lasp process.
%%
%%      These processes only run until their transform functions complete
%%      succesfully one time. Used to model reads and binds on values.
%%
start_single_fire_process(Args) ->
    start_tracked_process(1, Args).

%% @doc Starts a lasp process, tracked by the dependency graph module.
%%
%%      EventCount specifies the maximum number of iterations
%%      that it will perform.
%%
start_tracked_process(EventCount, [ReadFuns, TransFun, {To, _}=WriteFun]) ->
    From = [Id || {Id, _} <- ReadFuns],
    case lasp_config:get(dag_enabled, ?DAG_ENABLED) of
        false -> lasp_process_sup:start_child(EventCount, [ReadFuns, TransFun, WriteFun]);
        true -> case lasp_dependence_dag:will_form_cycle(From, To) of
            false -> lasp_process_sup:start_child(EventCount, [ReadFuns, TransFun, WriteFun]);
            true ->
                lager:warning("dependence dag edge from ~w to ~w would form a cycle~n", [From, To]),
                %% @todo propagate errors
                {ok, ignore}
        end
    end.

%% @doc Track a function in the dag.
%%
%%      Given the input and output variables, and a function,
%%      create a single-fire lasp process representing the dataflow
%%      computation.
%%
%%      This function is synchronous, as it waits for a value to
%%      be returned.
%%
single_fire_function(From, To, Fn, Args) ->
    Self = self(),
    Ref = erlang:make_ref(),

    %% We don't care for the input.
    ReadFun = [{From, fun(_, _) ->
        {ok, ignore}
    end}],

    %% The process function just executes the given function
    TransFun = fun(_) ->
        erlang:apply(Fn, Args)
    end,

    %% The write function "returns" the value by sending a message.
    WriteFun = {To, fun(_, Res) ->
        Self ! {Ref, Res}
    end},

    {ok, _Pid} = start_single_fire_process([ReadFun, TransFun, WriteFun]),

    receive {Ref, Result} -> Result end.

%%%===================================================================
%%% Callbacks
%%%===================================================================

%% @doc Initialize state.
init([nodag, ReadFuns, Function, WriteFun]) ->
    {ok, #state{read_funs=ReadFuns,
                trans_fun=Function,
                write_fun=WriteFun}};

init([ReadFuns, TransFun, {To, _}=WriteFun]) ->
    From = [Id || {Id, _} <- ReadFuns],
    case lasp_config:get(dag_enabled, ?DAG_ENABLED) of
        false -> ok;
        true ->
            ok = lasp_dependence_dag:add_edges(From, To, self(),
                                               ReadFuns,
                                               TransFun,
                                               WriteFun)
    end,
    {ok, #state{read_funs=ReadFuns,
                trans_fun=TransFun,
                write_fun=WriteFun}}.

%% @doc Return list of read functions.
read(#state{read_funs=ReadFuns0}=State) ->
    ReadFuns = [gen_read_fun(Id, ReadFun) || {Id, ReadFun} <- ReadFuns0],
    {ok, ReadFuns, State}.

%% @doc Computation to execute when inputs change.
process(Args, #state{trans_fun=Function, write_fun=WriteFunction}=State) ->
    Processed = case lists:any(fun(X) -> X =:= undefined end, Args) of
        true -> false;
        false ->
            case WriteFunction of
                undefined -> erlang:apply(Function, Args);
                {AccId, WFun} ->
                    WFun(AccId, erlang:apply(Function, Args))
            end,
            true
    end,
    {ok, {Processed, State}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @doc Generate ReadFun.
gen_read_fun(Id, ReadFun) ->
    fun(Value0) ->
                Value = case Value0 of
                    undefined ->
                        undefined;
                    {_, _, _, V} ->
                        V
                end,
                case ReadFun(Id, {strict, Value}) of
                    {ok, NewValue} ->
                        NewValue;
                    {error, not_found} ->
                        exit({lasp_process, not_found})
                end
        end.
