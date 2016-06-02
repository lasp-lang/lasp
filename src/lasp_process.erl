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
         start_dag_link/1]).

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

start_link(Args) ->
    lasp_process_sup:start_child(Args).

%% @todo rename to start_link once all functions are tracked
start_dag_link([ReadFuns, TransFun, {To, _}=WriteFun]) ->
    From = [Id || {Id, _} <- ReadFuns],
    case lasp_config:get(dag_enabled, false) of
        false -> lasp_process_sup:start_child([ReadFuns, TransFun, WriteFun]);
        true -> case lasp_dependence_dag:will_form_cycle(From, To) of
            false -> lasp_process_sup:start_child([ReadFuns, TransFun, WriteFun]);
            true ->
                %% @todo propagate errors
                {ok, ignore}
        end
    end.

%%%===================================================================
%%% Callbacks
%%%===================================================================

%% @doc Initialize state.
init([ReadFuns, Function]) ->
    {ok, #state{read_funs=ReadFuns,
                trans_fun=Function,
                write_fun=undefined}};

init([ReadFuns, TransFun, {To, _}=WriteFun]) ->
    From = [Id || {Id, _} <- ReadFuns],
    case lasp_config:get(dag_enabled, false) of
        true -> lasp_dependence_dag:add_edges(From, To, self());
        false -> ok
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
    case lists:any(fun(X) -> X =:= undefined end, Args) of
        true ->
            ok;
        false ->
            case WriteFunction of
                undefined -> erlang:apply(Function, Args);
                {AccId, WFun} ->
                    WFun(AccId, erlang:apply(Function, Args))
            end
    end,
    {ok, State}.

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
