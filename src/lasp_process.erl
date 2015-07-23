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
-export([start_link/1]).

%% Callbacks
-export([init/1, read/1, process/2]).

%% Records
-record(state, {read_funs, function}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Args) ->
    {ok, Pid} = gen_flow:start_link(?MODULE, Args),
    {ok, Pid}.

%%%===================================================================
%%% Callbacks
%%%===================================================================

%% @doc Initialize state.
init([ReadFuns, Function]) ->
    {ok, #state{read_funs=ReadFuns, function=Function}}.

%% @doc Return list of read functions.
read(#state{read_funs=ReadFuns0}=State) ->
    ReadFuns = [gen_read_fun(Id, ReadFun) || {Id, ReadFun} <- ReadFuns0],
    {ok, ReadFuns, State}.

%% @doc Computation to execute when inputs change.
process(Args, #state{function=Function}=State) ->
    case lists:any(fun(X) -> X =:= undefined end, Args) of
        true ->
            ok;
        false ->
            erlang:apply(Function, Args)
    end,
    {ok, State}.

%% @doc Generate ReadFun.
gen_read_fun(Id, ReadFun) ->
    fun(Value0) ->
                Value = case Value0 of
                    undefined ->
                        undefined;
                    {_, _, _, V} ->
                        V
                end,
                {ok, NewValue} = ReadFun(Id, {strict, Value}),
                NewValue
        end.
