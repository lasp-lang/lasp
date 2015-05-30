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

-module(gen_flow_example).
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
-record(state, {source}).

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
init([Source]) ->
    {ok, #state{source=Source}}.

%% @doc Return list of read functions.
read(State) ->
    ReadFuns = [fun(_) -> sets:from_list([1,2,3]) end,
                fun(_) -> sets:from_list([3,4,5]) end],
    {ok, ReadFuns, State}.

%% @doc Computation to execute when inputs change.
process(Args, #state{source=Source}=State) ->
    case Args of
        [undefined, _] ->
            ok;
        [_, undefined] ->
            ok;
        [X, Y] ->
            Set = sets:intersection(X, Y),
            Source ! {ok, sets:to_list(Set)},
            ok
    end,
    {ok, State}.

-ifdef(TEST).

gen_flow_test() ->
    {ok, _Pid} = gen_flow:start_link(gen_flow_example, [self()]),

    Response = receive
        ok ->
            ok;
        {ok, X} ->
            X
    end,

    ?assertEqual([3], Response).

-endif.
