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

-include("lasp.hrl").

%% API
-export([start_link/2,
         start/2]).

%% Callbacks
-export([process/2]).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Reads, Function) when is_list(Reads) ->
    Scope = [{Id, #read{id=Id, read_fun=Fun}} || {Id, Fun} <- Reads],
    Pid = erlang:spawn_link(?MODULE, process, [Scope, Function]),
    {ok, Pid};
start_link(Scope, Function) ->
    Pid = erlang:spawn_link(?MODULE, process, [Scope, Function]),
    {ok, Pid}.

%% @doc Process a notification.
start(Scope, Function) ->
    lasp_process_sup:start_child([Scope, Function]).

%%%===================================================================
%%% Callbacks
%%%===================================================================

%% @doc Lasp process.
%%
%%      This function defines a supervised Lasp process; a Lasp process
%%      is responsible for processing one stage in the dataflow graph:
%%      reading from one-or-many input CRDTs, transforming them in some
%%      fashion, and binding a subsequent output CRDT.
%%
process(Scope0, Function) ->
    Self = self(),

    %% For every variable that has to be read, spawn a process to
    %% perform the read and have it forward the response back to the
    %% notifier, which is waiting for the first change to trigger
    %% re-evaluation.
    %%
    lists:foreach(fun({Id, #read{read_fun=ReadFun, value=Value}}) ->
        spawn_link(fun() ->
                    {ok, Result} = ReadFun(Id, {strict, Value}),
                    Self ! {ok, Result}
                   end)
        end, Scope0),

    %% Wait for the first variable to change; once it changes, update
    %% the dict and trigger the process which was waiting for
    %% notification which causes re-evaluation.
    %%
    receive
        {ok, {Id, Type, Value}} ->
            %% Store updated value in the dict.
            {_, Read} = ?SCOPE:keyfind(Id, 1, Scope0),
            Scope = ?SCOPE:keyreplace(Id, 1, Scope0,
                                      {Id, Read#read{value=Value, type=Type}}),

            %% Apply function with updated scope.
            Args = [{I, T, V} || {I, #read{type=T, value=V}} <- Scope],
            erlang:apply(Function, Args),

            process(Scope, Function);
        Error ->
            lager:info("Received error: ~p~n", [Error]),
            process(Scope0, Function)
    end.
