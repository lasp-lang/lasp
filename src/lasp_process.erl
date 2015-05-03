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
-export([start_link/3,
         start/3]).

%% Callbacks
-export([process/3]).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Variables, Reads, Function) when is_list(Reads) ->
    Scope = dict:from_list([{Id, #read{id=Id, read_fun=Fun}}
                            || {Id, Fun} <- Reads]),
    Pid = erlang:spawn_link(?MODULE, process, [Variables, Scope, Function]),
    {ok, Pid};
start_link(Variables, Scope, Function) ->
    Pid = erlang:spawn_link(?MODULE, process, [Variables, Scope, Function]),
    {ok, Pid}.

%% @doc Process a notification.
start(Variables, Scope, Function) ->
    lasp_process_sup:start_child([Variables, Scope, Function]).

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
process(Variables, Scope0, Function) ->
    Self = self(),

    %% For every variable that has to be read, spawn a process to
    %% perform the read and have it forward the response back to the
    %% notifier, which is waiting for the first change to trigger
    %% re-evaluation.
    %%
    lists:foreach(fun({Id, #read{read_fun=ReadFun, value=Value}}) ->
        spawn_link(fun() ->
                    {ok, Result} = ReadFun(Id, {strict, Value}, Variables),
                    Self ! {ok, Result}
                   end)
        end, dict:to_list(Scope0)),

    %% Wait for the first variable to change; once it changes, update
    %% the dict and trigger the process which was waiting for
    %% notification which causes re-evaluation.
    %%
    receive
        {ok, {Id, Type, Value}} = Message ->
            lager:info("Received message: ~p~n", [Message]),

            %% Store updated value in the dict.
            ReadRecord = dict:fetch(Id, Scope0),
            Scope = dict:store(Id,
                               ReadRecord#read{value=Value, type=Type}, 
                               Scope0),

            %% Apply function with updated scope.
            Function(Scope),

            process(Variables, Scope, Function);
        Error ->
            lager:info("Received error: ~p~n", [Error]),
            process(Variables, Scope0, Function)
    end.
