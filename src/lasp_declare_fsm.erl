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

-module(lasp_declare_fsm).

-behaviour(gen_fsm).

-include("lasp.hrl").

-export([start_link/2]).
%% gen_fsm callbacks
-export([init/1, handle_event/3, handle_sync_event/4,
         handle_info/3, terminate/3, code_change/4]).
%% State-event callbacks
-export([execute/2, await_responses/2]).

-record(state, {from :: pid(),
                key,
                type,
                results=[] %% success responses
               }).

%% ===================================================================
%% Public API
%% ===================================================================

start_link(From, Type) ->
    gen_fsm:start_link(?MODULE, [From, Type], []).

%% ====================================================================
%% gen_fsm callbacks
%% ====================================================================

%% @private
init([From, Type]) ->
    {ok, execute, #state{from=From, type=Type}, 0}.

%% @private
execute(timeout, StateData=#state{type=Type}) ->
    Id = druuid:v4(),
    lasp_vnode:declare(Id, Type),
    {next_state, await_responses, StateData#state{key=Id}}.

await_responses({ok, Id}, StateData=#state{from=Pid, results=Results0}) ->
    Results = [Id | Results0],
    case length(Results) of
        ?W ->
            Pid ! {ok, Id},
            {stop, normal, StateData};
        _ ->
            {next_state, await_responses, StateData#state{results=Results}}
    end.

%% @private
handle_event(_Event, _StateName, StateData) ->
    {stop, badmsg, StateData}.

%% @private
handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop, badmsg, StateData}.

%% @private
handle_info(request_timeout, StateName, StateData) ->
    ?MODULE:StateName(request_timeout, StateData);
handle_info(_Info, _StateName, StateData) ->
    {stop, badmsg, StateData}.

%% @private
terminate(Reason, _StateName, _State) ->
    Reason.

%% @private
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================
