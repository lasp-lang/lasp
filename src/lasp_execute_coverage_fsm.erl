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

-module(lasp_execute_coverage_fsm).
-author('Christopher Meiklejohn <cmeiklejohn@basho.com>').

-behaviour(riak_core_coverage_fsm).

-include_lib("lasp.hrl").

-export([execute/1,
         execute/2]).

-export([init/2,
         process_results/2,
         finish/2]).

-record(state, {from, type, module, result}).

%% ===================================================================
%% API functions
%% ===================================================================

execute(Module) ->
    execute(Module, 1).

execute(Module, NVal) ->
    ReqId = ?REQID(),
    _ = lasp_execute_coverage_fsm_sup:start_child([{raw, ReqId, self()},
                                                   [?TIMEOUT, NVal, Module]]),
    {ok, ReqId}.

init(From={_, _, _}, [Timeout, NVal, Module]) ->
    Req = ?EXECUTE_REQUEST{module=Module},
    Type = Module:type(),
    Result = Type:new(),
    {Req, all, NVal, 1, lasp, lasp_vnode_master, Timeout,
     #state{from=From, type=Type, module=Module, result=Result}}.

process_results({error, Reason}, _State) ->
    {error, Reason};
process_results({done, Response},
                #state{type=Type, result=Result0}=State) ->
    Result = Type:merge(Response, Result0),
    {done, State#state{result=Result}};
process_results({stream, _From, {Key, Value}},
                #state{result=Result0}=State) ->
    Result = gb_trees:enter(Key, Value, Result0),
    {ok, State#state{result=Result}};
process_results(done, State) ->
    {done, State};
process_results(Message, State) ->
    lager:info("Unhandled result: ~p", [Message]),
    {ok, State}.

finish({error, Reason}=Error,
       StateData=#state{from={raw, ReqId, ClientPid}}) ->
    lager:info("Finish triggered with error: ~p", [Reason]),
    ClientPid ! {ReqId, Error},
    {stop, normal, StateData};
finish(clean,
       StateData=#state{from={raw, ReqId, ClientPid},
                        type=Type,
                        module=Module,
                        result=Result}) ->
    %% First, compute the value from the CRDT.
    Value0 = Type:value(Result),
    lager:info("Computed value is: ~p", [Value0]),

    %% Then, call the applications value function that will filter the
    %% response.
    Value = Module:value(Value0),

    %% Finally, send the response to the client.
    ClientPid ! {ReqId, ok, Value},

    {stop, normal, StateData};
finish(Message, StateData) ->
    lager:info("Unhandled finish: ~p", [Message]),
    {stop, normal, StateData}.
