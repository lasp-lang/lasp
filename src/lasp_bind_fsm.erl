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

-module(lasp_bind_fsm).
-author('Christopher Meiklejohn <cmeiklejohn@basho.com>').

-behaviour(gen_fsm).

-include("lasp.hrl").

%% API
-export([start_link/4,
         bind/2]).

%% Callbacks
-export([init/1,
         code_change/4,
         handle_event/3,
         handle_info/3,
         handle_sync_event/4,
         terminate/3]).

%% States
-export([prepare/2,
         execute/2,
         waiting/2,
         waiting_n/2,
         finalize/2]).

-record(state, {preflist,
                req_id,
                coordinator,
                from,
                id,
                type,
                value,
                values = [],
                num_responses,
                replies = []}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(ReqId, From, Id, Value) ->
    gen_fsm:start_link(?MODULE, [ReqId, From, Id, Value], []).

%% @doc Bind a variable.
bind(Id, Value) ->
    ReqId = lasp:mk_reqid(),
    _ = lasp_bind_fsm_sup:start_child([ReqId, self(), Id, Value]),
    {ok, ReqId}.

%%%===================================================================
%%% Callbacks
%%%===================================================================

handle_info(_Info, _StateName, StateData) ->
    {stop, badmsg, StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop, badmsg, StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop, badmsg, StateData}.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the request.
init([ReqId, From, Id, Value]) ->
    State = #state{preflist=undefined,
                   req_id=ReqId,
                   coordinator=node(),
                   from=From,
                   id=Id,
                   value=Value,
                   num_responses=0,
                   replies=[]},
    {ok, prepare, State, 0}.

%% @doc Prepare request by retrieving the preflist.
prepare(timeout, #state{id=Id}=State) ->
    Preflist = lasp:preflist(?N, Id, lasp),
    Preflist2 = [{Index, Node} || {{Index, Node}, _Type} <- Preflist],
    {next_state, execute, State#state{preflist=Preflist2}, 0}.

%% @doc Execute the request.
execute(timeout, #state{preflist=Preflist,
                        req_id=ReqId,
                        id=Id,
                        value=Value,
                        coordinator=Coordinator}=State) ->
    lasp_vnode:bind(Preflist, {ReqId, Coordinator}, Id, Value),
    {next_state, waiting, State}.

waiting({ok, _ReqId, IndexNode, {Id, Type, Value} = Reply},
        #state{from=From,
               req_id=ReqId,
               values=Values0,
               num_responses=NumResponses0,
               replies=Replies0}=State0) ->
    NumResponses = NumResponses0 + 1,
    Values = [{IndexNode, Value}|Values0],
    Replies = [{IndexNode, Reply}|Replies0],
    State = State0#state{num_responses=NumResponses,
                         replies=Replies,
                         id=Id,
                         type=Type,
                         values=Values},

    case NumResponses =:= ?R of
        true ->
            From ! {ReqId, ok, Reply},

            case NumResponses =:= ?N of
                true ->
                    {next_state, finalize, State, 0};
                false ->
                    {next_state, waiting_n, State}
            end;
        false ->
            {next_state, waiting, State}
    end.

waiting_n({ok, _ReqId, IndexNode, {Id, Type, Value} = Reply},
        #state{num_responses=NumResponses0,
               values=Values0,
               replies=Replies0}=State0) ->
    NumResponses = NumResponses0 + 1,
    Values = [{IndexNode, Value}|Values0],
    Replies = [{IndexNode, Reply}|Replies0],
    State = State0#state{num_responses=NumResponses,
                         replies=Replies,
                         id=Id,
                         type=Type,
                         values=Values},

    case NumResponses =:= ?N of
        true ->
            {next_state, finalize, State, 0};
        false ->
            {next_state, waiting_n, State}
    end.

finalize(timeout, #state{type=Type, values=Values}=State) ->
    MObj = merge(Type, Values),
    case needs_repair(Type, MObj, Values) of
        true ->
            repair(State, MObj, Values),
            {stop, normal, State};
        false ->
            {stop, normal, State}
    end.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% @doc Merge a series of replies.
merge(Type, Values) ->
    Objs = [Obj || {_, Obj} <- Values],
    lists:foldl(fun(X, Acc) -> Type:merge(X, Acc) end,
                Type:new(), Objs).

%% @doc Given the merged object `MObj' and a list of `Values'
%%      determine if repair is needed.
needs_repair(Type, MObj, Values) ->
    Objs = [Obj || {_, Obj} <- Values],
    lists:any(different(Type, MObj), Objs).

%% @doc Determine if two objects are different.
different(Type, A) ->
    fun(B) ->
        not Type:equal(A,B)
    end.

%% @doc Repair any vnodes that do not have the correct object.
repair(_, _, []) -> io;

repair(#state{id=Id, type=Type}=State, MObj, [{IdxNode, Obj}|T]) ->
    case Type:equal(MObj, Obj) of
        true ->
            repair(State, MObj, T);
        false ->
            lasp_vnode:repair(IdxNode, Id, Type, MObj),
            repair(State, MObj, T)
    end.
