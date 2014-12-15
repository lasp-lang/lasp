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

-module(derflow).

-include("derflow.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([declare/0,
         declare/1,
         bind/2,
         bind/4,
         read/1,
         read/2,
         foldl/3,
         produce/2,
         produce/4,
         consume/1,
         extend/1,
         is_det/1,
         wait_needed/1,
         spawn_mon/4,
         thread/3,
         preflist/3,
         get_stream/1,
         register/3,
         execute/2,
         mk_reqid/0]).

%% Public API

register(Module, File, preflist) ->
    {ok, ReqId} = derflow_register_fsm:register(Module, File),
    wait_for_reqid(ReqId, ?TIMEOUT);
register(Module, File, global) ->
    {ok, ReqId} = derflow_register_global_fsm:register(Module, File),
    wait_for_reqid(ReqId, ?TIMEOUT).

execute(Module, preflist) ->
    {ok, ReqId} = derflow_execute_fsm:execute(Module),
    wait_for_reqid(ReqId, ?TIMEOUT);
execute(Module, global) ->
    {ok, ReqId} = derflow_execute_coverage_fsm:execute(Module),
    wait_for_reqid(ReqId, ?TIMEOUT).

declare() ->
    declare(undefined).

declare(Type) ->
    derflow_vnode:declare(druuid:v4(), Type).

bind(Id, Value) ->
    derflow_vnode:bind(Id, Value).

bind(Id, Module, Function, Args) ->
    bind(Id, Module:Function(Args)).

read(Id) ->
    derflow_vnode:read(Id).

%% @doc Blocking threshold read.
read(Id, Threshold) ->
    derflow_vnode:read(Id, Threshold).

%% @doc Add foldl.
foldl(Id, Function, AccId) ->
    derflow_vnode:foldl(Id, Function, AccId).

produce(Id, Value) ->
    derflow_vnode:bind(Id, Value).

produce(Id, Module, Function, Args) ->
    derflow_vnode:bind(Id, Module:Function(Args)).

consume(Id) ->
    derflow_vnode:read(Id).

extend(Id) ->
    derflow_vnode:next(Id).

is_det(Id) ->
    derflow_vnode:is_det(Id).

thread(Module, Function, Args) ->
    derflow_vnode:thread(Module, Function, Args).

wait_needed(Id) ->
    derflow_vnode:wait_needed(Id).

spawn_mon(Supervisor, Module, Function, Args) ->
    {ok, Pid} = thread(Module, Function, Args),
    Supervisor ! {'SUPERVISE', Pid, Module, Function, Args}.

get_stream(Stream)->
    get_stream(Stream, []).

get_stream(Head, Output) ->
    lager:info("About to consume: ~p", [Head]),
    case consume(Head) of
        {ok, undefined, _} ->
            lager:info("Received: ~p", [undefined]),
            Output;
        {ok, Value, Next} ->
            lager:info("Received: ~p", [Value]),
            get_stream(Next, lists:append(Output, [Value]))
    end.

%% @doc Generate a preference list for a given N value and data item.
preflist(NVal, Param, VNode) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Param)}),
    riak_core_apl:get_primary_apl(DocIdx, NVal, VNode).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% @doc Generate a request id.
mk_reqid() ->
    erlang:phash2(erlang:now()).

%% @doc Wait for a response.
wait_for_reqid(ReqID, Timeout) ->
    receive
        {ReqID, ok} ->
            ok;
        {ReqID, ok, Val} ->
            {ok, Val}
    after Timeout ->
        {error, timeout}
    end.
