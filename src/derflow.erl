-module(derflow).
-include("derflow.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([declare/0,
         declare/1,
         bind/2,
         bind/4,
         read/1,
         read/2,
         produce/2,
         produce/4,
         consume/1,
         extend/1,
         is_det/1,
         wait_needed/1,
         spawn_mon/4,
         thread/3,
         preflist/3,
         get_stream/1]).

%% Public API

declare() ->
    declare(undefined).

declare(Type) ->
    derflow_vnode:declare(druuid:v4(), Type).

bind(Id, Value) ->
    case derflow_vnode:bind(Id, Value) of
        {ok, _} ->
            ok;
        error ->
            error
    end.

bind(Id, Module, Function, Args) ->
    bind(Id, Module:Function(Args)).

read(Id) ->
    {ok, Value, _Next} = derflow_vnode:read(Id),
    {ok, Value}.

%% @doc Blocking threshold read.
read(Id, Threshold) ->
    {ok, Value, _Next} = derflow_vnode:read(Id, Threshold),
    {ok, Value}.

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

%% The rest of primitives are not in the paper. They are just utilities.
%% maybe we should just remove them

spawn_mon(Supervisor, Module, Function, Args) ->
    {ok, Pid} = thread(Module, Function, Args),
    Supervisor ! {'SUPERVISE', Pid, Module, Function, Args}.

get_stream(Stream)->
    internal_get_stream(Stream, []).

%% Internal functions

internal_get_stream(Head, Output) ->
    lager:info("About to consume: ~p", [Head]),
    case consume(Head) of
        {ok, undefined, _} ->
            lager:info("Received: ~p", [undefined]),
            Output;
        {ok, Value, Next} ->
            lager:info("Received: ~p", [Value]),
            internal_get_stream(Next, lists:append(Output, [Value]))
    end.

%% @doc Generate a preference list for a given N value and data item.
preflist(NVal, Param, VNode) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Param)}),
    riak_core_apl:get_primary_apl(DocIdx, NVal, VNode).
