-module(derflow).
-include("derflow.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([declare/0,
         declare/1,
         bind/2,
         bind/4,
         read/1,
         produce/2,
         produce/4,
         consume/1,
         extend/1,
         is_det/1,
         wait_needed/1,
         spawn_mon/4,
         thread/3,
         get_stream/1]).

%% Public API

declare() ->
    declare(undefined).

declare(Type) ->
    Id = druuid:v4(),
    {ok, Id} = derflow_vnode:declare(Id, Type),
    {ok, Id}.

bind(Id, Value) ->
    case derflow_vnode:bind(Id, Value) of
        {ok, _} ->
            ok;
        error ->
            error
    end.

bind(Id, Module, Function, Args) ->
    Value = Module:Function(Args),
    bind(Id, Value).

read(Id) ->
    {ok, Value, _Next} = derflow_vnode:read(Id),
    {ok, Value}.

produce(Id, Value) ->
    derflow_vnode:bind(Id, Value).

produce(Id, Module, Function, Args) ->
    Value = Module:Function(Args),
    derflow_vnode:bind(Id, Value).

consume(Id) ->
    derflow_vnode:read(Id, undefined).

extend(Id) ->
    derflow_vnode:next(Id).

is_det(Id) ->
    derflow_vnode:is_det(Id).

thread(Module, Function, Args) ->
    {ok, Pid} = derflow_vnode:thread(Module, Function, Args),
    {ok, Pid}.

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
    case consume(Head) of
        {ok, nil, _} ->
            Output;
        {ok, Value, Next} ->
            internal_get_stream(Next, lists:append(Output, [Value]))
    end.
