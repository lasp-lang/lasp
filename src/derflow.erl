-module(derflow).
-include("derflow.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([async_bind/2,
         async_bind/3,
         bind/2,
         bind/3,
         read/1,
         touch/1,
         next/1,
         is_det/1,
         declare/0,
         thread_mon/4,
         thread/3,
         wait_needed/1,
         get_stream/1,
         async_print_stream/1]).

%% Public API

async_bind(Id, Value) ->
    derflow_vnode:async_bind(Id, Value).

async_bind(Id, Function, Args) ->
    derflow_vnode:async_bind(Id, Function, Args).

bind(Id, Value) ->
    derflow_vnode:bind(Id, Value).

bind(Id, Function, Args) ->
    derflow_vnode:bind(Id, Function, Args).

read(Id) ->
    derflow_vnode:read(Id).

touch(Id) ->
    derflow_vnode:touch(Id).

next(Id) ->
    derflow_vnode:next(Id).

is_det(Id) ->
    derflow_vnode:is_det(Id).

declare() ->
    Id = derflow_vnode:get_new_id(),
    derflow_vnode:declare(Id).

wait_needed(Id) ->
    derflow_vnode:wait_needed(Id).

thread_mon(Supervisor, Module, Function, Args) ->
    Pid = spawn(Module, Function, Args),
    Supervisor ! {'SUPERVISE', Pid, Module, Function, Args}.

thread(Module, Function, Args) ->
    spawn(Module, Function, Args).

get_stream(Stream)->
    internal_get_stream(Stream, []).

async_print_stream(Stream)->
    case read(Stream) of
        {ok, nil, _} ->
            {ok, stream_read};
        {ok, Value, Next} ->
            io:format("~w~n", [Value]),
            async_print_stream(Next);
         Any ->
            io:format("~w~n", [Any])
    end.

%% Internal functions

internal_get_stream(Head, Output) ->
    case read(Head) of
        {ok, nil, _} ->
            Output;
        {ok, Value, Next} ->
            internal_get_stream(Next, lists:append(Output, [Value]))
    end.
