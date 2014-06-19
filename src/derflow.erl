-module(derflow).
-include("derflow.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([bind/2,
         bind/4,
         read/1,
         touch/1,
         next/1,
         is_det/1,
         declare/0,
         declare/1,
         thread_mon/4,
         thread/3,
         wait_needed/1,
         get_stream/1]).

%% Public API

bind(Id, Value) ->
    _ = derflow_bind_fsm_sup:start_child([self(), Id, Value]),
    receive
        {ok, Next} ->
            {ok, Next}
    end.

bind(Id, Module, Function, Args) ->
    Value = Module:Function(Args),
    derflow_vnode:bind(Id, Value).

read(Id) ->
    _ = derflow_read_fsm_sup:start_child([self(), Id]),
    receive
        {ok, Value, Next} ->
            {ok, Value, Next}
    end.

touch(Id) ->
    _ = derflow_touch_fsm_sup:start_child([self(), Id]),
    receive
        {ok, Next} ->
            {ok, Next}
    end.

next(Id) ->
    _ = derflow_next_fsm_sup:start_child([self(), Id]),
    receive
        {ok, Next} ->
            {ok, Next}
    end.

is_det(Id) ->
    _ = derflow_is_det_fsm_sup:start_child([self(), Id]),
    receive
        {ok, Bool} ->
            {ok, Bool}
    end.

declare() ->
    declare(undefined).

declare(Type) ->
    _ = derflow_declare_fsm_sup:start_child([self(), Type]),
    receive
        {ok, Id} ->
            {ok, Id}
    end.

wait_needed(Id) ->
    _ = derflow_wait_needed_fsm_sup:start_child([self(), Id]),
    receive
        ok ->
            ok
    end.

thread_mon(Supervisor, Module, Function, Args) ->
    Pid = spawn(Module, Function, Args),
    Supervisor ! {'SUPERVISE', Pid, Module, Function, Args}.

thread(Module, Function, Args) ->
    spawn(Module, Function, Args).

get_stream(Stream)->
    internal_get_stream(Stream, []).

%% Internal functions

internal_get_stream(Head, Output) ->
    case read(Head) of
        {ok, nil, _} ->
            Output;
        {ok, Value, Next} ->
            internal_get_stream(Next, lists:append(Output, [Value]))
    end.
