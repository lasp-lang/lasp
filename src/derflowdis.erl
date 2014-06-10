-module(derflowdis).
-include("derflowdis.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
	 async_bind/2,
	 async_bind/3,
	 bind/2,
	 bind/3,
	 read/1,
	 touch/1,
	 next/1,
	 isDet/1,
	 declare/0,
	 thread_mon/4,
	 thread/3,
	 wait_needed/1,
	 get_stream/1,
	 async_print_stream/1]).

%% Public API

async_bind(Id, Value) ->
    derflowdis_vnode:async_bind(Id, Value).

async_bind(Id, Function, Args) ->
    derflowdis_vnode:async_bind(Id, Function, Args).

bind(Id, Value) ->
    derflowdis_vnode:bind(Id, Value).

bind(Id, Function, Args) ->
    derflowdis_vnode:bind(Id, Function, Args).

read(Id) ->
    derflowdis_vnode:read(Id).

touch(Id) ->
    derflowdis_vnode:touch(Id).

next(Id) ->
    derflowdis_vnode:next(Id).

isDet(Id) ->
    derflowdis_vnode:isDet(Id).

declare() ->
    Id = derflowdis_vnode:get_new_id(),
    derflowdis_vnode:declare(Id).

wait_needed(Id) ->
    derflowdis_vnode:wait_needed(Id).

thread_mon(Supervisor, Module, Function, Args) ->
    PID = spawn(Module, Function, Args),
    Supervisor ! {'SUPERVISE', PID, Module, Function, Args}.

thread(Module, Function, Args) ->
    spawn(Module, Function, Args).

get_stream(Stream)->
    internal_get_stream(Stream, []).

async_print_stream(Stream)->
    case read(Stream) of
	{nil, _} ->
	    {ok, stream_read};
	{Value, Next} ->
	    io:format("~w~n",[Value]),
	    async_print_stream(Next);
	 Any ->
	    io:format("Stream any: ~w~n",[Any])

    end.
    
%Internal functions

internal_get_stream(Head, Output) ->
    case read(Head) of
	{nil, _} -> Output;
	{Value, Next} -> 
	    internal_get_stream(Next, lists:append(Output, [Value]))
    end.
