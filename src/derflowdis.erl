-module(derflowdis).
-include("derflowdis.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
	 bind/2,
	 bind/3,
	 syncBind/2,
	 syncBind/3,
	 read/1,
	 touch/1,
	 next/1,
	 isDet/1,
	 declare/0,
	 thread/3,
	 waitNeeded/1,
	 get_stream/1,
	 async_print_stream/1]).

%% Public API

%ping() ->
%    DocIdx = riak_core_util:chash_key({<<"ping">>, term_to_binary(now())}),
%    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, derflowdis),
%    [{IndexNode, _Type}] = PrefList,
%    riak_core_vnode_master:sync_spawn_command(IndexNode, ping, derflowdis_vnode_master).
	
bind(Id, Value) ->
    derflowdis_vnode:bind(Id, Value).

bind(Id, Function, Args) ->
    derflowdis_vnode:bind(Id, Function, Args).

syncBind(Id, Value) ->
    derflowdis_vnode:syncBind(Id, Value).

syncBind(Id, Function, Args) ->
    derflowdis_vnode:syncBind(Id, Function, Args).

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

waitNeeded(Id) ->
    derflowdis_vnode:waitNeeded(Id).
    

thread(Module, Function, Args) ->
    spawn(Module, Function, Args).

get_stream(Stream)->
    internal_get_stream(Stream, []).

async_print_stream(Stream)->
    %io:format("Stream: ~w~n", [Stream]),
    %io:format("Before read async print~n"),
    case read(Stream) of
	{nil, _} ->
	    %io:format("After read async print: nil~n"), 
	    {ok, stream_read};
	{Value, Next} ->
	    %io:format("After read async print: ~w~n",[Value]), 
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
