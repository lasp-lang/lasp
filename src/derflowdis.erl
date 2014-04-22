-module(derflowdis).
-include("derflowdis.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
	 bind/2,
	 bind/3,
	 read/1,
	 declare/0,
	 thread/3,
	 get_stream/1,
	 async_print_stream/1]).

%% Public API

%ping() ->
%    DocIdx = riak_core_util:chash_key({<<"ping">>, term_to_binary(now())}),
%    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, derflowdis),
%    [{IndexNode, _Type}] = PrefList,
%    riak_core_vnode_master:sync_spawn_command(IndexNode, ping, derflowdis_vnode_master).
	
bind(Id, Value) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(now())}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, derflowdis),
    [{IndexNode, _Type}] = PrefList,
    derflowdis_vnode:bind(IndexNode, Id, Value).

bind(Id, Function, Args) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(now())}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, derflowdis),
    [{IndexNode, _Type}] = PrefList,
    derflowdis_vnode:bind(IndexNode, Id, Function, Args).

read(Id) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(now())}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, derflowdis),
    [{IndexNode, _Type}] = PrefList,
    derflowdis_vnode:read(IndexNode, Id).

declare() ->
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(now())}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, derflowdis),
    [{IndexNode, _Type}] = PrefList,
    derflowdis_vnode:declare(IndexNode).

thread(Module, Function, Args) ->
    spawn(Module, Function, Args).

get_stream(Stream)->
    internal_get_stream(Stream, []).

async_print_stream(Stream)->
    io:format("Stream: ~w~n", [Stream]),
    case read(Stream) of
	{nil, _} -> {ok, stream_read};
	{Value, Next} -> 
	    io:format("~w~n",[Value]),
	    async_print_stream(Next)
    end.
    
%Internal functions

internal_get_stream(Head, Output) ->
    case read(Head) of
	{nil, _} -> Output;
	{Value, Next} -> 
	    internal_get_stream(Next, lists:append(Output, [Value]))
    end.
