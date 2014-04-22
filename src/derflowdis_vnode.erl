-module(derflowdis_vnode).
-behaviour(riak_core_vnode).
-include("derflowdis.hrl").

-export([bind/3,
         bind/4,
         read/2,
         declare/1,
	 put/3,
	 execute_and_put/4]).

-export([start_vnode/1,
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_exit/3]).

-ignore_xref([
             start_vnode/1
             ]).

-record(state, {partition, clock}).
-record(dv, {value, next, waitingThreads = [], bounded = false}). 

%% Extrenal API
bind(IndexNode, Id, Value) -> 
    riak_core_vnode_master:sync_spawn_command(IndexNode, {bind, Id, Value}, derflowdis_vnode_master).

bind(IndexNode, Id, Function, Args) -> 
    riak_core_vnode_master:sync_spawn_command(IndexNode, {bind, Id, Function, Args}, derflowdis_vnode_master).

read(IndexNode, Id) -> 
    riak_core_vnode_master:sync_spawn_command(IndexNode, {read, Id}, derflowdis_vnode_master).

declare(IndexNode) -> 
    riak_core_vnode_master:sync_spawn_command(IndexNode, declare, derflowdis_vnode_master).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    ets:new(dvstore, [set, named_table, public, {write_concurrency, true}]),
    {ok, #state { partition=Partition, clock=0 }}.

%% Sample command: respond to a ping
handle_command({declare}, _From, State) ->
    Clock = State#state.clock +1,
    V = #dv{value=empty, next=empty},
    ets:insert(dvstore, {Clock, V}),
    {reply, {id, Clock}, State#state{clock=Clock}};

handle_command({bind, Id, F, Arg}, _From, State) ->
    io:format("Bind request~n"),
    Next = State#state.clock+1,
    ets:insert(dvstore, {Next, #dv{value=empty, next=empty}}),
    spawn(derflow_server, execute_and_put, [F, Arg, Next, Id]),
    {reply, {id, Next}, State#state{clock=Next}};

handle_command({bind,Id, Value}, _From, State) ->
    io:format("Bind request~n"),
    Next = State#state.clock+1,
    ets:insert(dvstore, {Next, #dv{value=empty, next=empty}}),
    spawn(derflow_server, put, [Value, Next, Id]),
    {reply, {id, Next}, State#state{clock=Next}};
%%%What if the Key does not exist in the map?%%%
handle_command({read,X}, From, State) ->
    [{_Key,V}] = ets:lookup(dvstore, X),
    Value = V#dv.value,
    Bounded = V#dv.bounded,
    %%%Need to distinguish that value is not calculated or is the end of a list%%%
    if Bounded == true ->
	{reply, {Value, V#dv.next}, State};
    true ->
	WT = lists:append(V#dv.waitingThreads, [From]),
	V1 = V#dv{waitingThreads=WT},
	ets:delete(dvstore, X),
	ets:insert(dvstore, {X, V1}),
	{noreply, State}
    end;

handle_command(Message, _Sender, State) ->
    ?PRINT({unhandled_command, Message}),
    {noreply, State}.

handle_handoff_command(_Message, _Sender, State) ->
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(_Data, State) ->
    {reply, ok, State}.

encode_handoff_item(_ObjectName, _ObjectValue) ->
    <<>>.

is_empty(State) ->
    {true, State}.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%Internal functions

put(Value, Next, Key) ->
    [{_Key,V}] = ets:lookup(dvstore, Key),
    Threads = V#dv.waitingThreads,
    V1 = #dv{value= Value, next =Next, bounded= true},
    ets:insert(dvstore, {Key, V1}),
    replyToAll(Threads, Value, Next).

execute_and_put(F, Arg, Next, Key) ->
    [{_Key,V}] = ets:lookup(dvstore, Key),
    Threads = V#dv.waitingThreads,
    Value = F(Arg),
    V1 = #dv{value= Value, next =Next, bounded= true},
    ets:insert(dvstore, {Key, V1}),
    replyToAll(Threads, Value, Next).

replyToAll([], _Value, _Next) ->
    ok;

replyToAll([H|T], Value, Next) ->
    gen_server:reply(H,{Value,Next}),
    replyToAll(T, Value, Next).

