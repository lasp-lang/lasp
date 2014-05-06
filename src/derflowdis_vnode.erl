-module(derflowdis_vnode).
-behaviour(riak_core_vnode).
-include("derflowdis.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([bind/2,
         bind/3,
	 syncBind/2,
	 syncBind/3,
         read/1,
	 touch/1,
	 next/1,
	 isDet/1,
	 waitNeeded/1,
         declare/1,
         declare/2,
	 get_new_id/0,
	 put/4,
	 execute_and_put/5]).

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

-record(state, {partition, clock, table}).
-record(dv, {value, next = empty, waitingThreads = [], creator, lazy= false, bounded = false}). 

%% Extrenal API
bind(Id, Value) -> 
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Id)}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, derflowdis),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, {bind, Id, Value}, derflowdis_vnode_master).

bind(Id, Function, Args) -> 
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Id)}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, derflowdis),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, {bind, Id, Function, Args}, derflowdis_vnode_master).

syncBind(Id, Value) -> 
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Id)}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, derflowdis),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, {syncBind, Id, Value}, derflowdis_vnode_master).

syncBind(Id, Function, Args) -> 
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Id)}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, derflowdis),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, {syncBind, Id, Function, Args}, derflowdis_vnode_master).

read(Id) -> 
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Id)}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, derflowdis),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, {read, Id}, derflowdis_vnode_master).

touch(Id) -> 
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Id)}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, derflowdis),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, {touch, Id}, derflowdis_vnode_master).

next(Id) -> 
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Id)}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, derflowdis),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, {next, Id}, derflowdis_vnode_master).

isDet(Id) -> 
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Id)}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, derflowdis),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, {isDet, Id}, derflowdis_vnode_master).

declare(Id, Partition) -> 
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Id)}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, derflowdis),
    [{IndexNode, _Type}] = PrefList,
    io:format("I am gonna send it to ~w and my partition is ~w~n",[IndexNode, Partition]),
    riak_core_vnode_master:sync_spawn_command(IndexNode, {declare, Id}, derflowdis_vnode_master).

declare(Id) -> 
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Id)}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, derflowdis),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, {declare, Id}, derflowdis_vnode_master).

get_new_id() -> 
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(now())}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, derflowdis),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, get_new_id, derflowdis_vnode_master).

waitNeeded(Id) -> 
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Id)}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, derflowdis),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, {waitNeeded, Id}, derflowdis_vnode_master).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    Table=string:concat(integer_to_list(Partition), "dvstore"),
    Table_atom=list_to_atom(Table),
    ets:new(Table_atom, [set, named_table, public, {write_concurrency, true}]),
    {ok, #state { partition=Partition, clock=0, table=Table_atom }}.

handle_command(get_new_id, _From, State=#state{partition=Partition}) ->
    Clock = State#state.clock +1,
    {reply, {Clock,Partition}, State#state{clock=Clock}};

handle_command({declare, Id}, _From, State=#state{table=Table}) ->
    %io:format("Procces ~w declaring ~w~n",[From, Id]),
    V = #dv{value=empty},
    ets:insert(Table, {Id, V}),
    %io:format("End process ~w declaring ~w~n",[From, Id]),
    {reply, {id, Id}, State};

handle_command({bind, Id, F, Arg}, _From, State=#state{partition=Partition, table=Table}) ->
    [{_Key, V}] = ets:lookup(Table, Id),
    PrevNextKey = V#dv.next,
    if PrevNextKey == empty -> 
	Next = State#state.clock+1,
    	NextKey={Next, Partition},
    	declare(NextKey);
	true ->
	{Next, _} = PrevNextKey,
	NextKey= PrevNextKey
    end,
    spawn(derflowdis_vnode, execute_and_put, [F, Arg, NextKey, Id, Table]),
    {reply, {id, NextKey}, State#state{clock=Next}};

handle_command({bind,Id, Value}, _From, State=#state{partition=Partition, table=Table}) ->
    [{_Key,V}] = ets:lookup(Table, Id),
    PrevNextKey = V#dv.next,
    if PrevNextKey == empty -> 
	Next = State#state.clock+1,
    	NextKey={Next, Partition},
    	declare(NextKey);
	true ->
	{Next, _} = PrevNextKey,
	NextKey= PrevNextKey
    end,
    spawn(derflowdis_vnode, put, [Value, NextKey, Id, Table]),
    {reply, {id, NextKey}, State#state{clock=Next}};

handle_command({syncBind, Id, F, Arg}, _From, State=#state{partition=Partition, table=Table}) ->
    [{_Key, V}] = ets:lookup(Table, Id),
    PrevNextKey = V#dv.next,
    if PrevNextKey == empty -> 
	Next = State#state.clock+1,
    	NextKey={Next, Partition},
    	declare(NextKey);
	true ->
	{Next, _} = PrevNextKey,
	NextKey= PrevNextKey
    end,
    execute_and_put(F, Arg, NextKey, Id, Table),
    {reply, {id, NextKey}, State#state{clock=Next}};

handle_command({syncBind,Id, Value}, _From, State=#state{partition=Partition, table=Table}) ->
    %io:format("Process ~w binding ~w~n",[From, Id]),
    [{_Key,V}] = ets:lookup(Table, Id),
    PrevNextKey = V#dv.next,
    if PrevNextKey == empty -> 
	NextClock = get_next_key(State#state.clock, Partition),
    	NextKey={NextClock, Partition},
    	declare(NextKey);
    true ->
        %io:format("Very WEIRD binding case ~w~n",[Id]),
	NextClock = State#state.clock,
	%{Next, _} = PrevNextKey,
	NextKey= PrevNextKey
    end,
    put(Value, NextKey, Id, Table),
    %io:format("End process ~w binding ~w~n",[From, Id]),
    {reply, {id, NextKey}, State#state{clock=NextClock}};

handle_command({waitNeeded, Id}, From, State=#state{table=Table}) ->
    [{_Key,V}] = ets:lookup(Table, Id),
    case V#dv.waitingThreads of [_H|_T] ->
        {reply, ok, State};
        _ ->
        ets:insert(Table, {Id, V#dv{lazy=true, creator=From}}),
        {noreply, State}
    end;


handle_command({read,X}, From, State=#state{table=Table}) ->
        [{_Key,V}] = ets:lookup(Table, X),
        Value = V#dv.value,
        Bounded = V#dv.bounded,
        Creator = V#dv.creator,
        Lazy = V#dv.lazy,
        %%%Need to distinguish that value is not calculated or is the end of a list%%%
        if Bounded == true ->
	  %io:format("Process: ~w read for ~w~n",[From, X]),
          {reply, {Value, V#dv.next}, State};
         true ->
          if Lazy == true ->
                WT = lists:append(V#dv.waitingThreads, [From]),
                V1 = V#dv{waitingThreads=WT},
                ets:insert(Table, {X, V1}),
		replyToAll([Creator],ok),
                {noreply, State};
          true ->
		io:format("Process: ~w waiting for ~w~n",[From, X]),
                WT = lists:append(V#dv.waitingThreads, [From]),
                V1 = V#dv{waitingThreads=WT},
                ets:insert(Table, {X, V1}),
	  	%io:format("End process: ~w waiting for ~w~n",[From, X]),
                {noreply, State}
          end
        end;

handle_command({touch,X}, _From, State=#state{partition=Partition,clock=Clock, table=Table}) ->
        [{_Key,V}] = ets:lookup(Table, X),
        Value = V#dv.value,
        Bounded = V#dv.bounded,
        Creator = V#dv.creator,
        Lazy = V#dv.lazy,
        %%%Need to distinguish that value is not calculated or is the end of a list%%%
        if Bounded == true ->
          {reply, {Value, V#dv.next}, State};
         true ->
	  Next = Clock+1,
	  NextKey = {Next, Partition},
    	  declare(NextKey),
          V1 = V#dv{next=NextKey},
          ets:insert(Table, {X, V1}),
          if Lazy == true ->
		replyToAll([Creator],ok),
                {reply, NextKey, State#state{clock=Next}};
          true ->
                {reply, NextKey, State#state{clock=Next}}
          end
        end;

handle_command({next,X}, _From, State=#state{partition=Partition,clock=Clock,table=Table}) ->
        [{_Key,V}] = ets:lookup(Table, X),
        PrevNextKey = V#dv.next,
	if PrevNextKey == empty ->
	  Next = Clock+1,
	  NextKey = {Next, Partition},
    	  declare(NextKey),
          V1 = V#dv{next=NextKey},
          ets:insert(Table, {X, V1}),
	  {reply, NextKey, State#state{clock=Next}}; 
	true ->
	   {reply, PrevNextKey, State}
	end;

handle_command({isDet,Id}, _From, State=#state{table=Table}) ->
        [{_Key,V}] = ets:lookup(Table, Id),
        Bounded = V#dv.bounded,
	{reply, Bounded, State};

	

handle_command(Message, _Sender, State) ->
    ?PRINT({unhandled_command, Message}),
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=FoldFun, acc0=Acc0}, _Sender,
                       #state{table=Table}=State) ->
    F = fun({Key, Operation}, Acc) -> FoldFun(Key, Operation, Acc) end,
    Acc = ets:foldl(F, Acc0, Table),
    {reply, Acc, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, #state{table=Table}=State) ->
    {Key, Operation} = binary_to_term(Data),
    Response = dets:insert_new(Table, {Key, Operation}),
    {reply, Response, State}.

encode_handoff_item(Key, Operation) ->
    term_to_binary({Key, Operation}).

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

put(Value, Next, Key, Table) ->
    [{_Key,V}] = ets:lookup(Table, Key),
    Threads = V#dv.waitingThreads,
    V1 = #dv{value= Value, next =Next, lazy=false, bounded= true},
    ets:insert(Table, {Key, V1}),
    replyToAll(Threads, {Value,Next}).

execute_and_put(F, Arg, Next, Key, Table) ->
    [{_Key,V}] = ets:lookup(Table, Key),
    Threads = V#dv.waitingThreads,
    Value = F(Arg),
    V1 = #dv{value= Value, next =Next, lazy=false,bounded= true},
    ets:insert(Table, {Key, V1}),
    replyToAll(Threads, {Value, Next}).

replyToAll([], _Result) ->
    ok;

replyToAll([H|T], Result) ->
    {server, undefined,{Address, Ref}} = H,
    io:format("Notifying ~w reply ~w~n", [H, Result]),
    gen_server:reply({Address, Ref}, Result),
    replyToAll(T, Result).

get_next_key(Clock, Partition) ->
    NextKey={NextClock=Clock+1, Partition},
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(NextKey)}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, derflowdis),
    [{{Index, _Node}, _Type}] = PrefList,
    if Index==Partition ->
	get_next_key(NextClock, Partition);
    true ->
	NextClock
    end.
