-module(derflow_vnode).
-behaviour(riak_core_vnode).
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
         wait_needed/1,
         declare/1,
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

-ignore_xref([start_vnode/1]).

-record(state, {partition, clock, table}).
-record(dv, {value, next = empty, waiting_threads = [], binding_list = [], creator, lazy= false, bounded = false}).

%% Extrenal API

async_bind(Id, Value) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Id)}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, derflow),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, {async_bind, Id, Value}, derflow_vnode_master).

async_bind(Id, Function, Args) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Id)}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, derflow),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, {async_bind, Id, Function, Args}, derflow_vnode_master).

bind(Id, Value) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Id)}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, derflow),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, {bind, Id, Value}, derflow_vnode_master).

bind(Id, Function, Args) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Id)}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, derflow),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, {bind, Id, Function, Args}, derflow_vnode_master).

read(Id) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Id)}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, derflow),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, {read, Id}, derflow_vnode_master).

touch(Id) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Id)}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, derflow),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, {touch, Id}, derflow_vnode_master).

next(Id) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Id)}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, derflow),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, {next, Id}, derflow_vnode_master).

is_det(Id) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Id)}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, derflow),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, {is_det, Id}, derflow_vnode_master).

declare(Id) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Id)}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, derflow),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, {declare, Id}, derflow_vnode_master).

fetch(Id, FromId, FromP) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Id)}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, derflow),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:command(IndexNode, {fetch, Id, FromId, FromP}, derflow_vnode_master).

reply_fetch(Id, FromP, DV) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Id)}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, derflow),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:command(IndexNode, {reply_fetch, Id, FromP, DV}, derflow_vnode_master).

notify_value(Id, Value) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Id)}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, derflow),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:command(IndexNode, {notify_value, Id, Value}, derflow_vnode_master).

get_new_id() ->
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(now())}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, derflow),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, get_new_id, derflow_vnode_master).

wait_needed(Id) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Id)}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, derflow),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, {wait_needed, Id}, derflow_vnode_master).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    Table = string:concat(integer_to_list(Partition), "dvstore"),
    TableAtom = list_to_atom(Table),
    TableAtom = ets:new(TableAtom, [set, named_table, public, {write_concurrency, true}]),
    {ok, #state {partition=Partition, clock=0, table=TableAtom}}.

handle_command(get_new_id, _From,
               State=#state{clock=Clock0, partition=Partition}) ->
    Clock = Clock0 + 1,
    {reply, {Clock,Partition}, State#state{clock=Clock}};

handle_command({declare, Id}, _From, State=#state{table=Table}) ->
    true = ets:insert(Table, {Id, #dv{value=empty}}),
    {reply, {ok, Id}, State};

handle_command({async_bind, Id, F, Arg}, _From,
               State=#state{partition=Partition, table=Table}) ->
    [{_Key, V}] = ets:lookup(Table, Id),
    PrevNextKey = V#dv.next,
    if
        PrevNextKey == empty ->
            Next = State#state.clock + 1,
            NextKey = {Next, Partition},
            declare(NextKey);
        true ->
            {Next, _} = PrevNextKey,
            NextKey= PrevNextKey
        end,
    spawn(derflow_vnode, execute_and_put, [F, Arg, NextKey, Id, Table]),
    {reply, {ok, NextKey}, State#state{clock=Next}};

handle_command({async_bind, Id, Value}, _From,
               State=#state{partition=Partition, table=Table}) ->
    [{_Key,V}] = ets:lookup(Table, Id),
    PrevNextKey = V#dv.next,
    if
        PrevNextKey == empty ->
            Next = State#state.clock + 1,
            NextKey={Next, Partition},
            declare(NextKey);
        true ->
            {Next, _} = PrevNextKey,
            NextKey= PrevNextKey
    end,
    spawn(derflow_vnode, put, [Value, NextKey, Id, Table]),
    {reply, {ok, NextKey}, State#state{clock=Next}};

handle_command({bind, Id, Fun, Arg}, _From,
               State=#state{partition=Partition, table=Table}) ->
    [{_Key, V}] = ets:lookup(Table, Id),
    {NextClock, NextKey} = next_key(V#dv.next, State#state.clock, Partition),
    execute_and_put(Fun, Arg, NextKey, Id, Table),
    {reply, {ok, NextKey}, State#state{clock=NextClock}};

handle_command({bind, Id, Value}, From,
               State=#state{partition=Partition, table=Table}) ->
    case Value of
        {id, DVId} ->
            true = ets:insert(Table, {Id, #dv{value={id,DVId}}}),
            fetch(DVId, Id, From),
            {noreply, State};
        _ ->
            [{_Key,V}] = ets:lookup(Table, Id),
            {NextClock, NextKey} = next_key(V#dv.next,
                                            State#state.clock,
                                            Partition),
            put(Value, NextKey, Id, Table),
            {reply, {ok, NextKey}, State#state{clock=NextClock}}
        end;

handle_command({fetch, TargetId, FromId, FromP}, _From,
               State=#state{partition=Partition,clock= Clock, table=Table}) ->
    [{_, DV}] = ets:lookup(Table, TargetId),
    if
        DV#dv.bounded == true ->
            reply_fetch(FromId, FromP, DV),
            {noreply, State};
        true ->
            case DV#dv.value of
                {id, BindId} ->
                    fetch(BindId, FromId, FromP),
                    {noreply, State};
                _ ->
                    {NextClock, NextKey} = next_key(DV#dv.next, Clock, Partition),
                    BindingList = lists:append(DV#dv.binding_list, [FromId]),
                    DV1 = DV#dv{binding_list=BindingList, next=NextKey},
                    true = ets:insert(Table, {TargetId, DV1}),
                    reply_fetch(FromId, FromP, DV1),
                    {noreply, State#state{clock=NextClock}}
                end
    end;

handle_command({reply_fetch, FromId, FromP, FetchDV}, _From,
               State=#state{table=Table}) ->
      if
        FetchDV#dv.bounded == true ->
            Value = FetchDV#dv.value,
            Next = FetchDV#dv.next,
            put(Value, Next, FromId, Table),
            reply_to_all([FromP], {ok, Next});
        true ->
            [{_,DV}] = ets:lookup(Table, FromId),
            DV1 = DV#dv{next= FetchDV#dv.next},
            ets:insert(Table, {FromId, DV1}),
            reply_to_all([FromP], {ok, FetchDV#dv.next})
      end,
      {noreply, State};

handle_command({notify_value, Id, Value}, _From,
               State=#state{table=Table}) ->
    [{_, DV}] = ets:lookup(Table, Id),
    Next = DV#dv.next,
    put(Value, Next, Id, Table),
    {noreply, State};

handle_command({wait_needed, Id}, From,
               State=#state{table=Table}) ->
    [{_Key, V}] = ets:lookup(Table, Id),
    if
        V#dv.bounded == true ->
            {reply, ok, State};
        true ->
            case V#dv.waiting_threads of
                [_H|_T] ->
                    {reply, ok, State};
                _ ->
                    true = ets:insert(Table, {Id, V#dv{lazy=true, creator=From}}),
                    {noreply, State}
                end
    end;

handle_command({read, X}, From,
               State=#state{table=Table}) ->
    [{_Key, V}] = ets:lookup(Table, X),
    Value = V#dv.value,
    Bounded = V#dv.bounded,
    Creator = V#dv.creator,
    Lazy = V#dv.lazy,
    if
        Bounded == true ->
            {reply, {ok, Value, V#dv.next}, State};
        true ->
            if
                Lazy == true ->
                    WT = lists:append(V#dv.waiting_threads, [From]),
                    V1 = V#dv{waiting_threads=WT},
                    true = ets:insert(Table, {X, V1}),
                    reply_to_all([Creator], ok),
                    {noreply, State};
                true ->
                    WT = lists:append(V#dv.waiting_threads, [From]),
                    V1 = V#dv{waiting_threads=WT},
                    true = ets:insert(Table, {X, V1}),
                    {noreply, State}
            end
    end;

handle_command({touch, X}, _From,
               State=#state{partition=Partition,clock=Clock, table=Table}) ->
    [{_Key, V}] = ets:lookup(Table, X),
    Value = V#dv.value,
    Bounded = V#dv.bounded,
    Creator = V#dv.creator,
    Lazy = V#dv.lazy,
    if
        Bounded == true ->
            {reply, {Value, V#dv.next}, State};
        true ->
            Next = Clock + 1,
            NextKey = {Next, Partition},
            declare(NextKey),
            V1 = V#dv{next=NextKey},
            true = ets:insert(Table, {X, V1}),
            if
                Lazy == true ->
                    reply_to_all([Creator], ok),
                    {reply, NextKey, State#state{clock=Next}};
                true ->
                    {reply, NextKey, State#state{clock=Next}}
            end
    end;

handle_command({next, X}, _From,
               State = #state{partition = Partition,clock = Clock,table = Table}) ->
    [{_Key,V}] = ets:lookup(Table, X),
    PrevNextKey = V#dv.next,
    if
        PrevNextKey == empty ->
            Next = Clock+1,
            NextKey = {Next, Partition},
            declare(NextKey),
            V1 = V#dv{next=NextKey},
            true = ets:insert(Table, {X, V1}),
            {reply, NextKey, State#state{clock=Next}};
        true ->
           {reply, PrevNextKey, State}
  end;

handle_command({is_det, Id}, _From, State = #state{table = Table}) ->
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
    true = ets:insert_new(Table, {Key, Operation}),
    {reply, ok, State}.

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

%% Internal functions

put(Value, Next, Key, Table) ->
    [{_Key,V}] = ets:lookup(Table, Key),
    Threads = V#dv.waiting_threads,
    BindingList = V#dv.binding_list,
    V1 = #dv{value= Value, next =Next, lazy=false, bounded= true},
    true = ets:insert(Table, {Key, V1}),
    notify_all(BindingList, Value),
    reply_to_all(Threads, {ok, Value, Next}).

execute_and_put(F, Arg, Next, Key, Table) ->
    [{_Key,V}] = ets:lookup(Table, Key),
    Threads = V#dv.waiting_threads,
    BindingList = V#dv.binding_list,
    Value = F(Arg),
    V1 = #dv{value = Value, next = Next, lazy = false, bounded = true},
    true = ets:insert(Table, {Key, V1}),
    notify_all(BindingList, Value),
    reply_to_all(Threads, {ok, Value, Next}).

next_key(PrevNextKey, Clock, Partition) ->
    if
        PrevNextKey == empty ->
            NextClock = get_next_key(Clock, Partition),
            NextKey={NextClock, Partition},
            declare(NextKey);
        true ->
            NextClock = Clock,
            NextKey = PrevNextKey
    end,
    {NextClock, NextKey}.

reply_to_all([], _Result) ->
    ok;

reply_to_all([H|T], Result) ->
    {server, undefined,{Address, Ref}} = H,
    gen_server:reply({Address, Ref}, Result),
    reply_to_all(T, Result).

notify_all(L, Value) ->
    case L of
        [H|T] ->
            notify_value(H, Value),
            notify_all(T, Value);
        [] ->
            ok
    end.

get_next_key(Clock, Partition) ->
    NextKey = {NextClock = Clock + 1, Partition},
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(NextKey)}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, derflow),
    [{{Index, _Node}, _Type}] = PrefList,
    if
        Index == Partition ->
            get_next_key(NextClock, Partition);
        true ->
            NextClock
    end.
