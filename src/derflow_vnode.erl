-module(derflow_vnode).
-behaviour(riak_core_vnode).
-include("derflow.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-define(VNODE_MASTER, derflow_vnode_master).
-define(N, 1).

-export([bind/2,
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

-record(state, {partition, table}).

-record(dv, {value,
             next = undefined,
             waiting_threads = [],
             binding_list = [],
             creator,
             lazy = false,
             bounded = false}).

%% Extrenal API

bind(Id, Value) ->
    [{IndexNode, _Type}] = generate_preference_list(?N, Id),
    riak_core_vnode_master:sync_spawn_command(IndexNode,
                                              {bind, Id, Value},
                                              ?VNODE_MASTER).

bind(Id, Function, Args) ->
    [{IndexNode, _Type}] = generate_preference_list(?N, Id),
    riak_core_vnode_master:sync_spawn_command(IndexNode,
                                              {bind, Id, Function, Args},
                                              ?VNODE_MASTER).

read(Id) ->
    [{IndexNode, _Type}] = generate_preference_list(?N, Id),
    riak_core_vnode_master:sync_spawn_command(IndexNode,
                                              {read, Id},
                                              ?VNODE_MASTER).

touch(Id) ->
    [{IndexNode, _Type}] = generate_preference_list(?N, Id),
    riak_core_vnode_master:sync_spawn_command(IndexNode,
                                              {touch, Id},
                                              ?VNODE_MASTER).

next(Id) ->
    [{IndexNode, _Type}] = generate_preference_list(?N, Id),
    riak_core_vnode_master:sync_spawn_command(IndexNode,
                                              {next, Id},
                                              ?VNODE_MASTER).

is_det(Id) ->
    [{IndexNode, _Type}] = generate_preference_list(?N, Id),
    riak_core_vnode_master:sync_spawn_command(IndexNode,
                                              {is_det, Id},
                                              ?VNODE_MASTER).

declare(Id) ->
    Preflist = generate_preference_list(3, Id),
    Preflist2 = [IndexNode || {IndexNode,_Type} <- Preflist],
    lager:info("Preflist: ~w",[Preflist2]),
    riak_core_vnode_master:command(Preflist2,
                                   {declare, Id},
                                   {fsm, undefined, self()},
                                   ?VNODE_MASTER).

fetch(Id, FromId, FromP) ->
    [{IndexNode, _Type}] = generate_preference_list(?N, Id),
    riak_core_vnode_master:command(IndexNode,
                                   {fetch, Id, FromId, FromP},
                                   ?VNODE_MASTER).

reply_fetch(Id, FromP, DV) ->
    [{IndexNode, _Type}] = generate_preference_list(?N, Id),
    riak_core_vnode_master:command(IndexNode,
                                   {reply_fetch, Id, FromP, DV},
                                   ?VNODE_MASTER).

notify_value(Id, Value) ->
    [{IndexNode, _Type}] = generate_preference_list(?N, Id),
    riak_core_vnode_master:command(IndexNode,
                                   {notify_value, Id, Value},
                                   ?VNODE_MASTER).

get_new_id() ->
    [{IndexNode, _Type}] = generate_preference_list(?N, now()),
    riak_core_vnode_master:sync_spawn_command(IndexNode,
                                              get_new_id,
                                              ?VNODE_MASTER).

wait_needed(Id) ->
    [{IndexNode, _Type}] = generate_preference_list(?N, Id),
    riak_core_vnode_master:sync_spawn_command(IndexNode,
                                              {wait_needed, Id},
                                              ?VNODE_MASTER).

%% @doc Generate a preference list for a given N value and data item.
generate_preference_list(NVal, Param) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Param)}),
    riak_core_apl:get_primary_apl(DocIdx, NVal, derflow).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    Table = string:concat(integer_to_list(Partition), "dvstore"),
    TableAtom = list_to_atom(Table),
    TableAtom = ets:new(TableAtom, [set, named_table, public, {write_concurrency, true}]),
    {ok, #state {partition=Partition, table=TableAtom}}.

handle_command({declare, Id}, _From, State=#state{table=Table}) ->
    true = ets:insert(Table, {Id, #dv{value=undefined}}),
    {reply, {ok, Id}, State};

handle_command({bind, Id, Fun, Arg}, _From,
               State=#state{table=Table}) ->
    [{_Key, V}] = ets:lookup(Table, Id),
    NextKey = next_key(V#dv.next),
    execute_and_put(Fun, Arg, NextKey, Id, Table),
    {reply, {ok, NextKey}, State};

handle_command({bind, Id, Value}, From,
               State=#state{table=Table}) ->
    case Value of
        {id, DVId} ->
            true = ets:insert(Table, {Id, #dv{value={id,DVId}}}),
            fetch(DVId, Id, From),
            {noreply, State};
        _ ->
            [{_Key,V}] = ets:lookup(Table, Id),
            NextKey = next_key(V#dv.next),
            put(Value, NextKey, Id, Table),
            {reply, {ok, NextKey}, State}
        end;

handle_command({fetch, TargetId, FromId, FromP}, _From,
               State=#state{table=Table}) ->
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
                    NextKey = next_key(DV#dv.next),
                    BindingList = lists:append(DV#dv.binding_list, [FromId]),
                    DV1 = DV#dv{binding_list=BindingList, next=NextKey},
                    true = ets:insert(Table, {TargetId, DV1}),
                    reply_fetch(FromId, FromP, DV1),
                    {noreply, State}
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
               State=#state{table=Table}) ->
    [{_Key, V}] = ets:lookup(Table, X),
    Value = V#dv.value,
    Bounded = V#dv.bounded,
    Creator = V#dv.creator,
    Lazy = V#dv.lazy,
    if
        Bounded == true ->
            {reply, {Value, V#dv.next}, State};
        true ->
            {ok, NextKey} = declare_next(),
            V1 = V#dv{next=NextKey},
            true = ets:insert(Table, {X, V1}),
            if
                Lazy == true ->
                    reply_to_all([Creator], ok),
                    {reply, NextKey, State};
                true ->
                    {reply, NextKey, State}
            end
    end;

handle_command({next, X}, _From,
               State = #state{table = Table}) ->
    [{_Key,V}] = ets:lookup(Table, X),
    NextKey0 = V#dv.next,
    if
        NextKey0 == undefined ->
            {ok, NextKey} = declare_next(),
            V1 = V#dv{next=NextKey},
            true = ets:insert(Table, {X, V1}),
            {reply, {ok, NextKey}, State};
        true ->
            {reply, {ok, NextKey0}, State}
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

reply_to_all([], _Result) ->
    ok;

reply_to_all([H|T], Result) ->
    {server, undefined,{Address, Ref}} = H,
    gen_server:reply({Address, Ref}, Result),
    reply_to_all(T, Result).

next_key(NextKey0) ->
    if
        NextKey0 == undefined ->
            {ok, NextKey} = declare_next();
        true ->
            NextKey = NextKey0
    end,
    NextKey.

notify_all(L, Value) ->
    case L of
        [H|T] ->
            notify_value(H, Value),
            notify_all(T, Value);
        [] ->
            ok
    end.

declare_next()->
    _ = derflow_declare_fsm_sup:start_child([self()]),
        receive
            {ok, Id} ->
                {ok, Id}
        end.
