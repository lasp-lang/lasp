-module(derflow_vnode).
-behaviour(riak_core_vnode).
-include("derflow.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-define(VNODE_MASTER, derflow_vnode_master).
-define(N, 3).

-export([bind/2,
         read/1,
         touch/1,
         next/1,
         is_det/1,
         wait_needed/1,
         declare/3,
         put/4]).

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
             next,
             waiting_threads = [],
             binding_list = [],
             type,
             creator,
             lazy = false,
             bound = false}).

%% Extrenal API

bind(Id, Value) ->
    Preflist2 = generate_preflist2(?N, Id),
    riak_core_vnode_master:command(Preflist2,
                                   {bind, Id, Value},
                                   {fsm, undefined, self()},
                                   ?VNODE_MASTER).

read(Id) ->
    Preflist2 = generate_preflist2(?N, Id),
    riak_core_vnode_master:command(Preflist2,
                                   {read, Id},
                                   {fsm, undefined, self()},
                                   ?VNODE_MASTER).

touch(Id) ->
    Preflist2 = generate_preflist2(?N, Id),
    riak_core_vnode_master:command(Preflist2,
                                   {touch, Id},
                                   {fsm, undefined, self()},
                                   ?VNODE_MASTER).

next(Id) ->
    Preflist2 = generate_preflist2(?N, Id),
    riak_core_vnode_master:command(Preflist2,
                                   {next, Id},
                                   {fsm, undefined, self()},
                                   ?VNODE_MASTER).

is_det(Id) ->
    Preflist2 = generate_preflist2(?N, Id),
    riak_core_vnode_master:command(Preflist2,
                                   {is_det, Id},
                                   {fsm, undefined, self()},
                                   ?VNODE_MASTER).

declare(Id, NextId, Type) ->
    Preflist2 = generate_preflist2(?N, Id),
    riak_core_vnode_master:command(Preflist2,
                                   {declare, Id, NextId, Type},
                                   {fsm, undefined, self()},
                                   ?VNODE_MASTER).

fetch(Id, FromId, FromP) ->
    Preflist2 = generate_preflist2(?N, Id),
    riak_core_vnode_master:command(Preflist2,
                                   {fetch, Id, FromId, FromP},
                                   {fsm, undefined, self()},
                                   ?VNODE_MASTER).

reply_fetch(Id, FromP, DV) ->
    Preflist2 = generate_preflist2(?N, Id),
    riak_core_vnode_master:command(Preflist2,
                                   {reply_fetch, Id, FromP, DV},
                                   {fsm, undefined, self()},
                                   ?VNODE_MASTER).

notify_value(Id, Value) ->
    Preflist2 = generate_preflist2(?N, Id),
    riak_core_vnode_master:command(Preflist2,
                                   {notify_value, Id, Value},
                                   {fsm, undefined, self()},
                                   ?VNODE_MASTER).

wait_needed(Id) ->
    Preflist2 = generate_preflist2(?N, Id),
    riak_core_vnode_master:command(Preflist2,
                                   {wait_needed, Id},
                                   {fsm, undefined, self()},
                                   ?VNODE_MASTER).

%% @doc Generate a preference list for a given N value and data item.
generate_preflist2(NVal, Param) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Param)}),
    Preflist = riak_core_apl:get_primary_apl(DocIdx, NVal, derflow),
    [IndexNode || {IndexNode, _Type} <- Preflist].

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    Table = string:concat(integer_to_list(Partition), "dvstore"),
    TableAtom = list_to_atom(Table),
    TableAtom = ets:new(TableAtom, [set, named_table, public,
                                    {write_concurrency, true}]),
    {ok, #state {partition=Partition, table=TableAtom}}.

handle_command({declare, Id, NextId, Type}, _From, State=#state{table=Table}) ->
    Record = case Type of
        undefined ->
            %% Undefined represents a single assignment variable, which
            %% means that we start it off in the undefined state, as
            %% unbound.
            #dv{type=Type, value=undefined, next=NextId, bound = false};
        Type ->
            %% Given we have another type, for now, assume this is one
            %% of the riak_dt types.
            #dv{type=Type, value=Type:new(), next=NextId, bound = true}
    end,
    true = ets:insert(Table, {Id, Record}),
    {reply, {ok, Id}, State};

handle_command({bind, Id, Value}, From,
               State=#state{table=Table}) ->
    case Value of
        {id, DVId} ->
            true = ets:insert(Table, {Id, #dv{value={id, DVId}}}),
            fetch(DVId, Id, From),
            {noreply, State};
        _ ->
            [{_Key, V}] = ets:lookup(Table, Id),
            NextKey = next_key(V#dv.next),
            case V#dv.bound of
                true ->
                    ok;
                false ->
                    put(Value, NextKey, Id, Table),
                    ok
            end,
            {reply, {ok, NextKey}, State}
        end;

handle_command({fetch, TargetId, FromId, FromP}, _From,
               State=#state{table=Table}) ->
    [{_, DV}] = ets:lookup(Table, TargetId),
    if
        DV#dv.bound == true ->
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
        FetchDV#dv.bound == true ->
            Value = FetchDV#dv.value,
            Next = FetchDV#dv.next,
            put(Value, Next, FromId, Table),
            reply_to_all([FromP], {ok, Next});
        true ->
            [{_, DV}] = ets:lookup(Table, FromId),
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
        V#dv.bound == true ->
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
    Bound = V#dv.bound,
    Creator = V#dv.creator,
    Lazy = V#dv.lazy,
    if
        Bound == true ->
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
    Bound = V#dv.bound,
    Creator = V#dv.creator,
    Lazy = V#dv.lazy,
    if
        Bound == true ->
            {reply, {ok, Value, V#dv.next}, State};
        true ->
            {ok, NextKey} = declare_next(),
            V1 = V#dv{next=NextKey},
            true = ets:insert(Table, {X, V1}),
            if
                Lazy == true ->
                    reply_to_all([Creator], ok),
                    {reply, {ok, NextKey}, State};
                true ->
                    {reply, {ok, NextKey}, State}
            end
    end;

handle_command({next, X}, _From,
               State = #state{table = Table}) ->
    [{_Key, V}] = ets:lookup(Table, X),
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
    [{_Key, V}] = ets:lookup(Table, Id),
    Bound = V#dv.bound,
    {reply, {ok, Bound}, State};

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

put({Mod, Fun, Args}, Next, Key, Table) ->
    put(Mod:Fun(Args), Next, Key, Table);
put(Value, Next, Key, Table) ->
    [{_Key, V}] = ets:lookup(Table, Key),
    Threads = V#dv.waiting_threads,
    BindingList = V#dv.binding_list,
    Lazy = V#dv.lazy,
    V1 = #dv{value = Value, next = Next, lazy = Lazy, bound = true},
    true = ets:insert(Table, {Key, V1}),
    [{_Key, V1}] = ets:lookup(Table, Key),
    notify_all(BindingList, Value),
    reply_to_all(Threads, {ok, Value, Next}).

reply_to_all([], _Result) ->
    ok;

reply_to_all([H|T], Result) ->
    {server, undefined,{Address, Ref}} = H,
    gen_server:reply({Address, Ref}, Result),
    reply_to_all(T, Result).

next_key(undefined) ->
    druuid:v4();
next_key(Key) ->
    Key.

declare_next() ->
    druuid:v4().

notify_all(L, Value) ->
    case L of
        [H|T] ->
            notify_value(H, Value),
            notify_all(T, Value);
        [] ->
            ok
    end.
