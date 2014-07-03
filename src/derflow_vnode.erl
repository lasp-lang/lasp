-module(derflow_vnode).
-behaviour(riak_core_vnode).
-include("derflow.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-define(VNODE_MASTER, derflow_vnode_master).

-export([bind/2,
         read/1,
         read/2,
         next/1,
         is_det/1,
         wait_needed/1,
         declare/2,
         write/5,
         thread/3]).

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

-record(state, {node, partition, variables}).

-record(dv, {value,
             next,
             waiting_threads = [],
             binding_list = [],
             functions = [],
             creator,
             type,
             lazy = false,
             bound = false}).

%% Extrenal API

bind(Id, Value) ->
    lager:info("Bind called by process ~p, value ~p, id: ~p",
               [self(), Value, Id]),
    [{IndexNode, _Type}] = derflow:preflist(?N, Id, derflow),
    riak_core_vnode_master:sync_spawn_command(IndexNode,
                                              {bind, Id, Value},
                                              ?VNODE_MASTER).

read(Id) ->
    read(Id, undefined).

read(Id, Threshold) ->
    Function = get(initial_call),
    lager:info("Read by process ~p, function ~p, id: ~p thresh: ~p",
               [self(), Function, Id, Threshold]),
    [{IndexNode, _Type}] = derflow:preflist(?N, Id, derflow),
    riak_core_vnode_master:sync_spawn_command(IndexNode,
                                              {read, Id, Threshold, Function},
                                              ?VNODE_MASTER).

thread(Module, Function, Args) ->
    [{IndexNode, _Type}] = derflow:preflist(?N,
                                            {Module, Function, Args},
                                            derflow),
    riak_core_vnode_master:sync_spawn_command(IndexNode,
                                              {thread, Module, Function, Args},
                                              ?VNODE_MASTER).

next(Id) ->
    [{IndexNode, _Type}] = derflow:preflist(?N, Id, derflow),
    riak_core_vnode_master:sync_spawn_command(IndexNode,
                                              {next, Id},
                                              ?VNODE_MASTER).

is_det(Id) ->
    [{IndexNode, _Type}] = derflow:preflist(?N, Id, derflow),
    riak_core_vnode_master:sync_spawn_command(IndexNode,
                                              {is_det, Id},
                                              ?VNODE_MASTER).

declare(Id, Type) ->
    [{IndexNode, _Type}] = derflow:preflist(?N, Id, derflow),
    riak_core_vnode_master:sync_spawn_command(IndexNode,
                                              {declare, Id, Type},
                                              ?VNODE_MASTER).

fetch(Id, FromId, FromP) ->
    [{IndexNode, _Type}] = derflow:preflist(?N, Id, derflow),
    riak_core_vnode_master:command(IndexNode,
                                   {fetch, Id, FromId, FromP},
                                   ?VNODE_MASTER).

reply_fetch(Id, FromP, DV) ->
    [{IndexNode, _Type}] = derflow:preflist(?N, Id, derflow),
    riak_core_vnode_master:command(IndexNode,
                                   {reply_fetch, Id, FromP, DV},
                                   ?VNODE_MASTER).

notify_value(Id, Value) ->
    [{IndexNode, _Type}] = derflow:preflist(?N, Id, derflow),
    riak_core_vnode_master:command(IndexNode,
                                   {notify_value, Id, Value},
                                   ?VNODE_MASTER).

wait_needed(Id) ->
    [{IndexNode, _Type}] = derflow:preflist(?N, Id, derflow),
    riak_core_vnode_master:sync_spawn_command(IndexNode,
                                              {wait_needed, Id},
                                              ?VNODE_MASTER).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    Variables = string:concat(integer_to_list(Partition), "dvstore"),
    VariableAtom = list_to_atom(Variables),
    VariableAtom = ets:new(VariableAtom, [set, named_table, public,
                                          {write_concurrency, true}]),
    {ok, #state{partition=Partition, node=node(), variables=VariableAtom}}.

handle_command({declare, Id, Type}, _From, State) ->
    {ok, Id} = internal_declare(Id, Type, State),
    {reply, {ok, Id}, State};

handle_command({bind, Id, {id, DVId}}, From,
               State=#state{variables=Variables}) ->
    true = ets:insert(Variables, {Id, #dv{value={id, DVId}}}),
    fetch(DVId, Id, From),
    {noreply, State};
handle_command({bind, Id, Value}, _From,
               State=#state{variables=Variables}) ->
    lager:info("Bind received: ~p", [Id]),
    [{_Key, V=#dv{functions=Functions}}] = ets:lookup(Variables, Id),
    NextKey = case Value of
        nil ->
            undefined;
        _ ->
            next_key(V#dv.next, V#dv.type, State)
    end,
    case V#dv.bound of
        true ->
            case V#dv.value of
                Value ->
                    {reply, {ok, NextKey}, State};
                _ ->
                    case is_lattice(V#dv.type) of
                        true ->
                            write(V#dv.type, Value, NextKey, [], Id, Variables),
                            case is_inflation(V#dv.type, V#dv.value, Value) of
                                true ->
                                    lager:info("Change is inflation: ~p ~p",
                                               [V#dv.value, Value]),
                                    execute(Functions, State),
                                    {reply, {ok, NextKey}, State};
                                false ->
                                    lager:info("Change is not inflation!"),
                                    {reply, {ok, NextKey}, State}
                            end;
                        false ->
                            lager:warning("Attempt to bind failed: ~p ~p ~p",
                                          [V#dv.type, V#dv.value, Value]),
                            {reply, error, State}
                    end
            end;
        false ->
            write(V#dv.type, Value, NextKey, Id, Variables),
            {reply, {ok, NextKey}, State}
    end;

handle_command({fetch, TargetId, FromId, FromP}, _From,
               State=#state{variables=Variables}) ->
    [{_, DV}] = ets:lookup(Variables, TargetId),
    case DV#dv.bound of
        true ->
            reply_fetch(FromId, FromP, DV),
            {noreply, State};
        false ->
            case DV#dv.value of
                {id, BindId} ->
                    fetch(BindId, FromId, FromP),
                    {noreply, State};
                _ ->
                    NextKey = next_key(DV#dv.next, DV#dv.type, State),
                    BindingList = lists:append(DV#dv.binding_list, [FromId]),
                    DV1 = DV#dv{binding_list=BindingList, next=NextKey},
                    true = ets:insert(Variables, {TargetId, DV1}),
                    reply_fetch(FromId, FromP, DV1),
                    {noreply, State}
                end
    end;

handle_command({reply_fetch, FromId, FromP,
                FetchDV=#dv{value=Value, next=Next, type=Type}}, _From, 
               State=#state{variables=Variables}) ->
    case FetchDV#dv.bound of
        true ->
            write(Type, Value, Next, FromId, Variables),
            {ok, _} = reply_to_all([FromP], {ok, Next}),
            ok;
        false ->
            [{_, DV}] = ets:lookup(Variables, FromId),
            DV1 = DV#dv{next=FetchDV#dv.next},
            true = ets:insert(Variables, {FromId, DV1}),
            {ok, _} = reply_to_all([FromP], {ok, FetchDV#dv.next}),
            ok
      end,
      {noreply, State};

handle_command({notify_value, Id, Value}, _From,
               State=#state{variables=Variables}) ->
    [{_, #dv{next=Next, type=Type}}] = ets:lookup(Variables, Id),
    write(Type, Value, Next, Id, Variables),
    {noreply, State};

handle_command({thread, Module, Function, Args}, _From, State) ->
    {ok, Pid} = internal_thread(Module, Function, Args),
    {reply, {ok, Pid}, State};

handle_command({wait_needed, Id}, From,
               State=#state{variables=Variables}) ->
    [{_Key, V}] = ets:lookup(Variables, Id),
    case V#dv.bound of
        true ->
            {reply, ok, State};
        false ->
            case V#dv.waiting_threads of
                [_H|_T] ->
                    {reply, ok, State};
                _ ->
                    true = ets:insert(Variables,
                                      {Id, V#dv{lazy=true, creator=From}}),
                    {noreply, State}
                end
    end;

handle_command({read, Id, Threshold, Function}, From,
               State=#state{variables=Variables}) ->
    [{_Key, V=#dv{value=Value,
                  bound=Bound,
                  creator=Creator,
                  lazy=Lazy,
                  type=Type,
                  next=NextKey,
                  functions=Functions0}}] = ets:lookup(Variables, Id),
    case Bound of
        true ->
            lager:info("Read received: ~p, bound: ~p, threshold: ~p",
                       [Id, V, Threshold]),
            case is_lattice(Type) of
                true ->
                    %% Handle recalling the dependent functions.
                    case Function of
                        undefined ->
                            ok;
                        _ ->
                            Functions = [Function|Functions0],
                            lager:info("Read depends on function: ~p",
                                       [Functions]),
                            write(Type, Value, NextKey, Functions, Id, Variables)
                    end,

                    %% Handle threshold reaads.
                    case Threshold of
                        undefined ->
                            lager:info("No threshold specified: ~p",
                                       [Threshold]),
                            {reply, {ok, Value, V#dv.next}, State};
                        _ ->
                            lager:info("Threshold specified: ~p",
                                       [Threshold]),
                            case threshold_met(Type, Value, Threshold) of
                                true ->
                                    {reply, {ok, Value, V#dv.next}, State};
                                false ->
                                    WT = lists:append(V#dv.waiting_threads,
                                                      [{From, Type, Threshold}]),
                                    true = ets:insert(Variables,
                                                      {Id, V#dv{waiting_threads=WT}}),
                                    {noreply, State}
                            end
                    end;
                false ->
                    {reply, {ok, Value, V#dv.next}, State}
            end;
        false ->
            lager:info("Read received: ~p, unbound, function: ~p",
                       [Id, Function]),
            WT = lists:append(V#dv.waiting_threads, [From]),
            true = ets:insert(Variables, {Id, V#dv{waiting_threads=WT}}),
            case Lazy of
                true ->
                    {ok, _} = reply_to_all([Creator], ok),
                    {noreply, State};
                false ->
                    {noreply, State}
            end
    end;

handle_command({next, Id}, _From,
               State=#state{variables=Variables}) ->
    [{_Key, V=#dv{next=NextKey0}}] = ets:lookup(Variables, Id),
    case NextKey0 of
        undefined ->
            {ok, NextKey} = declare_next(V#dv.type, State),
            true = ets:insert(Variables, {Id, V#dv{next=NextKey}}),
            {reply, {ok, NextKey}, State};
        _ ->
            {reply, {ok, NextKey0}, State}
  end;

handle_command({is_det, Id}, _From, State=#state{variables=Variables}) ->
    [{_Key, #dv{bound=Bound}}] = ets:lookup(Variables, Id),
    {reply, Bound, State};

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=FoldFun, acc0=Acc0}, _Sender,
                       #state{variables=Variables}=State) ->
    F = fun({Key, Operation}, Acc) -> FoldFun(Key, Operation, Acc) end,
    Acc = ets:foldl(F, Acc0, Variables),
    {reply, Acc, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, State=#state{variables=Variables}) ->
    {Key, Operation} = binary_to_term(Data),
    true = ets:insert_new(Variables, {Key, Operation}),
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

write(Type, Value, Next, Key, Variables) ->
    write(Type, Value, Next, [], Key, Variables).

write(Type, Value, Next, Functions0, Key, Variables) ->
    lager:info("Writing key: ~p next: ~p", [Key, Next]),
    [{_Key, #dv{waiting_threads=Threads,
                binding_list=BindingList,
                lazy=Lazy}}] = ets:lookup(Variables, Key),
    Functions = lists:usort(Functions0),
    {ok, StillWaiting} = reply_to_all(Threads, [], {ok, Value, Next}),
    V1 = #dv{type=Type, value=Value, functions=Functions, next=Next,
             lazy=Lazy, bound=true, waiting_threads=StillWaiting},
    true = ets:insert(Variables, {Key, V1}),
    notify_all(BindingList, Value).

reply_to_all(List, Result) ->
    reply_to_all(List, [], Result).

reply_to_all([{From, Type, Threshold}=H|T],
             StillWaiting0,
             {ok, Value, _Next}=Result) ->
    lager:info("Result: ~p, Threshold: ~p", [Result, Threshold]),
    StillWaiting = case threshold_met(Type, Value, Threshold) of
        true ->
            lager:info("Threshold ~p met: ~p", [Threshold, Value]),
            {server, undefined, {Address, Ref}} = From,
            gen_server:reply({Address, Ref}, Result),
            StillWaiting0;
        false ->
            lager:info("Threshold ~p NOT met: ~p", [Threshold, Value]),
            StillWaiting0 ++ [H]
    end,
    reply_to_all(T, StillWaiting, Result);
reply_to_all([H|T], StillWaiting, Result) ->
    {server, undefined, {Address, Ref}} = H,
    gen_server:reply({Address, Ref}, Result),
    reply_to_all(T, StillWaiting, Result);
reply_to_all([], StillWaiting, _Result) ->
    {ok, StillWaiting}.

next_key(undefined, Type, State) ->
    {ok, NextKey} = declare_next(Type, State),
    NextKey;
next_key(NextKey0, _, _) ->
    NextKey0.

notify_all([H|T], Value) ->
    notify_value(H, Value),
    notify_all(T, Value);
notify_all([], _) ->
    ok.

%% @doc Declare the next object for streams.
declare_next(Type, State=#state{partition=Partition, node=Node}) ->
    lager:info("Current partition and node: ", [Partition, Node]),
    Id = druuid:v4(),
    [{IndexNode, _Type}] = derflow:preflist(?N, Id, derflow),
    case IndexNode of
        {Partition, Node} ->
            lager:info("Internal declare triggered: ~p", [IndexNode]),
            internal_declare(Id, Type, State);
        _ ->
            lager:info("Declare triggered: ~p", [IndexNode]),
            declare(Id, Type)
    end.

%% @doc Determine if a threshold is met.
threshold_met(_, Value, {greater, Threshold}) ->
    Threshold < Value;
threshold_met(_, Value, Threshold) ->
    Threshold =< Value.

%% @doc Determine if `NewValue` is an inflation of `Value`.
is_inflation(riak_dt_gcounter, Value, NewValue) ->
    Value < NewValue;
is_inflation(riak_dt_gset, Value, NewValue) ->
    OldSet = riak_dt_gset:value(Value),
    NewSet = riak_dt_gset:value(NewValue),
    length(OldSet) < length(NewSet);
is_inflation(_, _Value, _NewValue) ->
    false.

%% @doc Return if something is a lattice or not.
is_lattice(Type) ->
    lists:member(Type, [riak_dt_gcounter,
                        riak_dt_lwwreg,
                        riak_dt_gset]).

%% @doc Execute a series of functions.
execute({Module, Function, Args},
        #state{partition=Partition, node=Node}) ->
    lager:info("Re-executing: ~p ~p ~p", [Module, Function, Args]),
    [{IndexNode, _Type}] = derflow:preflist(?N,
                                            {Module, Function, Args},
                                            derflow),
    case IndexNode of
        {Partition, Node} ->
            lager:info("Internal thread triggered: ~p", [IndexNode]),
            internal_thread(Module, Function, Args);
        _ ->
            lager:info("Thread triggered: ~p", [IndexNode]),
            thread(Module, Function, Args)
    end;
execute(Functions, State) ->
    [execute(Function, State) || Function <- Functions].

%% @doc Declare a new variable.
internal_declare(Id, Type, #state{variables=Variables}) ->
    lager:info("Declare received: ~p ~p", [Id, Type]),
    Record = case Type of
        undefined ->
            #dv{value=undefined, type=undefined, bound=false};
        Type ->
            #dv{value=Type:new(), type=Type, bound=true}
    end,
    true = ets:insert(Variables, {Id, Record}),
    {ok, Id}.

%% @doc Perform a thread operation locally.
internal_thread(Module, Function, Args) ->
    Fun = fun() ->
            put(initial_call, {Module, Function, Args}),
            erlang:apply(Module, Function, Args)
    end,
    Pid = spawn(Fun),
    lager:info("Spawned process ~p executing ~p",
               [Pid, {Module, Function, Args}]),
    {ok, Pid}.
