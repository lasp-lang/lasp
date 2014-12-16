%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc Derflow operational vnode, which powers the data flow variable
%%      assignment and read operations.
%%
%% @TODO This probably has a ton of bugs, because it needs to be
%%       rewritten to use the derflow_ets backend, which is now
%%       quickchecked.
%%

-module(derflow_vnode).

-behaviour(riak_core_vnode).

-include("derflow.hrl").

-include_lib("riak_core/include/riak_core_vnode.hrl").

-define(VNODE_MASTER, derflow_vnode_master).

%% Language execution primitives.
-export([bind/2,
         read/1,
         read/2,
         select/3,
         next/1,
         is_det/1,
         wait_needed/1,
         declare/2,
         thread/3]).

%% Program execution functions.
-export([register/4,
         execute/3]).

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

-export([select_harness/7]).

-ignore_xref([start_vnode/1]).

-record(state, {node,
                partition,
                variables,
                programs}).

%% Extrenal API

register(Preflist, Identity, Module, File) ->
    lager:info("Register called for module: ~p and file: ~p",
               [Module, File]),
    riak_core_vnode_master:command(Preflist,
                                   {register, Identity, Module, File},
                                   {fsm, undefined, self()},
                                   ?VNODE_MASTER).

execute(Preflist, Identity, Module) ->
    lager:info("Execute called for module: ~p", [Module]),
    riak_core_vnode_master:command(Preflist,
                                   {execute, Identity, Module},
                                   {fsm, undefined, self()},
                                   ?VNODE_MASTER).

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
    lager:info("Read by process ~p, id: ~p thresh: ~p",
               [self(), Id, Threshold]),
    [{IndexNode, _Type}] = derflow:preflist(?N, Id, derflow),
    riak_core_vnode_master:sync_spawn_command(IndexNode,
                                              {read, Id, Threshold},
                                              ?VNODE_MASTER).

select(Id, Function, AccId) ->
    lager:info("Select by process ~p, id: ~p accid: ~p",
               [self(), Id, AccId]),
    [{IndexNode, _Type}] = derflow:preflist(?N, Id, derflow),
    riak_core_vnode_master:sync_spawn_command(IndexNode,
                                              {select,
                                               Id, Function, AccId},
                                              ?VNODE_MASTER).

thread(Module, Function, Args) ->
    [{IndexNode, _Type}] = derflow:preflist(?N,
                                            {Module, Function, Args},
                                            derflow),
    riak_core_vnode_master:sync_spawn_command(IndexNode,
                                              {thread,
                                               Module, Function, Args},
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
    Node = node(),
    Variables = generate_unique_partition_identifier(Partition, Node),
    Variables = ets:new(Variables, [set, named_table, public,
                                    {write_concurrency, true}]),
    {ok, #state{partition=Partition,
                programs=dict:new(),
                node=Node,
                variables=Variables}}.

%% Program execution handling.

handle_command({execute, {ReqId, _}, Module0}, _From,
               #state{programs=Programs,
                      node=Node,
                      partition=Partition}=State) ->
    Module = generate_unique_module_identifier(Partition,
                                               Node,
                                               Module0),
    case execute(Module, Programs) of
        {ok, Result} ->
            {reply, {ok, ReqId, Result}, State};
        {error, undefined} ->
            {reply, {error, ReqId}, State}
    end;

handle_command({register, {ReqId, _}, Module0, File}, _From,
               #state{partition=Partition,
                      node=Node,
                      variables=Variables,
                      programs=Programs0}=State) ->
    try
        lager:info("Partition registration for module: ~p, partition: ~p",
                   [Module0, Partition]),
        Module = generate_unique_module_identifier(Partition,
                                                   Node,
                                                   Module0),
        case compile:file(File, [binary,
                                 {parse_transform, lager_transform},
                                 {parse_transform, derflow_transform},
                                 {store, Variables},
                                 {partition, Partition},
                                 {module, Module},
                                 {node, Node}]) of
            {ok, _, Bin} ->
                lager:info("Compilation succeeded; partition: ~p",
                           [Partition]),
                case code:load_binary(Module, File, Bin) of
                    {module, Module} ->
                        lager:info("Binary loaded, module: ~p, partition: ~p",
                                   [Module, Partition]),
                        {ok, Value} = Module:init(),
                        lager:info("Module initialized: value: ~p",
                                   [Value]),
                        Programs = dict:store(Module, Value, Programs0),
                        lager:info("Initialized module at partition: ~p",
                                   [Partition]),
                        {reply, {ok, ReqId}, State#state{programs=Programs}};
                    Reason ->
                        lager:info("Binary not loaded, reason: ~p, partition: ~p",
                                   [Reason, Partition]),
                        {reply, {error, Reason}, State}
                end;
            Error ->
                lager:info("Compilation failed; error: ~p, partition: ~p",
                           [Error, Partition]),
                {reply, {error, Error}, State}
        end
    catch
        _:Exception ->
            lager:info("Exception: ~p, partition: ~p, module: ~p",
                       [Exception, Partition, Module0]),
            {reply, {error, Exception}, State}
    end;

%% Language handling.

handle_command({declare, Id, Type}, _From,
               #state{variables=Variables}=State) ->
    {ok, Id} = derflow_ets:declare(Id, Type, Variables),
    {reply, {ok, Id}, State};

%% @TODO: use derflow_ets
handle_command({bind, Id, {id, DVId}}, From,
               State=#state{variables=Variables}) ->
    true = ets:insert(Variables, {Id, #dv{value={id, DVId}}}),
    fetch(DVId, Id, From),
    {noreply, State};

handle_command({bind, Id, Value}, _From,
               State=#state{variables=Variables}) ->
    NextKeyFun = fun(Type, Next) ->
                        case Value of
                            undefined ->
                                undefined;
                            _ ->
                                next_key(Next, Type, State)
                        end
                 end,
    Result = derflow_ets:bind(Id, Value, Variables, NextKeyFun),
    {reply, Result, State};

%% @TODO: use derflow_ets
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

%% @TODO: use derflow_ets
handle_command({reply_fetch, FromId, FromP,
                FetchDV=#dv{value=Value, next=Next, type=Type}}, _From, 
               State=#state{variables=Variables}) ->
    case FetchDV#dv.bound of
        true ->
            write(Type, Value, Next, FromId, Variables),
            {ok, _} = derflow_ets:reply_to_all([FromP], {ok, Next}),
            ok;
        false ->
            [{_, DV}] = ets:lookup(Variables, FromId),
            DV1 = DV#dv{next=FetchDV#dv.next},
            true = ets:insert(Variables, {FromId, DV1}),
            {ok, _} = derflow_ets:reply_to_all([FromP], {ok, FetchDV#dv.next}),
            ok
      end,
      {noreply, State};

%% @TODO: use derflow_ets
handle_command({notify_value, Id, Value}, _From,
               State=#state{variables=Variables}) ->
    [{_, #dv{next=Next, type=Type}}] = ets:lookup(Variables, Id),
    write(Type, Value, Next, Id, Variables),
    {noreply, State};

handle_command({thread, Module, Function, Args}, _From,
               #state{variables=Variables}=State) ->
    {ok, Pid} = derflow_ets:thread(Module, Function, Args, Variables),
    {reply, {ok, Pid}, State};

%% @TODO: use derflow_ets
handle_command({wait_needed, Id}, From,
               State=#state{variables=Variables}) ->
    lager:info("Wait needed issued for identifier: ~p", [Id]),
    [{_Key, V=#dv{waiting_threads=WT, bound=Bound}}] = ets:lookup(Variables, Id),
    case Bound of
        true ->
            {reply, ok, State};
        false ->
            case WT of
                [_H|_T] ->
                    {reply, ok, State};
                _ ->
                    true = ets:insert(Variables,
                                      {Id, V#dv{lazy=true, creator=From}}),
                    {noreply, State}
                end
    end;

handle_command({read, Id, Threshold}, From,
               State=#state{variables=Variables}) ->
    Self = From,
    ReplyFun = fun(Type, Value, Next) ->
                       {reply, {ok, Type, Value, Next}, State}
               end,
    BlockingFun = fun() ->
                        {noreply, State}
                  end,
    derflow_ets:read(Id,
                     Threshold,
                     Variables,
                     Self,
                     ReplyFun,
                     BlockingFun);

%% @TODO: needs to be added to derflow_ets.
handle_command({select, Id, Function, AccId}, _From,
               State=#state{variables=Variables,
                            partition=Partition,
                            node=Node}) ->
    Pid = spawn_link(?MODULE, select_harness, [Node,
                                               Partition,
                                               Variables,
                                               Id,
                                               Function,
                                               AccId,
                                               undefined]),
    {reply, {ok, Pid}, State};

handle_command({next, Id}, _From,
               State=#state{variables=Variables}) ->
    DeclareNextFun = fun(Type) ->
                            declare_next(Type, State)
                     end,
    Result = derflow_ets:next(Id, Variables, DeclareNextFun),
    {reply, Result, State};

handle_command({is_det, Id}, _From, State=#state{variables=Variables}) ->
    Result = derflow_ets:is_det(Id, Variables),
    {reply, Result, State};

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

handle_coverage(?EXECUTE_REQUEST{module=Module0}, _KeySpaces, _Sender,
                #state{programs=Programs,
                       node=Node,
                       partition=Partition}=State) ->
    lager:info("Coverage execute request received for module: ~p",
               [Module0]),
    Module = generate_unique_module_identifier(Partition,
                                               Node,
                                               Module0),
    case execute(Module, Programs) of
        {ok, Result} ->
            {reply, {done, Result}, State};
        {error, undefined} ->
            {reply, {error, undefined}, State}
    end;
handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%% Internal functions

write(Type, Value, Next, Key, Variables) ->
    lager:info("Writing key: ~p next: ~p", [Key, Next]),
    [{_Key, #dv{waiting_threads=Threads,
                binding_list=BindingList,
                lazy=Lazy}}] = ets:lookup(Variables, Key),
    lager:info("Waiting threads are: ~p", [Threads]),
    {ok, StillWaiting} = derflow_ets:reply_to_all(Threads, [], {ok, Type, Value, Next}),
    V1 = #dv{type=Type, value=Value, next=Next,
             lazy=Lazy, bound=true, waiting_threads=StillWaiting},
    true = ets:insert(Variables, {Key, V1}),
    notify_all(BindingList, Value).

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
declare_next(Type, #state{partition=Partition, node=Node, variables=Variables}) ->
    lager:info("Current partition and node: ~p ~p", [Partition, Node]),
    Id = druuid:v4(),

    %% Beware of cycles in the gen_server calls!
    [{IndexNode, _Type}] = derflow:preflist(?N, Id, derflow),

    case IndexNode of
        {Partition, Node} ->
            %% We're local, which means that we can interact directly
            %% with the data store.
            lager:info("Local declare triggered: ~p", [IndexNode]),
            derflow_ets:declare(Id, Type, Variables);
        _ ->
            %% We're remote, go through all of the routing logic.
            lager:info("Declare triggered: ~p", [IndexNode]),
            declare(Id, Type)
    end.

%% @doc Execute a given program.
execute(Module, Programs) ->
    lager:info("Execute triggered for module: ~p", [Module]),
    case dict:is_key(Module, Programs) of
        true ->
            Acc = dict:fetch(Module, Programs),
            lager:info("Executing module: ~p", [Module]),
            Module:execute(Acc);
        false ->
            lager:info("Failed to execute module: ~p", [Module]),
            {error, undefined}
    end.

%% @doc Generate a unique partition identifier.
generate_unique_partition_identifier(Partition, Node) ->
    list_to_atom(
        integer_to_list(Partition) ++ "-" ++ atom_to_list(Node)).

%% @doc Generate a unique module identifier.
generate_unique_module_identifier(Partition, Node, Module) ->
    list_to_atom(
        integer_to_list(Partition) ++ "-" ++
            atom_to_list(Node) ++ "-" ++ atom_to_list(Module)).

%% @doc Harness for managing the select operation.
select_harness(Node, Partition, Variables, Id, Function, AccId, Previous) ->
    lager:info("Select executing!"),
    {ok, Type, Value, _} = derflow_ets:read(Id, {strict, Previous}, Variables),
    lager:info("Threshold was met!"),
    [{_Key, #dv{type=Type, value=Value}}] = ets:lookup(Variables, Id),

    %% Generate operations for given data type.
    {ok, Operations} = derflow_ets:generate_operations(Type, Value),
    lager:info("Operations generated: ~p", [Operations]),

    %% Build new data structure.
    AccValue = lists:foldl(
                 fun({add, Element} = Op, Acc) ->
                         case Function(Element) of
                             true ->
                                 {ok, NewAcc} = Type:update(Op, undefined, Acc),
                                 NewAcc;
                             _ ->
                                 Acc
                         end
                 end, Type:new(), Operations),

    %% Beware of cycles in the gen_server calls!
    [{IndexNode, _Type}] = derflow:preflist(?N, AccId, derflow),

    case IndexNode of
        {Partition, Node} ->
            %% We're local, which means that we can interact directly
            %% with the data store.
            derflow_ets:bind(AccId, AccValue, Variables);
        _ ->
            %% We're remote, go through all of the routing logic.
            bind(AccId, AccValue)
    end,
    select_harness(Node, Partition, Variables, Id, Function, AccId, Value).
