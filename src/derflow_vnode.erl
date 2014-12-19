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

%% @doc derpflow operational vnode, which powers the data flow variable
%%      assignment and read operations.
%%

-module(derpflow_vnode).

-behaviour(riak_core_vnode).

-include("derpflow.hrl").

-include_lib("riak_core/include/riak_core_vnode.hrl").

-define(VNODE_MASTER, derpflow_vnode_master).

%% Language execution primitives.
-export([bind/2,
         bind_to/2,
         read/1,
         read/2,
         select/3,
         next/1,
         is_det/1,
         wait_needed/1,
         wait_needed/2,
         declare/2,
         thread/3]).

%% Program execution functions.
-export([register/4,
         execute/3]).

%% Callbacks from the backend module.
-export([notify_value/2,
         fetch/3,
         next_key/3,
         reply_fetch/3]).

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
    [{IndexNode, _Type}] = derpflow:preflist(?N, Id, derpflow),
    riak_core_vnode_master:sync_spawn_command(IndexNode,
                                              {bind, Id, Value},
                                              ?VNODE_MASTER).

bind_to(Id, TheirId) ->
    lager:info("Bind called by process ~p, their_id ~p, id: ~p",
               [self(), TheirId, Id]),
    [{IndexNode, _Type}] = derpflow:preflist(?N, Id, derpflow),
    riak_core_vnode_master:sync_spawn_command(IndexNode,
                                              {bind_to, Id, TheirId},
                                              ?VNODE_MASTER).

read(Id) ->
    read(Id, undefined).

read(Id, Threshold) ->
    lager:info("Read by process ~p, id: ~p thresh: ~p",
               [self(), Id, Threshold]),
    [{IndexNode, _Type}] = derpflow:preflist(?N, Id, derpflow),
    riak_core_vnode_master:sync_spawn_command(IndexNode,
                                              {read, Id, Threshold},
                                              ?VNODE_MASTER).

select(Id, Function, AccId) ->
    lager:info("Select by process ~p, id: ~p accid: ~p",
               [self(), Id, AccId]),
    [{IndexNode, _Type}] = derpflow:preflist(?N, Id, derpflow),
    riak_core_vnode_master:sync_spawn_command(IndexNode,
                                              {select,
                                               Id, Function, AccId},
                                              ?VNODE_MASTER).

thread(Module, Function, Args) ->
    [{IndexNode, _Type}] = derpflow:preflist(?N,
                                            {Module, Function, Args},
                                            derpflow),
    riak_core_vnode_master:sync_spawn_command(IndexNode,
                                              {thread,
                                               Module, Function, Args},
                                              ?VNODE_MASTER).

next(Id) ->
    [{IndexNode, _Type}] = derpflow:preflist(?N, Id, derpflow),
    riak_core_vnode_master:sync_spawn_command(IndexNode,
                                              {next, Id},
                                              ?VNODE_MASTER).

is_det(Id) ->
    [{IndexNode, _Type}] = derpflow:preflist(?N, Id, derpflow),
    riak_core_vnode_master:sync_spawn_command(IndexNode,
                                              {is_det, Id},
                                              ?VNODE_MASTER).

declare(Id, Type) ->
    [{IndexNode, _Type}] = derpflow:preflist(?N, Id, derpflow),
    riak_core_vnode_master:sync_spawn_command(IndexNode,
                                              {declare, Id, Type},
                                              ?VNODE_MASTER).

fetch(Id, FromId, FromPid) ->
    [{IndexNode, _Type}] = derpflow:preflist(?N, Id, derpflow),
    riak_core_vnode_master:command(IndexNode,
                                   {fetch, Id, FromId, FromPid},
                                   ?VNODE_MASTER).

reply_fetch(Id, FromPid, DV) ->
    [{IndexNode, _Type}] = derpflow:preflist(?N, Id, derpflow),
    riak_core_vnode_master:command(IndexNode,
                                   {reply_fetch, Id, FromPid, DV},
                                   ?VNODE_MASTER).

notify_value(Id, Value) ->
    [{IndexNode, _Type}] = derpflow:preflist(?N, Id, derpflow),
    riak_core_vnode_master:command(IndexNode,
                                   {notify_value, Id, Value},
                                   ?VNODE_MASTER).

wait_needed(Id) ->
    wait_needed(Id, undefined).

wait_needed(Id, Threshold) ->
    [{IndexNode, _Type}] = derpflow:preflist(?N, Id, derpflow),
    riak_core_vnode_master:sync_spawn_command(IndexNode,
                                              {wait_needed, Id, Threshold},
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
                                 {parse_transform, derpflow_transform},
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
    {ok, Id} = ?BACKEND:declare(Id, Type, Variables),
    {reply, {ok, Id}, State};

handle_command({bind_to, Id, DVId}, FromPid,
               State=#state{variables=Variables}) ->
    FetchFun = fun(_TargetId, _FromId, _FromPid) ->
            ?MODULE:fetch(_TargetId, _FromId, _FromPid)
    end,
    ?BACKEND:bind_to(Id, DVId, Variables, FetchFun, FromPid),
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
    NotifyFun = fun(_Id, NewValue) ->
                        ?MODULE:notify_value(_Id, NewValue)
                end,
    Result = ?BACKEND:bind(Id, Value, Variables, NextKeyFun, NotifyFun),
    {reply, Result, State};

handle_command({fetch, TargetId, FromId, FromPid}, _From,
               State=#state{variables=Variables}) ->
    ResponseFun = fun() ->
            {noreply, State}
    end,
    FetchFun = fun(_TargetId, _FromId, _FromPid) ->
            ?MODULE:fetch(_TargetId, _FromId, _FromPid)
    end,
    ReplyFetchFun = fun(_FromId, _FromPid, DV) ->
            ?MODULE:reply_fetch(_FromId, _FromPid, DV)
    end,
    NextKeyFun = fun(Next, Type) ->
            ?MODULE:next_key(Next, Type, State)
    end,
    ?BACKEND:fetch(TargetId,
                   FromId,
                   FromPid,
                   Variables,
                   ResponseFun,
                   FetchFun,
                   ReplyFetchFun,
                   NextKeyFun);

handle_command({reply_fetch, FromId, FromPid,
                #dv{value=Value, next=Next, type=Type}}, _From,
               State=#state{variables=Variables}) ->
    NotifyFun = fun(Id, NewValue) ->
                        ?MODULE:notify_value(Id, NewValue)
                end,
    ?BACKEND:write(Type, Value, Next, FromId, Variables, NotifyFun),
    {ok, _} = ?BACKEND:reply_to_all([FromPid], {ok, Next}),
    {noreply, State};

handle_command({notify_value, Id, Value}, _From,
               State=#state{variables=Variables}) ->
    NotifyFun = fun(_Id, NewValue) ->
                        ?MODULE:notify_value(_Id, NewValue)
                end,
    ?BACKEND:notify_value(Id, Value, Variables, NotifyFun),
    {noreply, State};

handle_command({thread, Module, Function, Args}, _From,
               #state{variables=Variables}=State) ->
    {ok, Pid} = ?BACKEND:thread(Module, Function, Args, Variables),
    {reply, {ok, Pid}, State};

handle_command({wait_needed, Id, Threshold}, From,
               State=#state{variables=Variables}) ->
    Self = From,
    ReplyFun = fun(ReadThreshold) ->
                       {reply, {ok, ReadThreshold}, State}
               end,
    BlockingFun = fun() ->
                        {noreply, State}
                  end,
    ?BACKEND:wait_needed(Id, Threshold, Variables, Self, ReplyFun, BlockingFun);

handle_command({read, Id, Threshold}, From,
               State=#state{variables=Variables}) ->
    Self = From,
    ReplyFun = fun(Type, Value, Next) ->
                       {reply, {ok, Type, Value, Next}, State}
               end,
    BlockingFun = fun() ->
                        {noreply, State}
                  end,
    ?BACKEND:read(Id,
                  Threshold,
                  Variables,
                  Self,
                  ReplyFun,
                  BlockingFun);

handle_command({select, Id, Function, AccId}, _From,
               State=#state{variables=Variables,
                            partition=Partition,
                            node=Node}) ->
    BindFun = fun(_AccId, AccValue, _Variables) ->
            %% Beware of cycles in the gen_server calls!
            [{IndexNode, _Type}] = derpflow:preflist(?N, _AccId, derpflow),

            case IndexNode of
                {Partition, Node} ->
                    %% We're local, which means that we can interact
                    %% directly with the data store.
                    ?BACKEND:bind(_AccId, AccValue, _Variables);
                _ ->
                    %% We're remote, go through all of the routing logic.
                    ?MODULE:bind(_AccId, AccValue)
            end
    end,
    Result = ?BACKEND:select(Id, Function, AccId, Variables, BindFun),
    {reply, Result, State};

handle_command({next, Id}, _From,
               State=#state{variables=Variables}) ->
    DeclareNextFun = fun(Type) ->
                            declare_next(Type, State)
                     end,
    Result = ?BACKEND:next(Id, Variables, DeclareNextFun),
    {reply, Result, State};

handle_command({is_det, Id}, _From, State=#state{variables=Variables}) ->
    Result = ?BACKEND:is_det(Id, Variables),
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

%% Internal language functions.

next_key(undefined, Type, State) ->
    {ok, NextKey} = declare_next(Type, State),
    NextKey;
next_key(NextKey0, _, _) ->
    NextKey0.

%% @doc Declare the next object for streams.
declare_next(Type, #state{partition=Partition, node=Node, variables=Variables}) ->
    lager:info("Current partition and node: ~p ~p", [Partition, Node]),
    Id = druuid:v4(),

    %% Beware of cycles in the gen_server calls!
    [{IndexNode, _Type}] = derpflow:preflist(?N, Id, derpflow),

    case IndexNode of
        {Partition, Node} ->
            %% We're local, which means that we can interact directly
            %% with the data store.
            lager:info("Local declare triggered: ~p", [IndexNode]),
            ?BACKEND:declare(Id, Type, Variables);
        _ ->
            %% We're remote, go through all of the routing logic.
            lager:info("Declare triggered: ~p", [IndexNode]),
            ?MODULE:declare(Id, Type)
    end.

%% Internal program execution functions.

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
