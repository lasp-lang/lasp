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

%% @doc Operational vnode, which powers the data flow variable
%%      assignment and read operations.
%%

-module(lasp_vnode).

-behaviour(riak_core_vnode).

-include("lasp.hrl").

-include_lib("riak_core/include/riak_core_vnode.hrl").

-define(VNODE_MASTER, lasp_vnode_master).

%% Language execution primitives.
-export([bind/4,
         bind_to/4,
         update/5,
         read/4,
         filter/5,
         map/5,
         product/5,
         fold/5,
         next/3,
         wait_needed/4,
         declare/4,
         thread/5]).

%% Program execution functions.
-export([register/4,
         execute/3,
         process/6]).

%% Callbacks from the backend module.
-export([notify_value/2,
         fetch/4,
         next_key/3,
         reply_fetch/4]).

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

%-export([handle_event/2]).

-ignore_xref([start_vnode/1]).

-record(state, {node,
                partition,
                variables,
                programs}).

%% Extrenal API

register(Preflist, Identity, Module, File) ->
    riak_core_vnode_master:command(Preflist,
                                   {register, Identity, Module, File},
                                   {fsm, undefined, self()},
                                   ?VNODE_MASTER).

execute(Preflist, Identity, Module) ->
    riak_core_vnode_master:command(Preflist,
                                   {execute, Identity, Module},
                                   {fsm, undefined, self()},
                                   ?VNODE_MASTER).

process(Preflist, Identity, Module, Object, Reason, Idx) ->
    riak_core_vnode_master:command(Preflist,
                                   {process, Identity, Module, Object, Reason, Idx},
                                   {fsm, undefined, self()},
                                   ?VNODE_MASTER).

bind(Preflist, Identity, Id, Value) ->
    riak_core_vnode_master:command(Preflist,
                                   {bind, Identity, Id, Value},
                                   {fsm, undefined, self()},
                                   ?VNODE_MASTER).

bind_to(Preflist, Identity, Id, TheirId) ->
    riak_core_vnode_master:command(Preflist,
                                   {bind_to, Identity, Id, TheirId},
                                   {fsm, undefined, self()},
                                   ?VNODE_MASTER).

update(Preflist, Identity, Id, Operation, Actor) ->
    riak_core_vnode_master:command(Preflist,
                                   {update, Identity, Id, Operation,
                                    Actor},
                                   {fsm, undefined, self()},
                                   ?VNODE_MASTER).

read(Preflist, Identity, Id, Threshold) ->
    riak_core_vnode_master:command(Preflist,
                                   {read, Identity, Id, Threshold},
                                   {fsm, undefined, self()},
                                   ?VNODE_MASTER).

filter(Preflist, Identity, Id, Function, AccId) ->
    riak_core_vnode_master:command(Preflist,
                                   {filter, Identity, Id, Function, AccId},
                                   {fsm, undefined, self()},
                                   ?VNODE_MASTER).

map(Preflist, Identity, Id, Function, AccId) ->
    riak_core_vnode_master:command(Preflist,
                                   {map, Identity, Id, Function, AccId},
                                   {fsm, undefined, self()},
                                   ?VNODE_MASTER).

product(Preflist, Identity, Left, Right, Product) ->
    riak_core_vnode_master:command(Preflist,
                                   {product, Identity, Left, Right, Product},
                                   {fsm, undefined, self()},
                                   ?VNODE_MASTER).

fold(Preflist, Identity, Id, Function, AccId) ->
    riak_core_vnode_master:command(Preflist,
                                   {fold, Identity, Id, Function, AccId},
                                   {fsm, undefined, self()},
                                   ?VNODE_MASTER).

thread(Preflist, Identity, Module, Function, Args) ->
    riak_core_vnode_master:command(Preflist,
                                   {thread, Identity, Module, Function, Args},
                                   {fsm, undefined, self()},
                                   ?VNODE_MASTER).

next(Preflist, Identity, Id) ->
    riak_core_vnode_master:command(Preflist,
                                   {next, Identity, Id},
                                   {fsm, undefined, self()},
                                   ?VNODE_MASTER).

declare(Preflist, Identity, Id, Type) ->
    riak_core_vnode_master:command(Preflist,
                                   {declare, Identity, Id, Type},
                                   {fsm, undefined, self()},
                                   ?VNODE_MASTER).

wait_needed(Preflist, Identity, Id, Threshold) ->
    riak_core_vnode_master:command(Preflist,
                                   {wait_needed, Identity, Id, Threshold},
                                   {fsm, undefined, self()},
                                   ?VNODE_MASTER).


fetch(Id, FromId, FromPid, ReqId) ->
    [{IndexNode, _Type}] = lasp:preflist(?N, Id, lasp),
    riak_core_vnode_master:command(IndexNode,
                                   {fetch, Id, FromId, FromPid, ReqId},
                                   ?VNODE_MASTER).

reply_fetch(Id, FromPid, ReqId, DV) ->
    [{IndexNode, _Type}] = lasp:preflist(?N, Id, lasp),
    riak_core_vnode_master:command(IndexNode,
                                   {reply_fetch, Id, FromPid, ReqId, DV},
                                   ?VNODE_MASTER).

notify_value(Id, Value) ->
    [{IndexNode, _Type}] = lasp:preflist(?N, Id, lasp),
    riak_core_vnode_master:command(IndexNode,
                                   {notify_value, Id, Value},
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

%% Backdoor
handle_command({get_dict, {ReqId, _}, _}, _From,
               state=State) ->
    lager:info("TEST1 : ~p", [State]),
    {reply, {ok, ReqId}, State};

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

handle_command({process, {ReqId, _}, Module0, Object, Reason, Idx}, _From,
               #state{programs=Programs0,
                      node=Node,
                      partition=Partition}=State) ->
    Module = generate_unique_module_identifier(Partition,
                                               Node,
                                               Module0),
    case process(Module, Object, Reason, Idx, Programs0) of
        {ok, Result, Programs} ->
            {reply, {ok, ReqId, Result}, State#state{programs=Programs}};
        {error, undefined} ->
            {reply, {error, ReqId}, State}
    end;

handle_command({register, {ReqId, _}, Module0, File}, _From,
               #state{partition=Partition,
                      node=Node,
                      variables=Variables,
                      programs=Programs0}=State) ->
    try
        %% Compile under original name, for the pure functions like
        %% `sum' and `merge'.
        %%
        {ok, _, Bin0} = compile:file(File, [binary,
                                            {parse_transform, lager_transform}]),
        {module, Module0} = code:load_binary(Module0, File, Bin0),

        %% Compile under unique name for vnode.
        %%
        Module = generate_unique_module_identifier(Partition,
                                                   Node,
                                                   Module0),
        case compile:file(File, [binary,
                                 {parse_transform, lager_transform},
                                 {parse_transform, lasp_transform},
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

handle_command({declare, {ReqId, _}, Id, Type}, _From,
               #state{variables=Variables}=State) ->
    {ok, Id} = ?BACKEND:declare(Id, Type, Variables),
    {reply, {ok, ReqId, Id}, State};

handle_command({bind_to, {ReqId, _}, Id, DVId}, FromPid,
               State=#state{variables=Variables}) ->
    FetchFun = fun(_TargetId, _FromId, _FromPid) ->
            ?MODULE:fetch(_TargetId, _FromId, _FromPid, ReqId)
    end,
    ?BACKEND:bind_to(Id, DVId, Variables, FetchFun, FromPid),
    {noreply, State};

handle_command({bind, {ReqId, _}, Id, Value}, _From,
               State=#state{variables=Variables}) ->
    NextKeyFun = fun(Type, Next) ->
                        next_key(Next, Type, State)
                 end,
    NotifyFun = fun(_Id, NewValue) ->
                        ?MODULE:notify_value(_Id, NewValue)
                end,
    {ok, Result} = ?BACKEND:bind(Id, Value, Variables, NextKeyFun,
                                 NotifyFun),
    {reply, {ok, ReqId, Result}, State};

handle_command({update, {ReqId, _}, Id, Operation, Actor}, _From,
               State=#state{variables=Variables}) ->
    NextKeyFun = fun(Type, Next) ->
                        next_key(Next, Type, State)
                 end,
    NotifyFun = fun(_Id, NewValue) ->
                        ?MODULE:notify_value(_Id, NewValue)
                end,
    {ok, Result} = ?BACKEND:update(Id, Operation, Actor, Variables,
                                   NextKeyFun, NotifyFun),
    {reply, {ok, ReqId, Result}, State};

handle_command({fetch, TargetId, FromId, FromPid, ReqId}, _From,
               State=#state{variables=Variables}) ->
    ResponseFun = fun() ->
            {noreply, State}
    end,
    FetchFun = fun(_TargetId, _FromId, _FromPid) ->
            ?MODULE:fetch(_TargetId, _FromId, _FromPid, ReqId)
    end,
    ReplyFetchFun = fun(_FromId, _FromPid, DV) ->
            ?MODULE:reply_fetch(_FromId, _FromPid, ReqId, DV)
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

handle_command({reply_fetch, FromId, FromPid, ReqId,
                #dv{value=Value, next=Next, type=Type}}, _From,
               State=#state{variables=Variables}) ->
    NotifyFun = fun(Id, NewValue) ->
                        ?MODULE:notify_value(Id, NewValue)
                end,
    ?BACKEND:write(Type, Value, Next, FromId, Variables, NotifyFun),
    {ok, _} = ?BACKEND:reply_to_all([FromPid], {ok, ReqId, Next}),
    {noreply, State};

handle_command({notify_value, Id, Value}, _From,
               State=#state{variables=Variables}) ->
    NotifyFun = fun(_Id, NewValue) ->
                        ?MODULE:notify_value(_Id, NewValue)
                end,
    ?BACKEND:notify_value(Id, Value, Variables, NotifyFun),
    {noreply, State};

handle_command({thread, {ReqId, _}, Module, Function, Args}, _From,
               #state{variables=Variables}=State) ->
    ok = ?BACKEND:thread(Module, Function, Args, Variables),
    {reply, {ok, ReqId, ok}, State};

handle_command({wait_needed, {ReqId, _}, Id, Threshold}, From,
               State=#state{variables=Variables}) ->
    Self = From,
    ReplyFun = fun(ReadThreshold) ->
                       {reply, {ok, ReqId, ReadThreshold}, State}
               end,
    BlockingFun = fun() ->
                        {noreply, State}
                  end,
    ?BACKEND:wait_needed(Id, Threshold, Variables, Self, ReplyFun, BlockingFun);

handle_command({read, {ReqId, _}, Id, Threshold}, From,
               State=#state{variables=Variables}) ->
    Self = From,
    ReplyFun = fun(_Id, Type, Value, Next) ->
            {reply, {ok, ReqId, {_Id, Type, Value, Next}}, State}
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

handle_command({filter, {ReqId, _}, Id, Function, AccId}, _From,
               State=#state{variables=Variables,
                            partition=Partition,
                            node=Node}) ->
    BindFun = fun(_AccId, AccValue, _Variables) ->
            %% Beware of cycles in the gen_server calls!
            [{IndexNode, _Type}] = lasp:preflist(?N, _AccId, lasp),

            case IndexNode of
                {Partition, Node} ->
                    %% We're local, which means that we can interact
                    %% directly with the data store.
                    ?BACKEND:bind(_AccId, AccValue, _Variables);
                _ ->
                    %% We're remote, go through all of the routing logic.
                    lasp:bind(_AccId, AccValue)
            end
    end,
    ReadFun = fun(_Id, _Threshold, _Variables) ->
            %% Beware of cycles in the gen_server calls!
            [{IndexNode, _Type}] = lasp:preflist(?N, _Id, lasp),

            case IndexNode of
                {Partition, Node} ->
                    %% We're local, which means that we can interact
                    %% directly with the data store.
                    ?BACKEND:read(_Id, _Threshold, _Variables);
                _ ->
                    %% We're remote, go through all of the routing logic.
                    lasp:read(_Id, _Threshold)
            end
    end,
    ok = ?BACKEND:filter(Id, Function, AccId, Variables, BindFun, ReadFun),
    {reply, {ok, ReqId, ok}, State};

handle_command({product, {ReqId, _}, Left, Right, Product}, _From,
               State=#state{variables=Variables,
                            partition=Partition,
                            node=Node}) ->
    BindFun = fun(_Product, AccValue, _Variables) ->
            %% Beware of cycles in the gen_server calls!
            [{IndexNode, _Type}] = lasp:preflist(?N, _Product, lasp),

            case IndexNode of
                {Partition, Node} ->
                    %% We're local, which means that we can interact
                    %% directly with the data store.
                    ?BACKEND:bind(_Product, AccValue, _Variables);
                _ ->
                    %% We're remote, go through all of the routing logic.
                    lasp:bind(_Product, AccValue)
            end
    end,
    ReadLeftFun = fun(_Left, _Threshold, _Variables) ->
            %% Beware of cycles in the gen_server calls!
            [{IndexNode, _Type}] = lasp:preflist(?N, Left, lasp),

            case IndexNode of
                {Partition, Node} ->
                    %% We're local, which means that we can interact
                    %% directly with the data store.
                    ?BACKEND:read(Left, _Threshold, _Variables);
                _ ->
                    %% We're remote, go through all of the routing logic.
                    lasp:read(Left, _Threshold)
            end
    end,
    ReadRightFun = fun(_Right, _Threshold, _Variables) ->
            %% Beware of cycles in the gen_server calls!
            [{IndexNode, _Type}] = lasp:preflist(?N, Right, lasp),

            case IndexNode of
                {Partition, Node} ->
                    %% We're local, which means that we can interact
                    %% directly with the data store.
                    ?BACKEND:read(Right, _Threshold, _Variables);
                _ ->
                    %% We're remote, go through all of the routing logic.
                    lasp:read(Right, _Threshold)
            end
    end,
    ok = ?BACKEND:product(Left, Right, Product, Variables, BindFun, ReadLeftFun, ReadRightFun),
    {reply, {ok, ReqId, ok}, State};

handle_command({map, {ReqId, _}, Id, Function, AccId}, _From,
               State=#state{variables=Variables,
                            partition=Partition,
                            node=Node}) ->
    BindFun = fun(_AccId, AccValue, _Variables) ->
            %% Beware of cycles in the gen_server calls!
            [{IndexNode, _Type}] = lasp:preflist(?N, _AccId, lasp),

            case IndexNode of
                {Partition, Node} ->
                    %% We're local, which means that we can interact
                    %% directly with the data store.
                    ?BACKEND:bind(_AccId, AccValue, _Variables);
                _ ->
                    %% We're remote, go through all of the routing logic.
                    lasp:bind(_AccId, AccValue)
            end
    end,
    ReadFun = fun(_Id, _Threshold, _Variables) ->
            %% Beware of cycles in the gen_server calls!
            [{IndexNode, _Type}] = lasp:preflist(?N, _Id, lasp),

            case IndexNode of
                {Partition, Node} ->
                    %% We're local, which means that we can interact
                    %% directly with the data store.
                    ?BACKEND:read(_Id, _Threshold, _Variables);
                _ ->
                    %% We're remote, go through all of the routing logic.
                    lasp:read(_Id, _Threshold)
            end
    end,
    ok = ?BACKEND:map(Id, Function, AccId, Variables, BindFun, ReadFun),
    {reply, {ok, ReqId, ok}, State};

handle_command({fold, {ReqId, _}, Id, Function, AccId}, _From,
               State=#state{variables=Variables,
                            partition=Partition,
                            node=Node}) ->
    BindFun = fun(_AccId, AccValue, _Variables) ->
            %% Beware of cycles in the gen_server calls!
            [{IndexNode, _Type}] = lasp:preflist(?N, _AccId, lasp),

            case IndexNode of
                {Partition, Node} ->
                    %% We're local, which means that we can interact
                    %% directly with the data store.
                    ?BACKEND:bind(_AccId, AccValue, _Variables);
                _ ->
                    %% We're remote, go through all of the routing logic.
                    lasp:bind(_AccId, AccValue)
            end
    end,
    ReadFun = fun(_Id, _Threshold, _Variables) ->
            %% Beware of cycles in the gen_server calls!
            [{IndexNode, _Type}] = lasp:preflist(?N, _Id, lasp),

            case IndexNode of
                {Partition, Node} ->
                    %% We're local, which means that we can interact
                    %% directly with the data store.
                    ?BACKEND:read(_Id, _Threshold, _Variables);
                _ ->
                    %% We're remote, go through all of the routing logic.
                    lasp:read(_Id, _Threshold)
            end
    end,
    ok = ?BACKEND:fold(Id, Function, AccId, Variables, BindFun, ReadFun),
    {reply, {ok, ReqId, ok}, State};

handle_command({next, {ReqId, _}, Id}, _From,
               State=#state{variables=Variables}) ->
    DeclareNextFun = fun(Type) ->
                            declare_next(Type, State)
                     end,
    {ok, Result} = ?BACKEND:next(Id, Variables, DeclareNextFun),
    {reply, {ok, ReqId, Result}, State};

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
declare_next(Type, #state{partition=Partition,
                          node=Node,
                          variables=Variables}) ->
    Id = druuid:v4(),

    %% Beware of cycles in the gen_server calls!
    [{IndexNode, _Type}] = lasp:preflist(?N, Id, lasp),

    case IndexNode of
        {Partition, Node} ->
            %% We're local, which means that we can interact directly
            %% with the data store.
            ?BACKEND:declare(Id, Type, Variables);
        _ ->
            %% We're remote, go through all of the routing logic.
            lasp:declare(Id, Type)
    end.

%% Internal program execution functions.

%% @doc Execute a given program.
execute(Module, Programs) ->
    case dict:is_key(Module, Programs) of
        true ->
            State = dict:fetch(Module, Programs),
            Self = self(),
            ReqId = lasp:mk_reqid(),
            spawn_link(fun() ->
                        Result = Module:execute(State),
                        Self ! {ReqId, ok, Result}
                        end),
            {ok, Result} = lasp:wait_for_reqid(ReqId, infinity),
            Result;
        false ->
            lager:info("Failed to execute module: ~p", [Module]),
            {error, undefined}
    end.

%% @doc Process a given program.
process(Module, Object, Reason, Idx, Programs0) ->
    case dict:is_key(Module, Programs0) of
        true ->
            State0 = dict:fetch(Module, Programs0),
            Self = self(),
            ReqId = lasp:mk_reqid(),
            spawn_link(fun() ->
                        {ok, State} = Module:process(Object, Reason, Idx, State0),
                        Programs = dict:store(Module, State, Programs0),
                        Self ! {ReqId, ok, {ok, Programs}}
                        end),
            {ok, {Result, Programs}} = lasp:wait_for_reqid(ReqId, infinity),
            {ok, Result, Programs};
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
