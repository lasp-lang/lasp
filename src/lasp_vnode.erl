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
         union/5,
         intersection/5,
         fold/5,
         wait_needed/4,
         repair/4,
         declare/4,
         thread/5]).

%% Program execution functions.
-export([register/5,
         execute/3,
         process/6]).

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

-record(program, {module,
                  file,
                  bin,
                  state}).

-record(state, {node,
                partition,
                store,
                reference}).

-define(READ, fun(_Id, _Threshold, _Store) ->
                  %% Beware of cycles in the gen_server calls!
                  [{IndexNode, _Type}|_] = ?APP:preflist(?N, _Id, lasp),

                  case IndexNode of
                      {Partition, Node} ->
                          %% We're local, which means that we can interact
                          %% directly with the data store.
                          ?CORE:read(_Id, _Threshold, _Store);
                      _ ->
                          %% We're remote, go through all of the routing logic.
                          ?APP:read(_Id, _Threshold)
                  end
              end).

-define(BIND, fun(_AccId, _AccValue, _Store) ->
                  ?APP:bind(_AccId, _AccValue)
              end).

-define(BLOCKING, fun() -> {noreply, State} end).

%% Extrenal API

repair(IdxNode, Id, Type, Value) ->
    riak_core_vnode_master:command(IdxNode,
                                   {repair, undefined, Id, Type, Value},
                                   ignore,
                                   ?VNODE_MASTER).

register(Preflist, Identity, Module, File, Options) ->
    riak_core_vnode_master:command(Preflist,
                                   {register, Identity, Module, File, Options},
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

union(Preflist, Identity, Left, Right, Union) ->
    riak_core_vnode_master:command(Preflist,
                                   {union, Identity, Left, Right, Union},
                                   {fsm, undefined, self()},
                                   ?VNODE_MASTER).

intersection(Preflist, Identity, Left, Right, Intersection) ->
    riak_core_vnode_master:command(Preflist,
                                   {intersection, Identity, Left, Right, Intersection},
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

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    %% Initialize the partition.
    Node = node(),
    Identifier = generate_unique_partition_identifier(Partition, Node),
    {ok, Store} = ?CORE:start(Identifier),

    %% Load all applications from stored binaries.
    {ok, Reference} = dets:open_file(Identifier, []),
    TraverseFun = fun({Module, #program{file=File, bin=Bin}}) ->
            case code:load_binary(Module, File, Bin) of
                {module, Module} ->
                    ok;
                Error ->
                    lager:info("Error loading module; error: ~p!",
                               [Error]),
                    ok
            end,
            continue
    end,
    dets:traverse(Reference, TraverseFun),

    {ok, #state{partition=Partition,
                node=Node,
                store=Store,
                reference=Reference}}.

%% Program execution handling.

handle_command({repair, undefined, Id, Type, Value}, _From,
               #state{store=Store}=State) ->
    ?CORE:write(Type, Value, Id, Store),
    {noreply, State};

handle_command({execute, {ReqId, _}, Module0}, _From,
               #state{reference=Reference,
                      node=Node,
                      store=Store,
                      partition=Partition}=State) ->
    Module = generate_unique_module_identifier(Partition,
                                               Node,
                                               Module0),
    case module_execute(Module, Reference, Store) of
        {ok, Result} ->
            {reply, {ok, ReqId, Result}, State};
        {error, undefined} ->
            {reply, {error, ReqId}, State}
    end;

handle_command({process, {ReqId, _}, Module0, Object, Reason, Idx}, _From,
               #state{reference=Reference,
                      node=Node,
                      store=Store,
                      partition=Partition}=State) ->
    Module = generate_unique_module_identifier(Partition,
                                               Node,
                                               Module0),
    case module_process(Module, Object, Reason, Idx, Reference, Store) of
        ok ->
            {reply, {ok, ReqId}, State};
        {error, undefined} ->
            {reply, {error, ReqId}, State}
    end;

handle_command({register, {ReqId, _}, Module0, File, Options0}, _From,
               #state{partition=Partition,
                      node=Node,
                      store=Store,
                      reference=Reference}=State) ->
    try
        %% Allow override module name from users trying to register
        %% programs.
        Module1 = case lists:keyfind(module, 1, Options0) of
            {module, M} ->
                M;
            _ ->
                Module0
        end,

        %% Compile under original name, for the pure functions like
        %% `sum' and `merge', but only if not already compiled.
        try
            _ = Module1:module_info()
        catch
            _:_ ->
                Opts = [binary,
                        {parse_transform, lager_transform},
                        {parse_transform, lasp_transform}] ++ Options0,
                try
                    {ok, _, Bin0} = compile:file(File, Opts),
                    try
                        {module, Module1} = code:load_binary(Module1,
                                                             File,
                                                             Bin0)
                    catch
                        _:LoadError ->
                            lager:info("Failed to load binary; module: ~p, error: ~p",
                                       [Module1, LoadError])
                    end
                catch
                    _:CompileError ->
                        lager:info("Failed to compile; module: ~p, error: ~p",
                                   [Module1, CompileError])
                end
        end,

        %% Compile under unique name for vnode.
        %%
        Module = generate_unique_module_identifier(Partition,
                                                   Node,
                                                   Module1),

        %% Generate options for compilation.
        Options = [binary,
                   {parse_transform, lager_transform},
                   {parse_transform, lasp_transform},
                   {store, Store},
                   {partition, Partition},
                   {module, Module},
                   {node, Node}] ++ Options0,
        try
            _ = Module:module_info(),
            {reply, {ok, ReqId}, State}
        catch
            _:_ ->
                case compile:file(File, Options) of
                    {ok, _, Bin} ->
                        case code:load_binary(Module, File, Bin) of
                            {module, Module} ->
                                {ok, Value} = Module:init(Store),
                                ok = dets:insert(Reference,
                                                 [{Module,
                                                   #program{module=Module,
                                                            file=File,
                                                            bin=Bin,
                                                            state=Value}}]),
                                ok = update_broadcast({add, Module1}, Partition),
                                {reply, {ok, ReqId}, State};
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
        end
    catch
        _:Exception ->
            lager:info("Exception: ~p, partition: ~p, module: ~p",
                       [Exception, Partition, Module0]),
            {reply, {error, Exception}, State}
    end;

%% Language handling.

handle_command({declare, {ReqId, _}, Id, Type}, _From,
               #state{store=Store, partition=_Partition}=State) ->
    {ok, Id} = ?CORE:declare(Id, Type, Store),
    {reply, {ok, ReqId, Id}, State};

handle_command({bind_to, {ReqId, _}, Id, DVId}, _From,
               State=#state{partition=Partition, node=Node, store=Store}) ->
    ok = ?CORE:bind_to(Id, DVId, Store, ?BIND, ?READ),
    {reply, {ok, ReqId, ok}, State};

handle_command({bind, {ReqId, _}, Id, Value}, _From,
               State=#state{partition=Partition, node=Node, store=Store}) ->
    {ok, Result} = ?CORE:bind(Id, Value, Store),
    {reply, {ok, ReqId, {Partition, Node}, Result}, State};

handle_command({update, {ReqId, _}, Id, Operation, Actor}, _From,
               State=#state{partition=Partition, node=Node, store=Store}) ->
    {ok, Result} = ?CORE:update(Id, Operation, Actor, Store),
    {reply, {ok, ReqId, {Partition, Node}, Result}, State};

handle_command({thread, {ReqId, _}, Module, Function, Args}, _From,
               #state{store=Store}=State) ->
    ok = ?CORE:thread(Module, Function, Args, Store),
    {reply, {ok, ReqId, ok}, State};

handle_command({wait_needed, {ReqId, _}, Id, Threshold}, From,
               State=#state{store=Store}) ->
    ReplyFun = fun(ReadThreshold) ->
                        {reply, {ok, ReqId, ReadThreshold}, State}
               end,
    ?CORE:wait_needed(Id, Threshold, Store, From, ReplyFun, ?BLOCKING);

handle_command({read, {ReqId, _}, Id, Threshold}, From,
               State=#state{store=Store}) ->
    ReplyFun = fun(_Id, Type, Value) ->
                    {reply, {ok, ReqId, {_Id, Type, Value}}, State}
               end,
    ?CORE:read(Id, Threshold, Store, From, ReplyFun, ?BLOCKING);

handle_command({filter, {ReqId, _}, Id, Function, AccId}, _From,
               State=#state{store=Store,
                            partition=Partition,
                            node=Node}) ->
    ok = ?CORE:filter(Id, Function, AccId, Store, ?BIND, ?READ),
    {reply, {ok, ReqId, ok}, State};

handle_command({product, {ReqId, _}, Left, Right, Product}, _From,
               State=#state{store=Store,
                            partition=Partition,
                            node=Node}) ->
    ok = ?CORE:product(Left, Right, Product, Store, ?BIND, ?READ, ?READ),
    {reply, {ok, ReqId, ok}, State};

handle_command({intersection, {ReqId, _}, Left, Right, Intersection}, _From,
               State=#state{store=Store,
                            partition=Partition,
                            node=Node}) ->
    ok = ?CORE:intersection(Left, Right, Intersection, Store, ?BIND, ?READ, ?READ),
    {reply, {ok, ReqId, ok}, State};

handle_command({union, {ReqId, _}, Left, Right, Union}, _From,
               State=#state{store=Store,
                            partition=Partition,
                            node=Node}) ->
    ok = ?CORE:union(Left, Right, Union, Store, ?BIND, ?READ, ?READ),
    {reply, {ok, ReqId, ok}, State};

handle_command({map, {ReqId, _}, Id, Function, AccId}, _From,
               State=#state{store=Store,
                            partition=Partition,
                            node=Node}) ->
    ok = ?CORE:map(Id, Function, AccId, Store, ?BIND, ?READ),
    {reply, {ok, ReqId, ok}, State};

handle_command({fold, {ReqId, _}, Id, Function, AccId}, _From,
               State=#state{store=Store,
                            partition=Partition,
                            node=Node}) ->
    ok = ?CORE:fold(Id, Function, AccId, Store, ?BIND, ?READ),
    {reply, {ok, ReqId, ok}, State};

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{}, _Sender, State) ->
    %% Ignore handoff.
    {reply, ok, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(_Data, State) ->
    %% Ignore handoff.
    {reply, ok, State}.

encode_handoff_item(Key, Operation) ->
    term_to_binary({Key, Operation}).

is_empty(State) ->
    {true, State}.

delete(State) ->
    {ok, State}.

handle_coverage(?EXECUTE_REQUEST{module=Module0}, _KeySpaces, Sender,
                #state{reference=Reference,
                       node=Node,
                       store=Store,
                       partition=Partition}=State) ->
    Module = generate_unique_module_identifier(Partition,
                                               Node,
                                               Module0),
    case module_execute(Module, Reference, Store) of
        {ok, Result} ->
            VisitFun = fun(Key, Value) ->
                    riak_core_vnode:reply(Sender,
                                          {self(), {Key, Value}})
            end,
            FinishFun = fun() ->
                    riak_core_vnode:reply(Sender, done)
            end,
            gb_trees_ext:iterate(VisitFun, FinishFun, Result),
            {noreply, State};
        {error, undefined} ->
            {reply, {error, undefined}, State}
    end;
handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%% Internal program execution functions.

%% @doc Execute a given program.
module_execute(Module, Reference, Store) ->
    case dets:member(Reference, Module) of
        true ->
            {Module, #program{state=State}} = hd(dets:lookup(Reference, Module)),
            Module:execute(State, Store);
        false ->
            lager:info("Failed to execute module: ~p", [Module]),
            {error, undefined}
    end.

%% @doc Process a given program.
module_process(Module, Object, Reason, Idx, Reference, Store) ->
    case dets:member(Reference, Module) of
        true ->
            {Module, #program{state=State0}=Program} = hd(dets:lookup(Reference, Module)),
            {ok, State} = Module:process(Object, Reason, Idx, State0, Store),
            ok = dets:insert(Reference, [{Module, Program#program{state=State}}]),
            ok;
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

update_broadcast({add, Module}, Partition) ->
    Set0 = case riak_core_metadata:get(?PROGRAM_PREFIX, ?PROGRAM_KEY, []) of
        undefined ->
            ?SET:new();
        X ->
            X
    end,
    {ok, Set} = ?SET:update({add, Module}, Partition, Set0),
    riak_core_metadata:put(?PROGRAM_PREFIX, ?PROGRAM_KEY, Set, []).
