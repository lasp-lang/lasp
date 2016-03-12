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

-module(lasp_core).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-include("lasp.hrl").

%% Core API.
-export([start/1,
         bind/3,
         bind/4,
         bind_to/3,
         read/2,
         read/3,
         read_any/2,
         declare/1,
         declare/2,
         declare/3,
         declare/4,
         declare/5,
         declare_dynamic/4,
         query/2,
         update/4,
         update/5,
         thread/4,
         filter/4,
         map/4,
         product/4,
         union/4,
         intersection/4,
         fold/4,
         wait_needed/2,
         wait_needed/3,
         reply_to_all/2,
         reply_to_all/3,
         receive_delta/2]).

%% Exported functions for vnode integration, where callback behavior is
%% dynamic.
-export([bind_to/4,
         bind_to/5,
         wait_needed/6,
         read/6,
         write/4,
         filter/6,
         map/6,
         product/7,
         union/7,
         intersection/7,
         fold/6]).

%% Definitions for the bind/read fun abstraction.
-define(BIND, fun(_AccId, AccValue, _Store) ->
                ?MODULE:bind(_AccId, AccValue, _Store)
              end).

-define(READ, fun(_Id, _Threshold) ->
                ?MODULE:read(_Id, _Threshold, Store)
              end).

%% @doc Initialize the storage backend.
-spec start(atom()) -> {ok, store()} | {error, term()}.
start(Identifier) ->
    do(start, [Identifier]).

%% @doc Filter values from one lattice into another.
%%
%%      Applies the given `Function' as a filter over the items in `Id',
%%      placing the result in `AccId', both of which need to be declared
%%      variables.
%%
-spec filter(id(), function(), id(), store()) -> {ok, pid()}.
filter(Id, Function, AccId, Store) ->
    filter(Id, Function, AccId, Store, ?BIND, ?READ).

%% @doc Fold values from one lattice into another.
%%
%%      Applies the given `Function' as a fold over the items in `Id',
%%      placing the result in `AccId', both of which need to be declared
%%      variables.
%%
-spec fold(id(), function(), id(), store()) -> {ok, pid()}.
fold(Id, Function, AccId, Store) ->
    fold(Id, Function, AccId, Store, ?BIND, ?READ).

%% @doc Map values from one lattice into another.
%%
%%      Applies the given `Function' as a map over the items in `Id',
%%      placing the result in `AccId', both of which need to be declared
%%      variables.
%%
-spec map(id(), function(), id(), store()) -> {ok, pid()}.
map(Id, Function, AccId, Store) ->
    map(Id, Function, AccId, Store, ?BIND, ?READ).

%% @doc Compute the intersection of two sets.
%%
%%      Computes the intersection of two sets and bind the result
%%      to a third.
%%
-spec intersection(id(), id(), id(), store()) -> {ok, pid()}.
intersection(Left, Right, Intersection, Store) ->
    ReadLeftFun = fun(_Left, _Threshold, _Variables) ->
            ?MODULE:read(_Left, _Threshold, _Variables)
    end,
    ReadRightFun = fun(_Right, _Threshold, _Variables) ->
            ?MODULE:read(_Right, _Threshold, _Variables)
    end,
    intersection(Left, Right, Intersection, Store, ?BIND, ReadLeftFun, ReadRightFun).

%% @doc Compute the union of two sets.
%%
%%      Computes the union of two sets and bind the result
%%      to a third.
%%
-spec union(id(), id(), id(), store()) -> {ok, pid()}.
union(Left, Right, Union, Store) ->
    ReadLeftFun = fun(_Left, _Threshold, _Variables) ->
            ?MODULE:read(_Left, _Threshold, _Variables)
    end,
    ReadRightFun = fun(_Right, _Threshold, _Variables) ->
            ?MODULE:read(_Right, _Threshold, _Variables)
    end,
    union(Left, Right, Union, Store, ?BIND, ReadLeftFun, ReadRightFun).

%% @doc Compute the cartesian product of two sets.
%%
%%      Computes the cartestian product of two sets and bind the result
%%      to a third.
%%
-spec product(id(), id(), id(), store()) -> {ok, pid()}.
product(Left, Right, Product, Store) ->
    ReadLeftFun = fun(_Left, _Threshold, _Variables) ->
            ?MODULE:read(_Left, _Threshold, _Variables)
    end,
    ReadRightFun = fun(_Right, _Threshold, _Variables) ->
            ?MODULE:read(_Right, _Threshold, _Variables)
    end,
    product(Left, Right, Product, Store, ?BIND, ReadLeftFun, ReadRightFun).

%% @doc Perform a read for a particular identifier.
%%
%%      Given an `Id', perform a blocking read until the variable is
%%      bound.
%%
-spec read(id(), store()) -> {ok, var()}.
read(Id, Store) ->
    read(Id, {strict, undefined}, Store).

%% @doc Perform a monotonic read read for a particular identifier.
%%
%%      Given an `Id', perform a blocking read until the variable is
%%      bound.
%%
%%      This operation blocks until `Threshold' has been reached.
%%
-spec read(id(), value(), store()) -> {ok, var()}.
read(Id, Threshold, Store) ->
    Self = self(),
    ReplyFun = fun({Id1, Type, Metadata, Value}) ->
                       {ok, {Id1, Type, Metadata, Value}}
               end,
    BlockingFun = fun() ->
                receive
                    X ->
                        X
                end
            end,
    read(Id, Threshold, Store, Self, ReplyFun, BlockingFun).

%% @doc Perform a monotonic read for a series of given idenfitiers --
%%      first response wins.
%%
-spec read_any([{id(), value()}], store()) -> {ok, var()}.
read_any(Reads, Store) ->
    Self = self(),
    case read_any(Reads, Self, Store) of
        {ok, not_available_yet} ->
            receive
                X ->
                    X
            end;
        {ok, {Id, Type, Metadata, Value}} ->
            {ok, {Id, Type, Metadata, Value}}
    end.

%% @doc Declare a dataflow variable in a provided by identifer.
-spec declare(store()) -> {ok, var()}.
declare(Store) ->
    declare(lasp_ivar, Store).

%% @doc Declare a dataflow variable, as a given type.
-spec declare(type(), store()) -> {ok, var()}.
declare(Type, Store) ->
    {ok, Unique} = lasp_unique:unique(),
    declare(Unique, Type, Store).

%% @doc Declare a dataflow variable in a provided by identifer.
-spec declare(id(), type(), store()) -> {ok, var()}.
declare(Id, Type, Store) ->
    MetadataFun = fun(X) -> X end,
    declare(Id, Type, MetadataFun, Store).

%% @doc Declare a dataflow variable in a provided by identifer.
-spec declare(id(), type(), function(), store()) -> {ok, var()}.
declare(Id, Type, MetadataFun, Store) ->
    declare(Id, Type, MetadataFun, orddict:new(), Store).

%% @doc Declare a dataflow variable in a provided by identifer.
-spec declare(id(), type(), function(), any(), store()) -> {ok, var()}.
declare(Id, Type, MetadataFun, MetadataNew, Store) ->
    case do(get, [Store, Id]) of
        {ok, #dv{value=Value, metadata=Metadata}} ->
            %% Do nothing; make declare idempotent at each replica.
            {ok, {Id, Type, Metadata, Value}};
        _ ->
            Value = lasp_type:new(Type),
            Metadata = MetadataFun(MetadataNew),
            Counter0 = 0,
            DeltaMap0 = orddict:new(),
            AckMap = orddict:new(),
            DeltaMap = orddict:store(Counter0, Value, DeltaMap0),
            ok = do(put, [Store, Id, #dv{value=Value,
                                         type=Type,
                                         metadata=Metadata,
                                         delta_counter=increment_counter(Counter0),
                                         delta_map=DeltaMap,
                                         delta_ack_map=AckMap}]),
            {ok, {Id, Type, Metadata, Value}}
    end.

%% @doc Declare a dynamic variable in a provided by identifer.
-spec declare_dynamic(id(), type(), function(), store()) -> {ok, var()}.
declare_dynamic(Id, Type, MetadataFun0, Store) ->
    MetadataFun = fun(X) ->
                          orddict:store(dynamic, true, MetadataFun0(X))
                  end,
    declare(Id, Type, MetadataFun, Store).

%% @doc Return the current value of a CRDT.
%%
-spec query(id(), store()) -> {ok, term()}.
query(Id, Store) ->
    {ok, #dv{value=Value0, type=Type}} = do(get, [Store, Id]),
    Value = lasp_type:query(Type, Value0),
    {ok, Value}.

%% @doc Define a dataflow variable to be bound to another dataflow
%%      variable.
%%
-spec bind_to(id(), id(), store()) -> {ok, pid()}.
bind_to(Id, TheirId, Store) ->
    bind_to(Id, TheirId, Store, ?BIND, ?READ).

%% @doc Spawn a function.
%%
%%      Spawn a process executing `Module:Function(Args)'.
%%
-spec thread(module(), func(), args(), store()) -> ok.
thread(Module, Function, Args, _Store) ->
    Fun = fun() -> erlang:apply(Module, Function, Args) end,
    spawn(Fun),
    ok.

%% Internal functions

%% Core API.

%% @doc Pause execution until value requested with given threshold.
%%
%%      Pause execution of calling thread until a read operation is
%%      issued for the given `Id'.  Used to introduce laziness into a
%%      computation.
%%
-spec wait_needed(id(), store()) -> {ok, threshold()}.
wait_needed(Id, Store) ->
    wait_needed(Id, {strict, undefined}, Store).

%% @doc Pause execution until value requested with given threshold.
%%
%%      Pause execution of calling thread until a read operation is
%%      issued for the given `Id'.  Used to introduce laziness into a
%%      computation.
%%
%%      This operation blocks until `Threshold' has been requested.
%%
-spec wait_needed(id(), threshold(), store()) -> {ok, threshold()}.
wait_needed(Id, Threshold, Store) ->
    Self = self(),
    ReplyFun = fun(ReadThreshold) ->
                       {ok, ReadThreshold}
               end,
    BlockingFun = fun() ->
                          receive
                              X ->
                                  X
                          end
                  end,
    wait_needed(Id, Threshold, Store, Self, ReplyFun, BlockingFun).

%% Callback functions.

%% @doc Update a dataflow variable given an operation.
%%
%%      Similar to {@link update/5}.
%%
%%      Read the given `Id' and update it given the provided
%%      `Operation', which should be valid for the type of CRDT stored
%%      at the given `Id'.
%%
-spec update(id(), operation(), actor(), store()) -> {ok, var()}.
update(Id, Operation, Actor, Store) ->
    MetadataFun = fun(X) -> X end,
    update(Id, Operation, Actor, MetadataFun, Store).

-spec update(id(), operation(), actor(), function(), store()) -> {ok, var()}.
update(Id, Operation, Actor, MetadataFun, Store) ->
    {ok, #dv{value=Value0, type=Type}} = do(get, [Store, Id]),
    {ok, Value} = lasp_type:update(Type, Operation, Actor, Value0),
    bind(Id, Value, MetadataFun, Store).

%% @doc Define a dataflow variable to be bound a value.
-spec bind(id(), value(), store()) -> {ok, var()}.
bind(Id, Value, Store) ->
    MetadataFun = fun(X) -> X end,
    bind(Id, Value, MetadataFun, Store).

%% @doc Define a dataflow variable to be bound a value.
-spec bind(id(), value(), function(), store()) -> {ok, var()}.
bind(Id, {delta, Value}, MetadataFun, Store) ->
    Mutator = fun(#dv{type=Type, metadata=Metadata0, value=Value0,
                      waiting_threads=WT, delta_counter=Counter0,
                      delta_map=DeltaMap0, delta_ack_map=AckMap}=Object) ->
            Metadata = MetadataFun(Metadata0),
            %% Merge may throw for invalid types.
            try
                Merged = lasp_type:merge(Type, Value0, Value),
                {ok, SW} = reply_to_all(WT, [], {ok, {Id, Type, Metadata, Merged}}),
                case lasp_lattice:is_strict_inflation(Type, Value0, Merged) of
                    true ->
                        DeltaMap = orddict:store(Counter0, Value, DeltaMap0),
                        NewObject = #dv{type=Type, metadata=Metadata, value=Merged,
                                        waiting_threads=SW,
                                        delta_counter=increment_counter(Counter0),
                                        delta_map=DeltaMap, delta_ack_map=AckMap},
                        %% Return value is a delta state.
                        {NewObject, {ok, {Id, Type, Metadata, {delta, Merged}}}};
                    false ->
                        %% Given delta state is already merged, no delta info update.
                        {Object#dv{waiting_threads=SW},
                         {ok, {Id, Type, Metadata, {delta, Merged}}}}
                end
            catch
                _:Reason ->
                    %% Merge threw.
                    lager:warning("Exception; type: ~p, reason: ~p ~p => ~p",
                                    [Type, Reason, Value0, Value]),
                    {Object, {ok, {Id, Type, Metadata, Value0}}}
            end
    end,
    do(update, [Store, Id, Mutator]);
bind(Id, Value, MetadataFun, Store) ->
    Mutator = fun(#dv{type=Type, metadata=Metadata0, value=Value0,
                      waiting_threads=WT, delta_counter=Counter,
                      delta_map=DeltaMap, delta_ack_map=AckMap}=Object) ->
            Metadata = MetadataFun(Metadata0),
            case Value0 of
                Value ->
                    %% Bind to current value.
                    {Object, {ok, {Id, Type, Metadata, Value}}};
                _ ->
                    %% Merge may throw for invalid types.
                    try
                        Merged = lasp_type:merge(Type, Value0, Value),
                        case lasp_lattice:is_inflation(Type, Value0, Merged) of
                            true ->
                                {ok, SW} = reply_to_all(WT, [], {ok, {Id, Type, Metadata, Merged}}),
                                NewObject = #dv{type=Type, metadata=Metadata,
                                                value=Merged, waiting_threads=SW,
                                                delta_counter=Counter,
                                                delta_map=DeltaMap,
                                                delta_ack_map=AckMap},
                                {NewObject, {ok, {Id, Type, Metadata, Merged}}};
                            false ->
                                %% Value is old.
                                {Object, {ok, {Id, Type, Metadata, Value0}}}
                        end
                    catch
                        _:Reason ->
                            %% Merge threw.
                            lager:warning("Exception; type: ~p, reason: ~p ~p => ~p",
                                          [Type, Reason, Value0, Value]),
                            {Object, {ok, {Id, Type, Metadata, Value0}}}
                    end
            end
    end,
    do(update, [Store, Id, Mutator]).

%% @doc Perform a read (or monotonic read) for a particular identifier.
%%
%%      Given an `Id', perform a blocking read until the variable is
%%      bound.
%%
%%      Perform a read -- reads will either block until the `Threshold'
%%      is met, or the variable is bound.  Reads will be performed
%%      against the `Store' provided.  When the process should register
%%      itself for notification of the variable being bound, it should
%%      supply the process identifier for notifications as `Self'.
%%      Finally, the `ReplyFun' and `BlockingFun' functions will be
%%      executed in the event that the reply is available immediately,
%%      or it will have to wait for the notification, in the event the
%%      variable is unbound or has not met the threshold yet.
%%
-spec read(id(), value(), store(), pid(), function(), function()) ->
    {ok, var()}.
read(Id, Threshold0, Store, Self, ReplyFun, BlockingFun) ->
    Mutator = fun(#dv{type=Type, value=Value, metadata=Metadata, lazy_threads=LT}=Object) ->
            %% When no threshold is specified, use the bottom value for the
            %% given lattice.
            %%
            Threshold = case Threshold0 of
                undefined ->
                    lasp_type:new(Type);
                {strict, undefined} ->
                    {strict, lasp_type:new(Type)};
                Threshold0 ->
                    Threshold0
            end,

            %% Notify all lazy processes of this read.
            {ok, SL} = reply_to_all(LT, {ok, Threshold}),

            %% Satisfy read if threshold is met.
            case lasp_lattice:threshold_met(Type, Value, Threshold) of
                true ->
                    {Object#dv{lazy_threads=SL}, {ok, {Id, Type, Metadata, Value}}};
                false ->
                    WT = lists:append(Object#dv.waiting_threads, [{threshold, read, Self, Type, Threshold}]),
                    {Object#dv{waiting_threads=WT, lazy_threads=SL}, {error, threshold_not_met}}
            end
    end,
    case do(update, [Store, Id, Mutator]) of
        {ok, {Id, Type, Metadata, Value}} ->
            ReplyFun({Id, Type, Metadata, Value});
        {error, threshold_not_met} ->
            %% Not valid for threshold; wait.
            BlockingFun();
        {error, Error} ->
            %% Error from the backend.
            ReplyFun({error, Error})
    end.

%% @doc Perform a read (or monotonic read) for a series of particular
%%      identifiers.
%%
-spec read_any([{id(), value()}], pid(), store()) ->
    {ok, var()} | {ok, not_available_yet}.
read_any(Reads, Self, Store) ->
    Found = lists:foldl(
            fun({Id, Threshold0}, AlreadyFound) ->
                    case AlreadyFound of
                        false ->
                            Mutator = fun(#dv{type=Type, value=Value, metadata=Metadata, lazy_threads=LT}=Object) ->
                                    %% When no threshold is specified, use the bottom
                                    %% value for the given lattice.
                                    %%
                                    Threshold = case Threshold0 of
                                        undefined ->
                                            lasp_type:new(Type);
                                        {strict, undefined} ->
                                            {strict, lasp_type:new(Type)};
                                        Threshold0 ->
                                            Threshold0
                                    end,

                                    %% Notify all lazy processes of this read.
                                    {ok, SL} = reply_to_all(LT, {ok, Threshold}),

                                    %% Satisfy read if threshold is met.
                                    case lasp_lattice:threshold_met(Type, Value, Threshold) of
                                        true ->
                                            {Object, {ok, {Id, Type, Metadata, Value}}};
                                        false ->
                                            WT = lists:append(Object#dv.waiting_threads, [{threshold, read, Self, Type, Threshold}]),
                                            {Object#dv{waiting_threads=WT, lazy_threads=SL}, error}
                                    end
                            end,

                            case do(update, [Store, Id, Mutator]) of
                                {ok, {Id, Type, Metadata, Value}} ->
                                    {ok, {Id, Type, Metadata, Value}};
                                error ->
                                    false
                            end;
                        Result ->
                            Result
                        end
                    end, false, Reads),

                    case Found of
                        false ->
                            {ok, not_available_yet};
                        Value ->
                            Value
                    end.

%% @doc Define a dataflow variable to be bound to another dataflow
%%      variable.
%%
%%      This version, performs a partial bind to another dataflow
%%      variable.
%%
%%      `FetchFun' is used to specify how to find the target identifier,
%%      given it is located in another data store.
%%
%%      `FromPid' is sent a message with the target identifiers value,
%%      if the target identifier is already bound.
%%
-spec bind_to(id(), id(), store(), function()) -> {ok, pid()}.
bind_to(AccId, Id, Store, BindFun) ->
    bind_to(AccId, Id, Store, BindFun, ?READ).

bind_to(AccId, Id, Store, BindFun, ReadFun) ->
    Fun = fun({_, _, _, V}) ->
        {ok, _} = BindFun(AccId, V, Store)
    end,
    gen_flow:start_link(lasp_process, [[{Id, ReadFun}], Fun]).

%% @doc Fold values from one lattice into another.
%%
%%      Applies the given `Function' as a fold over the items in `Id',
%%      placing the result in `AccId', both of which need to be declared
%%      variables.
%%
%%      Similar to {@link fold/4}, however, provides an override
%%      function for the `BindFun', which is responsible for binding the
%%      result, for instance, when it's located in another table.
%%
-spec fold(id(), function(), id(), store(), function(), function()) ->
    {ok, pid()}.
fold(Id, Function, AccId, Store, BindFun, ReadFun) ->
    {ok, {_, AccType, _, AccInitValue}} = ReadFun(AccId, undefined),
    Fun = fun({_, T, _, V}) ->
            AccValue = fold_internal(T, V, Function, AccType, AccInitValue),
            {ok, _} = BindFun(AccId, AccValue, Store)
    end,
    gen_flow:start_link(lasp_process, [[{Id, ReadFun}], Fun]).

fold_internal(lasp_orset, Value, Function, AccType, AccValue) ->
    lists:foldl(fun({X, Causality}, AccValue1) ->
        lists:foldl(fun({Actor, Deleted}, AccValue2) ->
                            %% Execute the fold function for the current
                            %% element.
                            Ops = Function(X, AccValue2),

                            %% Apply all operations to the accumulator.
                            lists:foldl(fun(Op, Acc) ->
                                                {ok, A} = AccType:update(Op, Actor, Acc),
                                                case Deleted of
                                                    true ->
                                                        InverseOp = lasp_operations:inverse(AccType, Op),
                                                        {ok, B} = AccType:update(InverseOp, Actor, A),
                                                        B;
                                                    false ->
                                                        A
                                                end
                                        end, AccValue2, Ops)
            end, AccValue1, Causality)
        end, AccValue, Value).

%% @doc Compute the cartesian product of two sets.
%%
%%      Computes the cartestian product of two sets and bind the result
%%      to a third.
%%
%%      Similar to {@link product/4}, however, provides an override
%%      function for the `BindFun', which is responsible for binding the
%%      result, for instance, when it's located in another table.
%%
-spec product(id(), id(), id(), store(), function(), function(),
              function()) -> {ok, pid()}.
product(Left, Right, AccId, Store, BindFun, ReadLeftFun, ReadRightFun) ->
    Fun = fun({_, T, _, LValue}, {_, _, _, RValue}) ->
            case {LValue, RValue} of
                {undefined, _} ->
                    ok;
                {_, undefined} ->
                    ok;
                {_, _} ->
                    AccValue = case T of
                        lasp_orset_gbtree ->
                            FolderFun = fun(X, XCausality, XAcc) ->
                                    InnerFoldFun = fun(Y, YCausality, YAcc) ->
                                            gb_trees:enter({X, Y},
                                                           lasp_lattice:causal_product(T, XCausality, YCausality),
                                                           YAcc)
                                    end,
                                    gb_trees_ext:foldl(InnerFoldFun, XAcc, RValue)
                            end,
                            gb_trees_ext:foldl(FolderFun, T:new(), LValue);
                        lasp_orset ->
                            FolderFun = fun({X, XCausality}, Acc) ->
                                    Acc ++ [{{X, Y}, lasp_lattice:causal_product(T, XCausality, YCausality)} || {Y, YCausality} <- RValue]
                            end,
                            lists:foldl(FolderFun, T:new(), LValue)
                    end,
                    {ok, _} = BindFun(AccId, AccValue, Store)
            end
    end,
    gen_flow:start_link(lasp_process,
                        [[{Left, ReadLeftFun}, {Right, ReadRightFun}],
                        Fun]).

%% @doc Compute the intersection of two sets.
%%
%%      Computes the intersection of two sets and bind the result
%%      to a third.
%%
%%      Similar to {@link intersection/4}, however, provides an override
%%      function for the `BindFun', which is responsible for binding the
%%      result, for instance, when it's located in another table.
%%
-spec intersection(id(), id(), id(), store(), function(), function(),
                   function()) -> {ok, pid()}.
intersection(Left, Right, AccId, Store, BindFun, ReadLeftFun, ReadRightFun) ->
    Fun = fun({_, T, _, LValue}, {_, _, _, RValue}) ->
            case {LValue, RValue} of
                {undefined, _} ->
                    ok;
                {_, undefined} ->
                    ok;
                {_, _} ->
                    AccValue = case T of
                                   lasp_orset_gbtree ->
                                       lasp_orset_gbtree:intersect(LValue, RValue);
                                   lasp_orset ->
                                       lasp_orset:intersect(LValue, RValue)
                               end,
                    {ok, _} = BindFun(AccId, AccValue, Store)
            end
    end,
    gen_flow:start_link(lasp_process,
                        [[{Left, ReadLeftFun}, {Right, ReadRightFun}],
                        Fun]).

%% @doc Compute the union of two sets.
%%
%%      Computes the union of two sets and bind the result
%%      to a third.
%%
%%      Similar to {@link union/4}, however, provides an override
%%      function for the `BindFun', which is responsible for binding the
%%      result, for instance, when it's located in another table.
%%
-spec union(id(), id(), id(), store(), function(), function(),
            function()) -> {ok, pid()}.
union(Left, Right, AccId, Store, BindFun, ReadLeftFun, ReadRightFun) ->
    Fun = fun({_, T, _, LValue}, {_, _, _, RValue}) ->
        case {LValue, RValue} of
                {undefined, _} ->
                    ok;
                {_, undefined} ->
                    ok;
                {_, _} ->
                    AccValue = case T of
                        lasp_orset_gbtree ->
                            lasp_orset_gbtree:merge(LValue, RValue);
                        lasp_orset ->
                            lasp_orset:merge(LValue, RValue)
                    end,
                    {ok, _} = BindFun(AccId, AccValue, Store)
            end
    end,
    gen_flow:start_link(lasp_process, [[{Left, ReadLeftFun}, {Right, ReadRightFun}], Fun]).

%% @doc Lap values from one lattice into another.
%%
%%      Applies the given `Function' as a map over the items in `Id',
%%      placing the result in `AccId', both of which need to be declared
%%      variables.
%%
%%      Similar to {@link map/4}, however, provides an override
%%      function for the `BindFun', which is responsible for binding the
%%      result, for instance, when it's located in another table.
%%
-spec map(id(), function(), id(), store(), function(), function()) ->
    {ok, pid()}.
map(Id, Function, AccId, Store, BindFun, ReadFun) ->
    Fun = fun({_, T, _, V}) ->
                  AccValue = case T of
                                 lasp_orset_gbtree ->
                                     lasp_orset_gbtree:map(Function, V);
                                 lasp_orset ->
                                     lasp_orset:map(Function, V)
                             end,
                  {ok, _} = BindFun(AccId, AccValue, Store)
          end,
    gen_flow:start_link(lasp_process, [[{Id, ReadFun}], Fun]).

%% @doc Filter values from one lattice into another.
%%
%%      Applies the given `Function' as a filter over the items in `Id',
%%      placing the result in `AccId', both of which need to be declared
%%      variables.
%%
%%      Similar to {@link filter/4}, however, provides an override
%%      function for the `BindFun', which is responsible for binding the
%%      result, for instance, when it's located in another table.
%%
-spec filter(id(), function(), id(), store(), function(), function()) ->
    {ok, pid()}.
filter(Id, Function, AccId, Store, BindFun, ReadFun) ->
    Fun = fun({_, T, _, V}) ->
            AccValue = case T of
                lasp_orset_gbtree -> lasp_orset_gbtree:filter(Function, V);
                lasp_orset -> lasp_orset:filter(Function, V)
            end,
            {ok, _} = BindFun(AccId, AccValue, Store)
    end,
    gen_flow:start_link(lasp_process, [[{Id, ReadFun}], Fun]).

%% @doc Callback wait_needed function for lasp_vnode, where we
%%      change the reply and blocking replies.
%%
%%      Similar to {@link wait_needed/2}.
%%
%%      `BlockingFun' is used to override the handling of waiting for a
%%      read operation to trigger on something that is waiting (lazy).
%%
%%      `ReplyFun' is used to override the function which is used to
%%      notify waiting processes, for instance, if they are running on
%%      another node.
%%
%%      This operation blocks until `Threshold' has been requested.
%%
-spec wait_needed(id(), threshold(), store(), pid(), function(),
                  function()) -> {ok, threshold()}.
wait_needed(Id, Threshold, Store, Self, ReplyFun, BlockingFun) ->
    {ok, #dv{waiting_threads=WT,
             type=Type,
             value=Value,
             lazy_threads=LazyThreads0}} = do(get, [Store, Id]),
    case lasp_lattice:threshold_met(Type, Value, Threshold) of
        true ->
            ReplyFun(Threshold);
        false ->
            case WT of
                [_H|_T] ->
                    ReplyFun(Threshold);
                _ ->
                    Mutator = fun(Object) ->
                            LazyThreads = case Threshold of
                                            undefined ->
                                                lists:append(LazyThreads0, [Self]);
                                            Threshold ->
                                                lists:append(LazyThreads0, [{threshold, wait, Self, Type, Threshold}])
                            end,
                            {Object#dv{lazy_threads=LazyThreads}, ok}
                    end,
                    ok = do(update, [Store, Id, Mutator]),
                    BlockingFun()
            end
    end.

%% @doc Given a group of processes which are blocking on reads, notify
%%      them of bound values or met thresholds.
%%
-spec reply_to_all(list(pid() | pending_threshold()), term()) ->
    {ok, list(pending_threshold())}.
reply_to_all(List, Result) ->
    reply_to_all(List, [], Result).

%% @doc Given a group of processes which are blocking on reads, notify
%%      them of bound values or met thresholds.
%%
-spec reply_to_all(list(pid() | pending_threshold()),
                   list(pending_threshold()), term()) ->
    {ok, list(pending_threshold())}.
reply_to_all([{threshold, read, From, Type, Threshold}=H|T],
             StillWaiting0,
             {ok, {Id, Type, Metadata, Value}}=Result) ->
    SW = case lasp_lattice:threshold_met(Type, Value, Threshold) of
        true ->
            case From of
                {server, undefined, {Address, Ref}} ->
                    gen_server:reply({Address, Ref},
                                     {ok, {Id, Type, Metadata, Value}});
                {fsm, undefined, Address} ->
                    gen_fsm:send_event(Address,
                                       {ok, undefined,
                                        {Id, Type, Metadata, Value}});
                {Address, Ref} ->
                    gen_server:reply({Address, Ref},
                                     {ok, {Id, Type, Metadata, Value}});
                _ ->
                    From ! Result
            end,
            StillWaiting0;
        false ->
            StillWaiting0 ++ [H]
    end,
    reply_to_all(T, SW, Result);
reply_to_all([{threshold, wait, From, Type, Threshold}=H|T],
             StillWaiting0,
             {ok, RThreshold}=Result) ->
    SW = case lasp_lattice:threshold_met(Type, Threshold, RThreshold) of
        true ->
            case From of
                {server, undefined, {Address, Ref}} ->
                    gen_server:reply({Address, Ref}, {ok, RThreshold});
                {fsm, undefined, Address} ->
                    gen_fsm:send_event(Address,
                                       {ok, undefined, RThreshold});
                {Address, Ref} ->
                    gen_server:reply({Address, Ref}, {ok, RThreshold});
                _ ->
                    From ! Result
            end,
            StillWaiting0;
        false ->
            StillWaiting0 ++ [H]
    end,
    reply_to_all(T, SW, Result);
reply_to_all([From|T], StillWaiting, Result) ->
    case From of
        {server, undefined, {Address, Ref}} ->
            gen_server:reply({Address, Ref}, Result);
        {fsm, undefined, Address} ->
            gen_fsm:send_event(Address, Result);
        {Address, Ref} ->
            gen_server:reply({Address, Ref}, Result);
        _ ->
            lager:info("Result: ~p", [Result]),
            From ! Result
    end,
    reply_to_all(T, StillWaiting, Result);
reply_to_all([], StillWaiting, _Result) ->
    {ok, StillWaiting}.

%% @doc When the delta interval is arrived, bind it with the existing object.
%%      If the object does not exist, declare it.
%%
-spec receive_delta(store(), {delta_send, value(), function(), function()} |
                             {delta_ack, id(), node(), non_neg_integer()}) ->
    ok | error.
receive_delta(Store, {delta_send, {Id, Type, _Metadata, Deltas},
                      MetadataFunBind, MetadataFunDeclare}) ->
    case do(get, [Store, Id]) of
        {ok, _Object} ->
            {ok, _Result} = bind(Id, Deltas, MetadataFunBind, Store);
        {error, not_found} ->
            {ok, _Result} = declare(Id, Type, MetadataFunDeclare, Store),
            receive_delta(Store, {delta_send, {Id, Type, _Metadata, Deltas},
                                  MetadataFunBind, MetadataFunDeclare})
    end,
    ok;
%% @doc When the delta ack is arrived with the counter, store it in the ack map.
%%
receive_delta(Store, {delta_ack, Id, From, Counter}) ->
    case do(get, [Store, Id]) of
        {ok, #dv{delta_ack_map=AckMap0}=Object} ->
            OldAck = case orddict:find(From, AckMap0) of
                         {ok, Ack0} ->
                             Ack0;
                         error ->
                             0
                     end,
            AckMap = orddict:store(From, max(OldAck, Counter), AckMap0),
            do(put, [Store, Id, Object#dv{delta_ack_map=AckMap}]);
        _ ->
            error
    end.

%% Internal functions.

%% @private
%% @doc Send responses to waiting threads, via messages.
%%
%%      Perform the following operations:
%%
%%      * Reply to all waiting threads via message.
%%      * Perform binding of any variables which are partially bound.
%%      * Mark variable as bound.
%%      * Check thresholds and send notifications, if required.
%%
-spec write(type(), value(), id(), store()) -> ok.
write(Type, Value, Key, Store) ->
    {ok, #dv{metadata=Metadata, waiting_threads=WT}} = do(get, [Store, Key]),
    {ok, StillWaiting} = reply_to_all(WT, [], {ok, {Key, Type, Metadata, Value}}),
    V1 = #dv{type=Type, value=Value, waiting_threads=StillWaiting},
    ok = do(put, [Store, Key, V1]),
    ok.

%% @private
increment_counter(Counter) ->
    Counter + 1.

-ifdef(TEST).

do(Function, Args) ->
    Backend = lasp_ets_storage_backend,
    erlang:apply(Backend, Function, Args).

-else.

%% @doc Execute call to the proper backend.
do(Function, Args) ->
    Backend = application:get_env(?APP,
                                  storage_backend,
                                  lasp_dets_storage_backend),
    erlang:apply(Backend, Function, Args).


-endif.
