
% -------------------------------------------------------------------
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
         bind_to/3,
         read/2,
         read/3,
         read_any/2,
         declare/1,
         declare/2,
         declare/3,
         update/4,
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
         reply_to_all/3]).

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
-spec start(atom()) -> {ok, store()} | {error, atom()}.
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
    ReplyFun = fun({_Id, Type, Value}) -> {ok, {_Id, Type, Value}} end,
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
        {ok, {Id, Type, Value}} ->
            {ok, {Id, Type, Value}}
    end.

%% @doc Declare a dataflow variable in a provided by identifer.
-spec declare(store()) -> {ok, id()}.
declare(Store) ->
    declare(lasp_ivar, Store).

%% @doc Declare a dataflow variable, as a given type.
-spec declare(type(), store()) -> {ok, id()}.
declare(Type, Store) ->
    declare(druuid:v4(), Type, Store).

%% @doc Declare a dataflow variable in a provided by identifer.
-spec declare(id(), type(), store()) -> {ok, id()}.
declare(Id, Type, Store) ->
    case do(get, [Store, Id]) of
        {ok, _} ->
            %% Do nothing; make declare idempotent at each replica.
            {ok, Id};
        _ ->
            Value = Type:new(),
            ok = do(put, [Store, Id, #dv{value=Value, type=Type}]),
            {ok, Id}
    end.

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
    {ok, #dv{value=Value0, type=Type}} = do(get, [Store, Id]),
    {ok, Value} = Type:update(Operation, Actor, Value0),
    bind(Id, Value, Store).

%% @doc Define a dataflow variable to be bound a value.
%%
-spec bind(id(), value(), store()) -> {ok, var()}.
bind(Id, Value, Store) ->
    Mutator = fun(#dv{type=Type, value=Value0, waiting_threads=WT}=Object) ->
            case Value0 of
                Value ->
                    %% Bind to current value.
                    {Object, {ok, {Id, Type, Value}}};
                _ ->
                    %% Merge may throw for invalid types.
                    try
                        Merged = Type:merge(Value0, Value),
                        case lasp_lattice:is_inflation(Type, Value0, Merged) of
                            true ->
                                {ok, SW} = reply_to_all(WT, [], {ok, {Id, Type, Merged}}),
                                NewObject = #dv{type=Type, value=Merged, waiting_threads=SW},
                                {NewObject, {ok, {Id, Type, Merged}}};
                            false ->
                                %% Value is old.
                                {Object, {ok, {Id, Type, Value0}}}
                        end
                    catch
                        _:Reason ->
                            %% Merge threw.
                            lager:warning("Exception, reason: ~p ~p => ~p",
                                          [Reason, Value0, Value]),
                            {Object, {ok, {Id, Type, Value0}}}
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
    Mutator = fun(#dv{type=Type, value=Value, lazy_threads=LT}=Object) ->
            %% When no threshold is specified, use the bottom value for the
            %% given lattice.
            %%
            Threshold = case Threshold0 of
                undefined ->
                    Type:new();
                {strict, undefined} ->
                    {strict, Type:new()};
                Threshold0 ->
                    Threshold0
            end,

            %% Notify all lazy processes of this read.
            {ok, SL} = reply_to_all(LT, {ok, Threshold}),

            %% Satisfy read if threshold is met.
            case lasp_lattice:threshold_met(Type, Value, Threshold) of
                true ->
                    {Object#dv{lazy_threads=SL}, {ok, {Id, Type, Value}}};
                false ->
                    WT = lists:append(Object#dv.waiting_threads, [{threshold, read, Self, Type, Threshold}]),
                    {Object#dv{waiting_threads=WT, lazy_threads=SL}, error}
            end
    end,
    case do(update, [Store, Id, Mutator]) of
        {ok, {Id, Type, Value}} ->
            ReplyFun({Id, Type, Value});
        error ->
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
                            Mutator = fun(#dv{type=Type, value=Value, lazy_threads=LT}=Object) ->
                                    %% When no threshold is specified, use the bottom
                                    %% value for the given lattice.
                                    %%
                                    Threshold = case Threshold0 of
                                        undefined ->
                                            Type:new();
                                        {strict, undefined} ->
                                            {strict, Type:new()};
                                        Threshold0 ->
                                            Threshold0
                                    end,

                                    %% Notify all lazy processes of this read.
                                    {ok, SL} = reply_to_all(LT, {ok, Threshold}),

                                    %% Satisfy read if threshold is met.
                                    case lasp_lattice:threshold_met(Type, Value, Threshold) of
                                        true ->
                                            {Object, {ok, {Id, Type, Value}}};
                                        false ->
                                            WT = lists:append(Object#dv.waiting_threads, [{threshold, read, Self, Type, Threshold}]),
                                            {Object#dv{waiting_threads=WT, lazy_threads=SL}, error}
                                    end
                            end,

                            case do(update, [Store, Id, Mutator]) of
                                {ok, {Id, Type, Value}} ->
                                    {ok, {Id, Type, Value}};
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
    Fun = fun({_, _, V}) ->
        %% Bind new value back.
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
    Fun = fun({_, T, V}) ->
            AccValue = case T of
                lasp_orset_gbtree ->
                    FolderFun = fun(X, Causality, Acc) ->
                            lists:foldl(fun(Z, InnerAcc) ->
                                        gb_trees:enter(Z, Causality, InnerAcc)
                                end, Acc, Function(X))
                    end,
                    gb_trees_ext:foldl(FolderFun, T:new(), V);
                _ ->
                    FolderFun = fun(Element, Acc) ->
                            Values = case Element of
                                {X, Causality} ->
                                    %% lasp_orset
                                    [{Z, Causality} || Z <- Function(X)];
                                X ->
                                    %% lasp_gset
                                    Function(X)
                            end,

                            Acc ++ Values
                    end,
                    lists:foldl(FolderFun, T:new(), V)
            end,

            %% Bind new value back.
            {ok, _} = BindFun(AccId, AccValue, Store)
    end,
    gen_flow:start_link(lasp_process, [[{Id, ReadFun}], Fun]).

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
    Fun = fun({_, T, LValue}, {_, _, RValue}) ->
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
                        _ ->
                            FolderFun = fun(Element, Acc) ->
                                    Values = case Element of
                                        {X, XCausality} ->
                                            %% lasp_orset
                                            [{{X, Y}, lasp_lattice:causal_product(T, XCausality, YCausality)}
                                             || {Y, YCausality} <- RValue];
                                        X ->
                                            %% lasp_gset
                                            [{X, Y} || Y  <- RValue]
                                    end,

                                    Acc ++ Values
                            end,
                            lists:foldl(FolderFun, T:new(), LValue)
                    end,

                    %% Bind new value back.
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
    Fun = fun({_, T, LValue}, {_, _, RValue}) ->
            case {LValue, RValue} of
                {undefined, _} ->
                    ok;
                {_, undefined} ->
                    ok;
                {_, _} ->
                    AccValue = case T of
                        lasp_orset_gbtree ->
                            FolderFun = fun(X, XCausality, Acc) ->
                                    Value = case gb_trees:lookup(X, RValue) of
                                        {value, YCausality} ->
                                            lasp_lattice:causal_union(T, XCausality, YCausality);
                                        none ->
                                            Acc
                                    end,
                                    New = gb_trees:enter(X,
                                                         Value,
                                                         gb_trees:empty()),
                                    lasp_orset_gbtree:merge(New, Acc)
                            end,
                            gb_trees_ext:foldl(FolderFun,
                                               T:new(),
                                               LValue);
                        _ ->
                            FolderFun = fun(Element, Acc) ->
                                    Values = case Element of
                                        {X, XCausality} ->
                                            %% lasp_orset
                                            case lists:keyfind(X, 1, RValue) of
                                                {_Y, YCausality} ->
                                                    [{X, lasp_lattice:causal_union(T, XCausality, YCausality)}];
                                                false ->
                                                    []
                                            end;
                                        X ->
                                            %% lasp_gset
                                            case lists:member(X, RValue) of
                                                true ->
                                                    [X];
                                                false ->
                                                    []
                                            end
                                    end,

                                    Acc ++ Values
                            end,
                            lists:foldl(FolderFun, T:new(), LValue)
                    end,

                    %% Bind new value back.
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
    Fun = fun({_, T, LValue}, {_, _, RValue}) ->
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
                            lasp_orset:merge(LValue, RValue);
                        lasp_gset ->
                            lasp_gset:merge(LValue, RValue)
                    end,

                    %% Bind new value back.
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
    Fun = fun({_, T, V}) ->
            AccValue = case T of
                lasp_orset_gbtree ->
                    FolderFun = fun(X, Value, Acc) ->
                            New = gb_trees:enter(Function(X), Value, gb_trees:empty()),
                            lasp_orset_gbtree:merge(New, Acc)
                    end,
                    gb_trees_ext:foldl(FolderFun, T:new(), V);
                _ ->
                    FolderFun = fun(Element, Acc) ->
                            Z = case Element of
                                {X, Causality} ->
                                    %% lasp_orset
                                    {Function(X), Causality};
                                X ->
                                    %% lasp_gset
                                    Function(X)
                            end,

                            Acc ++ [Z]
                    end,
                    lists:foldl(FolderFun, T:new(), V)
            end,

            %% Bind new value back.
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
    Fun = fun({_, T, V}) ->
            AccValue = case T of
                lasp_orset_gbtree ->
                    %% Iterator to map the data structure over.
                    FolderFun = fun(X, Value, Acc) ->
                            case Function(X) of
                                true ->
                                    New = gb_trees:enter(X, Value, gb_trees:empty()),
                                    lasp_orset_gbtree:merge(New, Acc);
                                _ ->
                                    Acc
                            end
                    end,
                    gb_trees_ext:foldl(FolderFun, T:new(), V);
                _ ->
                    FolderFun = fun(Element, Acc) ->
                            Z = case Element of
                                {X, _} ->
                                    %% lasp_orset
                                    X;
                                X ->
                                    %% lasp_gset
                                    X
                            end,

                            case Function(Z) of
                                true ->
                                    Acc ++ [Element];
                                _ ->
                                    Acc
                            end
                    end,

                    %% Apply change.
                    lists:foldl(FolderFun, T:new(), V)
            end,

            %% Bind new value back.
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
             {ok, {Id, Type, Value}}=Result) ->
    SW = case lasp_lattice:threshold_met(Type, Value, Threshold) of
        true ->
            case From of
                {server, undefined, {Address, Ref}} ->
                    gen_server:reply({Address, Ref},
                                     {ok, {Id, Type, Value}});
                {fsm, undefined, Address} ->
                    gen_fsm:send_event(Address,
                                       {ok, undefined,
                                        {Id, Type, Value}});
                {Address, Ref} ->
                    gen_server:reply({Address, Ref},
                                     {ok, {Id, Type, Value}});
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
            From ! Result
    end,
    reply_to_all(T, StillWaiting, Result);
reply_to_all([], StillWaiting, _Result) ->
    {ok, StillWaiting}.

%% Internal functions.

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
    {ok, #dv{waiting_threads=WT}} = do(get, [Store, Key]),
    {ok, StillWaiting} = reply_to_all(WT, [], {ok, {Key, Type, Value}}),
    V1 = #dv{type=Type, value=Value, waiting_threads=StillWaiting},
    ok = do(put, [Store, Key, V1]),
    ok.

-ifndef(EQC).

%% @doc Execute call to the proper backend.
do(Function, Args) ->
    Backend = application:get_env(?APP,
                                  storage_backend,
                                  lasp_eleveldb_storage_backend),
    erlang:apply(Backend, Function, Args).

-else.

%% @doc Execute call to the proper backend.
do(Function, Args) ->
    Backend = lasp_ets_storage_backend,
    erlang:apply(Backend, Function, Args).

-endif.
