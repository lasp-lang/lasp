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
         wait_needed/6,
         read/6,
         write/4,
         filter/6,
         map/6,
         product/7,
         union/7,
         intersection/7,
         fold/6]).

%% Erlang 17.
-ifdef(namespaced_types).
-type lasp_dict() :: dict:dict().
-else.
-type lasp_dict() :: dict().
-endif.

%% Exported helper functions.
-export([notify/3]).

-record(read, {id :: id(),
               type :: type(),
               value :: value(),
               read_fun :: function()}).

%% @doc Initialize the backend.
-spec start(atom()) -> {ok, store()} | {error, atom()}.
start(Identifier) ->
    ?BACKEND:start(Identifier).

%% @doc Filter values from one lattice into another.
%%
%%      Applies the given `Function' as a filter over the items in `Id',
%%      placing the result in `AccId', both of which need to be declared
%%      variables.
%%
-spec filter(id(), function(), id(), store()) -> ok.
filter(Id, Function, AccId, Store) ->
    BindFun = fun(_AccId, AccValue, _Variables) ->
            ?MODULE:bind(_AccId, AccValue, _Variables)
    end,
    ReadFun = fun(_Id, _Threshold, _Variables) ->
            ?MODULE:read(_Id, _Threshold, _Variables)
    end,
    filter(Id, Function, AccId, Store, BindFun, ReadFun).

%% @doc Fold values from one lattice into another.
%%
%%      Applies the given `Function' as a fold over the items in `Id',
%%      placing the result in `AccId', both of which need to be declared
%%      variables.
%%
-spec fold(id(), function(), id(), store()) -> ok.
fold(Id, Function, AccId, Store) ->
    BindFun = fun(_AccId, AccValue, _Variables) ->
            ?MODULE:bind(_AccId, AccValue, _Variables)
    end,
    ReadFun = fun(_Id, _Threshold, _Variables) ->
            ?MODULE:read(_Id, _Threshold, _Variables)
    end,
    fold(Id, Function, AccId, Store, BindFun, ReadFun).

%% @doc Map values from one lattice into another.
%%
%%      Applies the given `Function' as a map over the items in `Id',
%%      placing the result in `AccId', both of which need to be declared
%%      variables.
%%
-spec map(id(), function(), id(), store()) -> ok.
map(Id, Function, AccId, Store) ->
    BindFun = fun(_AccId, AccValue, _Variables) ->
            ?MODULE:bind(_AccId, AccValue, _Variables)
    end,
    ReadFun = fun(_Id, _Threshold, _Variables) ->
            ?MODULE:read(_Id, _Threshold, _Variables)
    end,
    map(Id, Function, AccId, Store, BindFun, ReadFun).

%% @doc Compute the intersection of two sets.
%%
%%      Computes the intersection of two sets and bind the result
%%      to a third.
%%
-spec intersection(id(), id(), id(), store()) -> ok.
intersection(Left, Right, Intersection, Store) ->
    BindFun = fun(_AccId, AccValue, _Variables) ->
            ?MODULE:bind(_AccId, AccValue, _Variables)
    end,
    ReadLeftFun = fun(_Left, _Threshold, _Variables) ->
            ?MODULE:read(_Left, _Threshold, _Variables)
    end,
    ReadRightFun = fun(_Right, _Threshold, _Variables) ->
            ?MODULE:read(_Right, _Threshold, _Variables)
    end,
    intersection(Left, Right, Intersection, Store, BindFun, ReadLeftFun, ReadRightFun).

%% @doc Compute the union of two sets.
%%
%%      Computes the union of two sets and bind the result
%%      to a third.
%%
-spec union(id(), id(), id(), store()) -> ok.
union(Left, Right, Union, Store) ->
    BindFun = fun(_AccId, AccValue, _Variables) ->
            ?MODULE:bind(_AccId, AccValue, _Variables)
    end,
    ReadLeftFun = fun(_Left, _Threshold, _Variables) ->
            ?MODULE:read(_Left, _Threshold, _Variables)
    end,
    ReadRightFun = fun(_Right, _Threshold, _Variables) ->
            ?MODULE:read(_Right, _Threshold, _Variables)
    end,
    union(Left, Right, Union, Store, BindFun, ReadLeftFun, ReadRightFun).

%% @doc Compute the cartesian product of two sets.
%%
%%      Computes the cartestian product of two sets and bind the result
%%      to a third.
%%
-spec product(id(), id(), id(), store()) -> ok.
product(Left, Right, Product, Store) ->
    BindFun = fun(_AccId, AccValue, _Variables) ->
            ?MODULE:bind(_AccId, AccValue, _Variables)
    end,
    ReadLeftFun = fun(_Left, _Threshold, _Variables) ->
            ?MODULE:read(_Left, _Threshold, _Variables)
    end,
    ReadRightFun = fun(_Right, _Threshold, _Variables) ->
            ?MODULE:read(_Right, _Threshold, _Variables)
    end,
    product(Left, Right, Product, Store, BindFun, ReadLeftFun,
            ReadRightFun).

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
    ReplyFun = fun(_Id, Type, Value) -> {ok, {_Id, Type, Value}} end,
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
    case ?BACKEND:get(Store, Id) of
        {ok, _} ->
            %% Do nothing; make declare idempotent at each replica.
            {ok, Id};
        _ ->
            Value = Type:new(),
            ok = ?BACKEND:put(Store, Id, #dv{value=Value, type=Type}),
            {ok, Id}
    end.

%% @doc Define a dataflow variable to be bound to another dataflow
%%      variable.
%%
-spec bind_to(id(), id(), store()) -> ok.
bind_to(Id, TheirId, Store) ->
    BindFun = fun(_AccId, _AccValue, _Variables) ->
            ?MODULE:bind(_AccId, _AccValue, _Variables)
    end,
    bind_to(Id, TheirId, Store, BindFun).

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
    {ok, #dv{value=Value0, type=Type}} = ?BACKEND:get(Store, Id),
    {ok, Value} = Type:update(Operation, Actor, Value0),
    bind(Id, Value, Store).

%% @doc Define a dataflow variable to be bound a value.
%%
-spec bind(id(), value(), store()) -> {ok, var()}.
bind(Id, Value, Store) ->
    {ok, #dv{value=Value0, type=Type}} = ?BACKEND:get(Store, Id),
    case Value0 of
        Value ->
            {ok, {Id, Type, Value}};
        _ ->
            %% Merge may throw for invalid types.
            try
                Merged = Type:merge(Value0, Value),
                case lasp_lattice:is_inflation(Type, Value0, Merged) of
                    true ->
                        write(Type, Merged, Id, Store),
                        {ok, {Id, Type, Value}};
                    false ->
                        {ok, {Id, Type, Value}}
                end
            catch
                _:_Reason ->
                    {ok, {Id, Type, Value}}
            end
    end.

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
    {ok, V=#dv{value=Value,
               lazy_threads=LazyThreads,
               type=Type}} = ?BACKEND:get(Store, Id),

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
    {ok, StillLazy} = reply_to_all(LazyThreads, {ok, Threshold}),

    %% Satisfy read if threshold is met.
    case lasp_lattice:threshold_met(Type, Value, Threshold) of
        true ->
            ReplyFun(Id, Type, Value);
        false ->
            WT = lists:append(V#dv.waiting_threads,
                              [{threshold,
                                read,
                                Self,
                                Type,
                                Threshold}]),
            ok = ?BACKEND:put(Store, Id, V#dv{waiting_threads=WT, lazy_threads=StillLazy}),
            BlockingFun()
    end.

%% @doc Perform a read (or monotonic read) for a series of particular
%%      identifiers.
%%
-spec read_any([{id(), value()}], pid(), store()) ->
    {ok, var()} | {ok, not_available_yet}.
read_any(Reads, Self, Store) ->
    Found = lists:foldl(fun({Id, Threshold0}, AlreadyFound) ->
       case AlreadyFound of
           false ->
               {ok, V=#dv{value=Value,
                          lazy_threads=LazyThreads,
                          type=Type}} = ?BACKEND:get(Store, Id),

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
               {ok, StillLazy} = reply_to_all(LazyThreads,
                                              {ok, Threshold}),

               %% Satisfy read if threshold is met.
               case lasp_lattice:threshold_met(Type,
                                               Value,
                                               Threshold) of
                   true ->
                       {Id, Type, Value};
                   false ->
                       WT = lists:append(V#dv.waiting_threads,
                                         [{threshold,
                                           read,
                                           Self,
                                           Type,
                                           Threshold}]),
                       ok = ?BACKEND:put(Store, Id, V#dv{waiting_threads=WT, lazy_threads=StillLazy}),
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
               {ok, Value}
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
-spec bind_to(id(), id(), store(), function()) -> ok.
bind_to(AccId, Id, Store, BindFun) ->
    Fun = fun(Scope) ->
        %% Read current value from the scope.
        #read{value=Value} = dict:fetch(Id, Scope),

        %% Bind new value back.
        {ok, _} = BindFun(AccId, Value, Store)
    end,
    notify(Store, [{Id, fun() -> 1 end}], Fun).

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
-spec fold(id(), function(), id(), store(), function(), function()) -> ok.
fold(Id, Function, AccId, Store, BindFun, ReadFun) ->
    Fun = fun(Scope) ->
        %% Read current value from the scope.
        #read{type=Type, value=Value} = dict:fetch(Id, Scope),

        %% Iterator to map the data structure over.
        FolderFun = fun(Element, Acc) ->
                Values = case Element of
                    {X, Causality} ->
                        %% riak_dt_orset | lasp_orset
                        [{V, Causality} || V <- Function(X)];
                    X ->
                        %% riak_dt_gset
                        Function(X)
                end,

                Acc ++ Values
        end,

        %% Apply change.
        AccValue = lists:foldl(FolderFun, Type:new(), Value),

        %% Bind new value back.
        {ok, _} = BindFun(AccId, AccValue, Store)

    end,
    notify(Store, [{Id, ReadFun}], Fun).

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
              function()) -> ok.
product(Left, Right, AccId, Store, BindFun, ReadLeftFun, ReadRightFun) ->
    Fun = fun(Scope) ->
        %% Read current value from the scope.
        #read{type=Type, value=LValue} = dict:fetch(Left, Scope),
        #read{type=_Type, value=RValue} = dict:fetch(Right, Scope),

        case {LValue, RValue} of
            {undefined, _} ->
                ok;
            {_, undefined} ->
                ok;
            {_, _} ->
                %% Iterator to map the data structure over.
                FolderFun = fun(Element, Acc) ->
                        Values = case Element of
                            {X, XCausality} ->
                                %% riak_dt_orset | lasp_orset
                                [{{X, Y}, lasp_lattice:orset_causal_product(XCausality, YCausality)}
                                 || {Y, YCausality} <- RValue];
                            X ->
                                %% riak_dt_gset
                                [{X, Y} || Y  <- RValue]
                        end,

                        Acc ++ Values
                end,

                %% Apply change.
                AccValue = lists:foldl(FolderFun, Type:new(), LValue),

                %% Bind new value back.
                {ok, _} = BindFun(AccId, AccValue, Store)
        end
    end,
    notify(Store, [{Left, ReadLeftFun}, {Right, ReadRightFun}], Fun).

%% @doc Compute the intersection of two sets.
%%
%%      Computes the intersection of two sets and bind the result
%%      to a third.
%%
%%      Similar to {@link intersection/4}, however, provides an override
%%      function for the `BindFun', which is responsible for binding the
%%      result, for instance, when it's located in another table.
%%
-spec intersection(id(), id(), id(), store(), function(), function(), function()) -> ok.
intersection(Left, Right, AccId, Store, BindFun, ReadLeftFun, ReadRightFun) ->
    Fun = fun(Scope) ->
        %% Read current value from the scope.
        #read{type=Type, value=LValue} = dict:fetch(Left, Scope),
        #read{type=_Type, value=RValue} = dict:fetch(Right, Scope),

        case {LValue, RValue} of
            {undefined, _} ->
                ok;
            {_, undefined} ->
                ok;
            {_, _} ->
                %% Iterator to map the data structure over.
                FolderFun = fun(Element, Acc) ->
                        Values = case Element of
                            {X, XCausality} ->
                                %% riak_dt_orset | lasp_orset
                                case lists:keyfind(X, 1, RValue) of
                                    {_Y, YCausality} ->
                                        [{X, lasp_lattice:orset_causal_union(XCausality, YCausality)}];
                                    false ->
                                        []
                                end;
                            X ->
                                %% riak_dt_gset
                                case lists:member(X, RValue) of
                                    true ->
                                        [X];
                                    false ->
                                        []
                                end
                        end,

                        Acc ++ Values
                end,

                %% Apply change.
                AccValue = lists:foldl(FolderFun, Type:new(), LValue),

                %% Bind new value back.
                {ok, _} = BindFun(AccId, AccValue, Store)
        end
    end,
    notify(Store, [{Left, ReadLeftFun}, {Right, ReadRightFun}], Fun).

%% @doc Compute the union of two sets.
%%
%%      Computes the union of two sets and bind the result
%%      to a third.
%%
%%      Similar to {@link union/4}, however, provides an override
%%      function for the `BindFun', which is responsible for binding the
%%      result, for instance, when it's located in another table.
%%
-spec union(id(), id(), id(), store(), function(), function(), function()) -> ok.
union(Left, Right, AccId, Store, BindFun, ReadLeftFun, ReadRightFun) ->
    Fun = fun(Scope) ->
        %% Read current value from the scope.
        %% @TODO: assume for the time being source and destination are
        %% the same type.
        #read{type=Type, value=LValue} = dict:fetch(Left, Scope),
        #read{value=RValue} = dict:fetch(Right, Scope),

        case {LValue, RValue} of
            {undefined, _} ->
                ok;
            {_, undefined} ->
                ok;
            {_, _} ->
                AccValue = case Type of
                    lasp_orset ->
                        orddict:merge(fun(_Key, L, _R) -> L end, LValue, RValue);
                    riak_dt_orset ->
                        orddict:merge(fun(_Key, L, _R) -> L end, LValue, RValue);
                    riak_dt_gset ->
                        LValue ++ RValue
                end,

                %% Bind new value back.
                {ok, _} = BindFun(AccId, AccValue, Store)
        end
    end,
    notify(Store, [{Left, ReadLeftFun}, {Right, ReadRightFun}], Fun).

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
-spec map(id(), function(), id(), store(), function(), function()) -> ok.
map(Id, Function, AccId, Store, BindFun, ReadFun) ->
    Fun = fun(Scope) ->
        %% Read current value from the scope.
        #read{type=Type, value=Value} = dict:fetch(Id, Scope),

        %% Iterator to map the data structure over.
        FolderFun = fun(Element, Acc) ->
                V = case Element of
                    {X, Causality} ->
                        %% riak_dt_orset | lasp_orset
                        {Function(X), Causality};
                    X ->
                        %% riak_dt_gset
                        Function(X)
                end,

                Acc ++ [V]
        end,

        %% Apply change.
        AccValue = lists:foldl(FolderFun, Type:new(), Value),

        %% Bind new value back.
        {ok, _} = BindFun(AccId, AccValue, Store)

    end,
    notify(Store, [{Id, ReadFun}], Fun).

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
-spec filter(id(), function(), id(), store(), function(), function()) -> ok.
filter(Id, Function, AccId, Store, BindFun, ReadFun) ->
    Fun = fun(Scope) ->
        %% Read current value from the scope.
        #read{type=Type, value=Value} = dict:fetch(Id, Scope),

        %% Iterator to map the data structure over.
        FolderFun = fun(Element, Acc) ->
                V = case Element of
                    {X, _} ->
                        %% riak_dt_orset | lasp_orset
                        X;
                    X ->
                        %% riak_dt_gset
                        X
                end,

                case Function(V) of
                    true ->
                        Acc ++ [Element];
                    _ ->
                        Acc
                end
        end,

        %% Apply change.
        AccValue = lists:foldl(FolderFun, Type:new(), Value),

        %% Bind new value back.
        {ok, _} = BindFun(AccId, AccValue, Store)

    end,
    notify(Store, [{Id, ReadFun}], Fun).

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
    {ok, V=#dv{waiting_threads=WT,
               type=Type,
               value=Value,
               lazy_threads=LazyThreads0}} = ?BACKEND:get(Store, Id),
    case lasp_lattice:threshold_met(Type, Value, Threshold) of
        true ->
            ReplyFun(Threshold);
        false ->
            case WT of
                [_H|_T] ->
                    ReplyFun(Threshold);
                _ ->
                    LazyThreads = case Threshold of
                                    undefined ->
                                        lists:append(LazyThreads0,
                                                     [Self]);
                                    Threshold ->
                                        lists:append(LazyThreads0,
                                                    [{threshold,
                                                      wait,
                                                      Self,
                                                      Type,
                                                      Threshold}])
                    end,
                    ok = ?BACKEND:put(Store, Id, V#dv{lazy_threads=LazyThreads}),
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
        _ ->
            From ! Result
    end,
    reply_to_all(T, StillWaiting, Result);
reply_to_all([], StillWaiting, _Result) ->
    {ok, StillWaiting}.

%% Internal functions.

-spec notify(store(), [{id(), function()}] | lasp_dict(), function()) -> ok.
notify(Variables, Reads, Function) when is_list(Reads) ->
    Scope = dict:from_list([{Id, #read{id=Id, read_fun=Fun}}
                            || {Id, Fun} <- Reads]),
    spawn_link(?MODULE, notify, [Variables, Scope, Function]),
    ok;

notify(Variables, Scope0, Function) ->
    %% Perform a read against all variables in the dict.
    Reads = [{Id, {strict, Value}} ||
             {Id, #read{value=Value}} <- dict:to_list(Scope0)],

    %% Wait for one of the variables to be modified.
    {ok, {Id, Type, Value}} = lasp:read_any(Reads),

    %% Store updated value in the dict.
    ReadRecord = dict:fetch(Id, Scope0),
    Scope = dict:store(Id, ReadRecord#read{value=Value, type=Type}, Scope0),

    %% Apply function with updated scope.
    Function(Scope),

    notify(Variables, Scope, Function).

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
    {ok, #dv{waiting_threads=WT}} = ?BACKEND:get(Store, Key),
    {ok, StillWaiting} = reply_to_all(WT, [], {ok, {Key, Type, Value}}),
    V1 = #dv{type=Type, value=Value, waiting_threads=StillWaiting},
    ok = ?BACKEND:put(Store, Key, V1),
    ok.
