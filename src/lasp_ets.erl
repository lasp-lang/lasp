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

-module(lasp_ets).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-include("lasp.hrl").

-behaviour(lasp_backend).

%% Core API.
-export([bind/3,
         bind_to/3,
         read/2,
         read/3,
         next/2,
         declare/1,
         declare/2,
         declare/3,
         thread/4,
         filter/4,
         wait_needed/2,
         wait_needed/3,
         reply_to_all/2,
         reply_to_all/3]).

%% Exported functions for vnode integration, where callback behavior is
%% dynamic.
-export([next/3,
         bind/5,
         bind_to/5,
         notify_value/3,
         notify_value/4,
         wait_needed/6,
         read/6,
         fetch/4,
         reply_fetch/4,
         write/6,
         filter/6,
         fetch/8]).

%% Exported helper functions.
-export([fold/6,
         fold_harness/8]).

%% Exported utility functions.
-export([next_key/3,
         notify_all/3,
         write/5]).

%% @doc Given an identifier, return the next identifier.
%%
%%      Given `Id', return the next identifier to create a stream of
%%      variables.
%%
-spec next(id(), store()) -> {ok, id()}.
next(Id, Store) ->
    DeclareNextFun = fun(Type) ->
            declare(Type, Store)
    end,
    next(Id, Store, DeclareNextFun).

%% @doc Filter values from one lattice into another.
%%
%%      Applies the given `Function' as a filter over the items in `Id',
%%      placing the result in `AccId', both of which need to be declared
%%      variables.
%%
-spec filter(id(), function(), id(), store()) -> {ok, pid()}.
filter(Id, Function, AccId, Store) ->
    BindFun = fun(_AccId, AccValue, _Variables) ->
            ?MODULE:bind(_AccId, AccValue, _Variables)
    end,
    ReadFun = fun(_Id, _Threshold, _Variables) ->
            ?MODULE:read(_Id, _Threshold, _Variables)
    end,
    filter(Id, Function, AccId, Store, BindFun, ReadFun).

%% @doc Perform a read for a particular identifier.
%%
%%      Given an `Id', perform a blocking read until the variable is
%%      bound.
%%
-spec read(id(), store()) -> {ok, type(), value(), id()}.
read(Id, Store) ->
    read(Id, {strict, undefined}, Store).

%% @doc Perform a monotonic read read for a particular identifier.
%%
%%      Given an `Id', perform a blocking read until the variable is
%%      bound.
%%
%%      This operation blocks until `Threshold' has been reached,
%%      reading from the provided {@link ets:new/2} store, `Store'.
%%
-spec read(id(), value(), store()) -> {ok, type(), value(), id()}.
read(Id, Threshold, Store) ->
    Self = self(),
    ReplyFun = fun(Type, Value, Next) ->
                    {ok, Type, Value, Next}
               end,
    BlockingFun = fun() ->
                        receive
                            X ->
                                lager:info("Value: ~p", [X]),
                                X
                        end
                 end,
    read(Id, Threshold, Store, Self, ReplyFun, BlockingFun).

%% @doc Declare a dataflow variable in a provided by identifer {@link
%%      ets:new/2} `Store'.
%%
-spec declare(store()) -> {ok, id()}.
declare(Store) ->
    declare(lasp_ivar, Store).

%% @doc Declare a dataflow variable, as a given type, in a provided by
%%      identifer {@link ets:new/2} `Store'.
%%
-spec declare(type(), store()) -> {ok, id()}.
declare(Type, Store) ->
    declare(druuid:v4(), Type, Store).

%% @doc Declare a dataflow variable in a provided by identifer {@link
%%      ets:new/2} `Store' with a given `Type'.
%%
-spec declare(id(), type(), store()) -> {ok, id()}.
declare(Id, Type, Store) ->
    Record = #dv{value=Type:new(), type=Type},
    true = ets:insert(Store, {Id, Record}),
    {ok, Id}.

%% @doc Define a dataflow variable to be bound to another dataflow
%%      variable.
%%
-spec bind_to(id(), id(), store()) -> {ok, id()} | {error, not_implemented}.
bind_to(Id, TheirId, Store) ->
    FromPid = self(),
    FetchFun = fun(_TheirId, _Id, _FromPid) ->
                    ?MODULE:fetch(_TheirId, _Id, _FromPid, Store)
               end,
    bind_to(Id, TheirId, Store, FetchFun, FromPid).

%% @doc Define a dataflow variable to be bound to a value.
%%
-spec bind(id(), value(), store()) -> {ok, id()}.
bind(Id, Value, Store) ->
    NextKeyFun = fun(Type, Next) ->
                        case Value of
                            undefined ->
                                undefined;
                            _ ->
                                next_key(Next, Type, Store)
                        end
                 end,
    NotifyFun = fun(_Id, NewValue) ->
                        ?MODULE:notify_value(_Id, NewValue, Store)
                end,
    bind(Id, Value, Store, NextKeyFun, NotifyFun).

%% @doc Spawn a function.
%%
%%      Spawn a process executing `Module:Function(Args)'.
%%
-spec thread(module(), func(), args(), store()) -> {ok, pid()}.
thread(Module, Function, Args, _Store) ->
    Fun = fun() -> erlang:apply(Module, Function, Args) end,
    Pid = spawn(Fun),
    {ok, Pid}.

%% Internal functions

%% @doc Declare next key, if undefined.  This function assumes that the
%%      next key will be declared in the local store.
%%
-spec next_key(undefined | id(), type(), store()) -> id().
next_key(undefined, Type, Store) ->
    {ok, NextKey} = declare(druuid:v4(), Type, Store),
    NextKey;
next_key(NextKey0, _, _) ->
    NextKey0.

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
%%      This operation blocks until `Threshold' has been requested,
%%      reading from the provided {@link ets:new/2} store, `Store'.
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

%% @doc Callback fetch function used in binding variables together.
%%
%%      Given a source, target, and a store, follow a series of links
%%      and retrieve the value for the original object.
%%
-spec fetch(id(), id(), pid(), store()) -> {ok, id()}.
fetch(TargetId, FromId, FromPid, Store) ->
    ResponseFun = fun() ->
                        receive
                            X ->
                                X
                        end
                  end,
    FetchFun = fun(_TargetId, _FromId, _FromPid) ->
            ?MODULE:fetch(_TargetId, _FromId, _FromPid, Store)
    end,
    ReplyFetchFun = fun(_FromId, _FromPid, _DV) ->
            ?MODULE:reply_fetch(_FromId, _FromPid, _DV, Store)
    end,
    NextKeyFun = fun(Next, Type) ->
            ?MODULE:next_key(Next, Type, Store)
    end,
    fetch(TargetId, FromId, FromPid, Store, ResponseFun, FetchFun,
          ReplyFetchFun, NextKeyFun).

%% @doc Callback function for replying to a bound variable request.
%%
%%      When responding to a local fetch, respond back to the waiting
%%      process with the response.
%%
-spec reply_fetch(id(), pid(), #dv{}, store()) -> {ok, id()}.
reply_fetch(FromId, FromPid,
            #dv{next=Next, type=Type, value=Value}, Store) ->
    NotifyFun = fun(Id, NewValue) ->
                        ?MODULE:notify_value(Id, NewValue, Store)
                end,
    ?MODULE:write(Type, Value, Next, FromId, Store, NotifyFun),
    {ok, _} = ?BACKEND:reply_to_all([FromPid], {ok, Next}),
    {ok, Next}.

%% @doc Define a dataflow variable to be bound a value.
%%
%%      Similar to {@link bind/3}.
%%
%%      `NextKeyFun' is used to determine how to generate the next
%%      identifier -- this is abstracted because in some settings this
%%      next key may be located in the local store, when running code at
%%      the replica, or located in a remote store, when running the code
%%      at the client.
%%
%%      `NotifyFun' is used to notify anything in the binding list that
%%      an update has occurred.
%%
-spec bind(id(), value(), store(), function(), function()) -> {ok, id()}.
bind(Id, Value, Store, NextKeyFun, NotifyFun) ->
    [{_Key, V=#dv{next=Next,
                  type=Type,
                  value=Value0}}] = ets:lookup(Store, Id),
    NextKey = NextKeyFun(Type, Next),
    case V#dv.value of
        Value ->
            {ok, NextKey};
        _ ->
            %% Merge may throw for invalid merge-types.
            try
                Merged = Type:merge(V#dv.value, Value),
                case lasp_lattice:is_inflation(Type, V#dv.value, Merged) of
                    true ->
                        write(Type, Merged, NextKey, Id, Store,
                              NotifyFun),
                        {ok, NextKey};
                    false ->
                        lager:error("Bind failed: ~p ~p ~p~n",
                                    [Type, Value0, Value]),
                        error
                end
            catch
                _:_ ->
                    lager:error("Bind failed: ~p ~p ~p~n",
                                [Type, Value0, Value]),
                    error
            end
    end.

%% @doc Given an identifier, return the next identifier.
%%
%%      Given `Id', return the next identifier to create a stream of
%%      variables.
%%
%%      Allow for an override function for performing the generation of
%%      the next identifier, using `DeclareNextFun'.
%%
-spec next(id(), store(), function()) -> {ok, id()}.
next(Id, Store, DeclareNextFun) ->
    [{_Key, V=#dv{next=NextKey0}}] = ets:lookup(Store, Id),
    case NextKey0 of
        undefined ->
            {ok, NextKey} = DeclareNextFun(V#dv.type),
            true = ets:insert(Store, {Id, V#dv{next=NextKey}}),
            {ok, NextKey};
        _ ->
            {ok, NextKey0}
    end.

%% @doc Perform a read (or monotonic read) for a particular identifier.
%%
%%      Given an `Id', perform a blocking read until the variable is
%%      bound.
%%
%%      Perform a read -- reads will either block until the `Threshold'
%%      is met, or the variable is bound.  Reads will be performed
%%      against the `Store' provided, which should be the identifier of
%%      an {@link ets:new/2} table.  When the process should register
%%      itself for notification of the variable being bound, it should
%%      supply the process identifier for notifications as `Self'.
%%      Finally, the `ReplyFun' and `BlockingFun' functions will be
%%      executed in the event that the reply is available immediately,
%%      or it will have to wait for the notification, in the event the
%%      variable is unbound or has not met the threshold yet.
%%
-spec read(id(), value(), store(), pid(), function(), function()) ->
    {ok, type(), value(), id()}.
read(Id, Threshold, Store, Self, ReplyFun, BlockingFun) ->
    [{_Key, V=#dv{value=Value,
                  lazy_threads=LazyThreads,
                  type=Type}}] = ets:lookup(Store, Id),
    lager:info("Read received: ~p, bound: ~p, threshold: ~p",
               [Id, V, Threshold]),

    %% Notify all lazy processes of this read.
    lager:info("Notifying lazy processes: ~p", [LazyThreads]),
    {ok, StillLazy} = reply_to_all(LazyThreads, {ok, Threshold}),
    lager:info("Done notifying: ~p", [StillLazy]),

    %% Satisfy read if threshold is met.
    case lasp_lattice:threshold_met(Type, Value, Threshold) of
        true ->
            lager:info("Replying with result: ~p",
                       [Value]),
            ReplyFun(Type, Value, V#dv.next);
        false ->
            lager:info("Threshold not met: ~p ~p ~p",
                       [Type, Value, Threshold]),
            WT = lists:append(V#dv.waiting_threads,
                              [{threshold,
                                read,
                                Self,
                                Type,
                                Threshold}]),
            true = ets:insert(Store,
                              {Id,
                               V#dv{waiting_threads=WT,
                                    lazy_threads=StillLazy}}),
            BlockingFun()
    end.

%% @doc Fetch is responsible for retreiving the value of a
%%      partially-bound variable from the ultimate source, if it's
%%      bound.
%%
%%      When running on a local replica, this is straightforward -- it¬
%%      can look the value up direclty up from the {@link ets:new/2}
%%      table supplied, as seen in the {@link bind/3} function.  Fetch
%%      is specifically used when we need to look in a series of tables,
%%      each running in a different process at a different node.
%%
%%      The function is implemented here, given it relies on knowing the
%%      structure of the table and is ets specific.  However, it's
%%      primarily used by the distribution code located in the virtual
%%      node.
%%
%%      Because of this, a number of functions need to be exported to
%%      support this behavior.
%%
%%      `ResponseFun' is a function responsible for the return value, as
%%      required by the virtual node which will call into this.
%%
%%      `FetchFun' is a function responsible for retreiving the value on
%%      another node, if the value is only partially bound here --
%%      consider the case where X is bound to Y which is bound to Z -- a
%%      fetch on Z requires transitive fetches through Y to X.
%%
%%      `ReplyFetchFun' is a function responsible for sending the
%%      response back to the originating fetch node.
%%
%%      `NextKeyFun' is responsible for generating the next identifier
%%      for use in building a stream from this partially-bound variable.
%%
-spec fetch(id(), id(), pid(), store(), function(), function(),
            function(), function()) -> term().
fetch(TargetId, FromId, FromPid, Store,
      ResponseFun, FetchFun, ReplyFetchFun, NextKeyFun) ->
    [{_, DV=#dv{binding=Binding}}] = ets:lookup(Store, TargetId),
    case Binding of
        undefined ->
            NextKey = NextKeyFun(DV#dv.next, DV#dv.type),
            BindingList = lists:append(DV#dv.binding_list, [FromId]),
            lager:info("Updating binding_list to: ~p for ~p",
                       [BindingList, TargetId]),
            DV1 = DV#dv{binding_list=BindingList, next=NextKey},
            true = ets:insert(Store, {TargetId, DV1}),
            ReplyFetchFun(FromId, FromPid, DV1),
            ResponseFun();
        BindId ->
            FetchFun(BindId, FromId, FromPid),
            ResponseFun()
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
-spec bind_to(id(), value(), store(), function(), pid()) -> any().
bind_to(Id, TheirId, Store, FetchFun, FromPid) ->
    true = ets:insert(Store, {Id, #dv{binding=TheirId}}),
    FetchFun(TheirId, Id, FromPid).

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
    FolderFun = fun({Type, {add, Element} = Op}, Acc) ->
            case Function(Element) of
                true ->
                    {ok, NewAcc} = Type:update(Op, undefined, Acc),
                    NewAcc;
                _ ->
                    Acc
            end
    end,
    fold(Store, Id, FolderFun, AccId, BindFun, ReadFun).

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
%%      This operation blocks until `Threshold' has been requested,
%%      reading from the provided {@link ets:new/2} store, `Store'.
%%
-spec wait_needed(id(), threshold(), store(), pid(), function(),
                  function()) -> {ok, threshold()}.
wait_needed(Id, Threshold, Store, Self, ReplyFun, BlockingFun) ->
    lager:info("Wait needed issued for identifier: ~p", [Id]),
    [{_Key, V=#dv{waiting_threads=WT,
                  type=Type,
                  lazy_threads=LazyThreads0}}] = ets:lookup(Store, Id),
    case WT of
        [_H|_T] ->
            ReplyFun(undefined);
        _ ->
            LazyThreads = case Threshold of
                            undefined ->
                                lists:append(LazyThreads0, [Self]);
                            Threshold ->
                                lists:append(LazyThreads0,
                                            [{threshold,
                                              wait,
                                              Self,
                                              Type,
                                              Threshold}])
            end,
            true = ets:insert(Store,
                              {Id, V#dv{lazy_threads=LazyThreads}}),
            BlockingFun()
    end.

%% @doc Notify a value of a change, and write changed value.
%%
%%      Given the local write might need to trigger another series of
%%      notifications, `NotifyFun' is used to specify how this process
%%      should be done.
%%
-spec notify_value(id(), value(), store(), function()) -> ok.
notify_value(Id, Value, Store, NotifyFun) ->
    [{_, #dv{next=Next, type=Type}}] = ets:lookup(Store, Id),
    write(Type, Value, Next, Id, Store, NotifyFun).

notify_value(Id, Value, Store) ->
    NotifyFun = fun(_Id, NewValue) ->
                        ?MODULE:notify_value(_Id, NewValue, Store)
                end,
    notify_value(Id, Value, Store, NotifyFun).

%% @doc Notify a series of variables of bind.
%%
-spec notify_all(function(), list(#dv{}), value()) -> ok.
notify_all(NotifyFun, [H|T], Value) ->
    NotifyFun(H, Value),
    notify_all(NotifyFun, T, Value);
notify_all(_, [], _) ->
    ok.

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
-spec reply_to_all(list(pid() | pending_threshold()), list(pending_threshold()),
                   term()) -> {ok, list(pending_threshold())}.
reply_to_all([{threshold, read, From, Type, Threshold}=H|T],
             StillWaiting0,
             {ok, Type, Value, Next}=Result) ->
    lager:info("Result: ~p, Read threshold: ~p", [Result, Threshold]),
    StillWaiting = case lasp_lattice:threshold_met(Type, Value, Threshold) of
        true ->
            lager:info("Read threshold ~p met: ~p", [Threshold, Value]),
            case From of
                {server, undefined, {Address, Ref}} ->
                    gen_server:reply({Address, Ref}, {ok, Type, Value, Next});
                _ ->
                    From ! Result
            end,
            StillWaiting0;
        false ->
            lager:info("Read threshold ~p NOT met: ~p", [Threshold, Value]),
            StillWaiting0 ++ [H]
    end,
    reply_to_all(T, StillWaiting, Result);
reply_to_all([{threshold, wait, From, Type, Threshold}=H|T],
             StillWaiting0,
             {ok, ReadThreshold}=Result) ->
    StillWaiting = case lasp_lattice:threshold_met(Type, Threshold, ReadThreshold) of
        true ->
            lager:info("Wait threshold ~p met: ~p", [Threshold, ReadThreshold]),
            case From of
                {server, undefined, {Address, Ref}} ->
                    gen_server:reply({Address, Ref}, {ok, ReadThreshold});
                _ ->
                    From ! Result
            end,
            StillWaiting0;
        false ->
            lager:info("Wait threshold ~p NOT met: ~p", [Threshold, ReadThreshold]),
            StillWaiting0 ++ [H]
    end,
    reply_to_all(T, StillWaiting, Result);
reply_to_all([From|T], StillWaiting, Result) ->
    case From of
        {server, undefined, {Address, Ref}} ->
            gen_server:reply({Address, Ref}, Result);
        _ ->
            From ! Result
    end,
    reply_to_all(T, StillWaiting, Result);
reply_to_all([], StillWaiting, _Result) ->
    {ok, StillWaiting}.

%% Internal functions.

-spec fold(store(), id(), function(), id(), fun(), function()) -> {ok, pid()}.
fold(Variables, Id, Function, AccId, BindFun, ReadFun) ->
    Pid = spawn_link(?MODULE, fold_harness, [
                Variables,
                Id,
                Function,
                AccId,
                BindFun,
                ReadFun,
                undefined,
                undefined]),
    {ok, Pid}.

-spec fold_harness(store(), id(), function(), id(), function(),
                   function(), value(), value()) -> function().
fold_harness(Variables, Id, Function, AccId, BindFun, ReadFun,
             PreviousValue, _PreviousAccValue) ->
    %% Blocking threshold read on source value.
    {ok, Type, Value, _} = ReadFun(Id, {strict, PreviousValue}, Variables),

    %% Generate operations for given data type.
    {ok, Operations} = lasp_lattice:generate_operations(Type, Value),
    lager:info("Operations generated: ~p", [Operations]),

    %% Build new data structure.
    AccValue = lists:foldl(Function, Type:new(), Operations),

    %% Update with result.
    BindFun(AccId, AccValue, Variables),

    fold_harness(Variables, Id, Function, AccId, BindFun, ReadFun, Value, AccValue).

%% @doc Send responses to waiting threads, via messages.
%%
%%      Perform the following operations:
%%
%%      * Reply to all waiting threads via message.
%%      * Perform binding of any variables which are partially bound.
%%      * Mark variable as bound.
%%      * Check thresholds and send notifications, if required.
%%
%%      When `NotifyFun' is supplied, override the function used to
%%      notify when a value is written -- this is required when talking
%%      to other tables in the system which are not the local table.
%%
-spec write(type(), value(), id(), id(), store()) -> ok.
write(Type, Value, Next, Key, Store) ->
    NotifyFun = fun(Id, NewValue) ->
                        [{_, #dv{next=Next,
                                 type=Type}}] = ets:lookup(Store, Id),
                        write(Type, NewValue, Next, Id, Store)
                end,
    write(Type, Value, Next, Key, Store, NotifyFun).

%% @doc Send responses to waiting threads, via messages.
%%
%%      Similar to {@link write/5}.
%%
%%      When `NotifyFun' is supplied, override the function used to
%%      notify when a value is written -- this is required when talking
%%      to other tables in the system which are not the local table.
%%
-spec write(type(), value(), id(), id(), store(), function()) -> ok.
write(Type, Value, Next, Key, Store, NotifyFun) ->
    lager:info("Writing key: ~p next: ~p", [Key, Next]),
    [{_Key, #dv{waiting_threads=Threads,
                binding_list=BindingList,
                binding=Binding}}] = ets:lookup(Store, Key),
    lager:info("Waiting threads are: ~p", [Threads]),
    lager:info("Binding list is: ~p", [BindingList]),
    {ok, StillWaiting} = reply_to_all(Threads, [], {ok, Type, Value, Next}),
    V1 = #dv{type=Type,
             value=Value,
             next=Next,
             binding=Binding,
             binding_list=BindingList,
             waiting_threads=StillWaiting},
    true = ets:insert(Store, {Key, V1}),
    notify_all(NotifyFun, BindingList, Value),
    ok.
