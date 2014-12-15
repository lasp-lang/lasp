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

-module(derflow_ets).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-include("derflow.hrl").

-export([is_det/2,
         bind/3,
         read/2,
         read/3,
         declare/1,
         declare/2,
         declare/3,
         thread/4,
         reply_to_all/2,
         reply_to_all/3]).

-export([threshold_met/3,
         is_lattice/1,
         is_inflation/3,
         generate_operations/2]).

%% @doc Perform a read of a particular identifier.
read(Id, Store) ->
    read(Id, undefined, Store).

%% @doc  Perform a threshold read of a particular identifier.
%% @todo Rewrite the derflow_vnode to use this read operation, and avoid
%%       the code duplication.
read(Id, Threshold, Store) ->
    [{_Key, V=#dv{value=Value,
                  bound=Bound,
                  creator=Creator,
                  lazy=Lazy,
                  type=Type}}] = ets:lookup(Store, Id),
    case Bound of
        true ->
            lager:info("Read received: ~p, bound: ~p, threshold: ~p",
                       [Id, V, Threshold]),
            case derflow_ets:is_lattice(Type) of
                true ->
                    case Threshold of
                        undefined ->
                            lager:info("No threshold specified: ~p",
                                       [Threshold]),
                            {ok, Value, V#dv.next};
                        _ ->
                            lager:info("Threshold specified: ~p",
                                       [Threshold]),
                            case derflow_ets:threshold_met(Type, Value, Threshold) of
                                true ->
                                    {ok, Value, V#dv.next};
                                false ->
                                    WT = lists:append(V#dv.waiting_threads,
                                                      [{threshold, self(), Type, Threshold}]),
                                    true = ets:insert(Store, {Id, V#dv{waiting_threads=WT}}),
                                    receive
                                        Value ->
                                            lager:info("Value: ~p", [Value]),
                                            Value
                                    end
                            end
                    end;
                false ->
                    {ok, Value, V#dv.next}
            end;
        false ->
            lager:info("Read received: ~p, unbound", [Id]),
            WT = lists:append(V#dv.waiting_threads, [self()]),
            true = ets:insert(Store, {Id, V#dv{waiting_threads=WT}}),
            case Lazy of
                true ->
                    {ok, _} = reply_to_all([Creator], ok),
                    receive
                        Value ->
                            lager:info("Value: ~p", [Value]),
                            Value
                    end;
                false ->
                    receive
                        Value ->
                            lager:info("Value: ~p", [Value]),
                            Value
                    end
            end
    end.

%% @doc Declare a dataflow variable.
declare(Store) ->
    declare(undefined, Store).

%% @doc Declare a dataflow variable of a given type.
declare(Type, Store) ->
    declare(druuid:v4(), Type, Store).

%% @doc Declare a dataflow variable of a given type with a given id.
declare(Id, Type, Store) ->
    Record = case Type of
        undefined ->
            #dv{value=undefined, type=undefined, bound=false};
        Type ->
            #dv{value=Type:new(), type=Type, bound=true}
    end,
    true = ets:insert(Store, {Id, Record}),
    {ok, Id}.

%% @doc  Define a dataflow variable to be bound to another or a value.
%% @todo Implement.
bind(_Id, {id, _DVId}, _Store) ->
    {error, not_implemented};

bind(Id, Value, Store) ->
    [{_Key, V=#dv{next=Next,
                  type=Type,
                  bound=Bound,
                  value=Value0}}] = ets:lookup(Store, Id),
    NextKey = case Value of
        undefined ->
            undefined;
        _ ->
            next_key(Next, Type, Store)
    end,
    case Bound of
        true ->
            case V#dv.value of
                Value ->
                    {ok, NextKey};
                _ ->
                    case is_lattice(Type) of
                        true ->
                            Merged = Type:merge(V#dv.value, Value),
                            case is_inflation(Type, V#dv.value, Merged) of
                                true ->
                                    write(Type, Merged, NextKey, Id, Store),
                                    {ok, NextKey};
                                false ->
                                    lager:error("Bind failed: ~p ~p ~p~n",
                                                [Type, Value0, Value]),
                                    error
                            end;
                        false ->
                            lager:error("Bind failed: ~p ~p ~p~n",
                                        [Type, Value0, Value]),
                            error
                    end
            end;
        false ->
            write(Type, Value, NextKey, Id, Store),
            {ok, NextKey}
    end.

%% @doc Return the binding status of a given dataflow variable.
is_det(Id, Store) ->
    [{_Key, #dv{bound=Bound}}] = ets:lookup(Store, Id),
    {ok, Bound}.

%% @doc Spawn a function.
thread(Module, Function, Args, _Store) ->
    Fun = fun() -> erlang:apply(Module, Function, Args) end,
    Pid = spawn(Fun),
    {ok, Pid}.

%% Internal functions

%% @doc Declare next key, if undefined.
next_key(undefined, Type, Store) ->
    {ok, NextKey} = declare(druuid:v4(), Type, Store),
    NextKey;
next_key(NextKey0, _, _) ->
    NextKey0.

%% @doc Determine if a threshold is met.
threshold_met(riak_dt_gset, Value, Threshold) ->
    is_inflation(riak_dt_gset, Threshold, Value);
threshold_met(riak_dt_gcounter, Value, Threshold) ->
    Threshold =< riak_dt_gcounter:value(Value).

%% @doc Determine if a change is an inflation or not.
is_inflation(Type, Previous, Current) ->
    is_lattice(Type) andalso is_lattice_inflation(Type, Previous, Current).

%% @doc Determine if a change is an inflation or not.
is_lattice_inflation(riak_dt_gcounter, undefined, _) ->
    true;
is_lattice_inflation(riak_dt_gcounter, Previous, Current) ->
    PreviousList = lists:sort(orddict:to_list(Previous)),
    CurrentList = lists:sort(orddict:to_list(Current)),
    lists:foldl(fun({Actor, Count}, Acc) ->
            case lists:keyfind(Actor, 1, CurrentList) of
                false ->
                    Acc andalso false;
                {_Actor1, Count1} ->
                    Acc andalso (Count =< Count1)
            end
            end, true, PreviousList);
is_lattice_inflation(riak_dt_gset, undefined, _) ->
    true;
is_lattice_inflation(riak_dt_gset, Previous, Current) ->
    sets:is_subset(
        sets:from_list(riak_dt_gset:value(Previous)),
        sets:from_list(riak_dt_gset:value(Current))).

%% @doc Return if something is a lattice or not.
is_lattice(Type) ->
    lists:member(Type, ?LATTICES).

%% @doc Send responses to waiting threads, via messages.
%%
%%      Perform three operations:
%%
%%      1. Reply to all waiting threads via message.
%%      2. Perform binding of any variables which are bound to just
%%         bound variable.
%%      3. Mark variable as bound.
%%
write(Type, Value, Next, Key, Store) ->
    lager:info("Writing key: ~p next: ~p", [Key, Next]),
    [{_Key, #dv{waiting_threads=Threads,
                binding_list=_BindingList,
                lazy=Lazy}}] = ets:lookup(Store, Key),
    lager:info("Waiting threads are: ~p", [Threads]),
    {ok, StillWaiting} = reply_to_all(Threads, [], {ok, Value, Next}),
    V1 = #dv{type=Type, value=Value, next=Next,
             lazy=Lazy, bound=true, waiting_threads=StillWaiting},
    true = ets:insert(Store, {Key, V1}),
    %% notify_all(BindingList, Value),
    ok.

%% @doc Given a group of processes which are blocking on reads, notify
%%      them of bound values or met thresholds.
reply_to_all(List, Result) ->
    reply_to_all(List, [], Result).

%% @doc Given a group of processes which are blocking on reads, notify
%%      them of bound values or met thresholds.
reply_to_all([{threshold, From, Type, Threshold}=H|T],
             StillWaiting0,
             {ok, Value, Next}=Result) ->
    lager:info("Result: ~p, Threshold: ~p", [Result, Threshold]),
    StillWaiting = case derflow_ets:threshold_met(Type, Value, Threshold) of
        true ->
            lager:info("Threshold ~p met: ~p", [Threshold, Value]),
            case From of
                {server, undefined, {Address, Ref}} ->
                    gen_server:reply({Address, Ref}, {ok, Value, Next});
                _ ->
                    From ! Result
            end,
            StillWaiting0;
        false ->
            lager:info("Threshold ~p NOT met: ~p", [Threshold, Value]),
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

%% @doc Given an object type from riak_dt; generate a series of
%%      operations for that type which are representative of a partial
%%      order of operations on this object yielding this state.
generate_operations(riak_dt_gset, Set) ->
    Values = riak_dt_gset:value(Set),
    {ok, [{add, Value} || Value <- Values]}.
