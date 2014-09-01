%% @doc Derflow ETS single-node core executor.

-module(derflow_ets).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-include("derflow.hrl").

-export([is_det/2,
         bind/3,
         declare/1,
         declare/2,
         declare/3,
         thread/4]).

-export([threshold_met/3,
         is_lattice/1]).

declare(Store) ->
    declare(undefined, Store).

declare(Type, Store) ->
    declare(druuid:v4(), Type, Store).

declare(Id, Type, Store) ->
    Record = case Type of
        undefined ->
            #dv{value=undefined, type=undefined, bound=false};
        Type ->
            #dv{value=Type:new(), type=Type, bound=true}
    end,
    true = ets:insert(Store, {Id, Record}),
    {ok, Id}.

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
    lager:info("Value is: ~p NextKey is: ~p", [Value, NextKey]),
    case Bound of
        true ->
            case V#dv.value of
                Value ->
                    {ok, NextKey};
                _ ->
                    case is_lattice(Type) of
                        true ->
                            write(Type, Value, NextKey, Id, Store),
                            {ok, NextKey};
                        false ->
                            lager:warning("Attempt to bind failed: ~p ~p ~p",
                                          [Type, Value0, Value]),
                            error
                    end
            end;
        false ->
            write(Type, Value, NextKey, Id, Store),
            {ok, NextKey}
    end.

is_det(Id, Store) ->
    [{_Key, #dv{bound=Bound}}] = ets:lookup(Store, Id),
    {ok, Bound}.

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
threshold_met(_, Value, {greater, Threshold}) ->
    Threshold < Value;
threshold_met(_, Value, Threshold) ->
    Threshold =< Value.

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
    %% {ok, StillWaiting} = reply_to_all(Threads, [], {ok, Value, Next}),
    V1 = #dv{type=Type, value=Value, next=Next,
             lazy=Lazy, bound=true, waiting_threads=Threads},
    true = ets:insert(Store, {Key, V1}),
    %% notify_all(BindingList, Value),
    ok.
