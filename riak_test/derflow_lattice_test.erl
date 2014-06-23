%% @doc Lattice test.

-module(derflow_lattice_test).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-export([test/0,
         producer/3,
         consumer/3]).

-ifdef(TEST).

-export([confirm/0]).

-define(HARNESS, (rt_config:get(rt_harness))).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    [Nodes] = rt:build_clusters([1]),
    lager:info("Nodes: ~p", [Nodes]),
    Node = hd(Nodes),

    lager:info("Remotely loading code on node ~p", [Node]),
    ok = derflow_test_helpers:load(Nodes),
    lager:info("Remote code loading complete."),

    lager:info("Remotely executing the test."),
    rpc:call(Node, ?MODULE, test, []),

    pass.

-endif.

test() ->
    %% Generate a stream of objects.
    {ok, ObjectStream} = derflow:declare(),
    spawn(?MODULE, producer, [0, 10, ObjectStream]),

    %% Accumulate the objects into a set.
    {ok, ObjectSetStream} = derflow:declare(),
    {ok, ObjectSet} = derflow:declare(riak_dt_gset),
    ObjectSetFun = fun(X) ->
            lager:info("~p set received: ~p", [self(), X]),
            {ok, Set0} = derflow:read(ObjectSet),
            {ok, Set} = riak_dt_gset:update({add, X}, undefined, Set0),
            ok = derflow:bind(ObjectSet, Set),
            lager:info("~p set bound to new set: ~p", [self(), Set]),
            Set
    end,
    spawn(?MODULE, consumer,
          [ObjectStream, ObjectSetFun, ObjectSetStream]),

    %% Accumulate set into a counter.
    {ok, ObjectCounterStream} = derflow:declare(),
    {ok, ObjectCounter} = derflow:declare(riak_dt_gcounter),
    ObjectCounterFun = fun(X) ->
            lager:info("~p counter received: ~p", [self(), X]),
            {ok, Counter0} = derflow:read(ObjectCounter),
            Delta = length(X) - riak_dt_gcounter:value(Counter0),
            {ok, Counter} = riak_dt_gcounter:update({increment, Delta},
                                                    ObjectCounterStream,
                                                    Counter0),
            ok = derflow:bind(ObjectCounter, Counter),
            lager:info("~p counter bound to new counter: ~p",
                       [self(), riak_dt_gcounter:value(Counter)]),
            X
    end,
    spawn(?MODULE, consumer,
          [ObjectSetStream, ObjectCounterFun, ObjectCounterStream]),

    %% Block until all operations are complete, to ensure we don't shut
    %% the test harness down until everything is computed.
    _ = derflow:get_stream(ObjectSetStream),
    _ = derflow:get_stream(ObjectCounterStream).

%% @doc Stream producer, which generates a series of inputs on a stream.
producer(Init, N, Output) ->
    case N > 0 of
        true ->
            timer:sleep(1000),
            {ok, Next} = derflow:produce(Output, Init),
            producer(Init + 1, N-1,  Next);
        false ->
            derflow:bind(Output, nil)
    end.

%% @doc Stream consumer, which accepts inputs on one stream, applies a
%%      function, and then produces inputs on another stream.
consumer(S1, F, S2) ->
    case derflow:consume(S1) of
        {ok, nil, _} ->
            lager:info("~p consumed: ~p", [self(), nil]),
            derflow:bind(S2, nil);
        {ok, Value, Next} ->
            lager:info("~p consumed: ~p", [self(), Value]),
            {ok, NextOutput} = derflow:produce(S2, F(Value)),
            consumer(Next, F, NextOutput)
    end.
