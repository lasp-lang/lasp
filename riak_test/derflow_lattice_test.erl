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
    ?assertEqual({ok, [[0],
                       [0,1],
                       [0,1,2],
                       [0,1,2,3],
                       [0,1,2,3,4],
                       [0,1,2,3,4,5],
                       [0,1,2,3,4,5,6],
                       [0,1,2,3,4,5,6,7],
                       [0,1,2,3,4,5,6,7,8],
                       [0,1,2,3,4,5,6,7,8,9]]}, rpc:call(Node, ?MODULE, test, [])),

    lager:info("Done!"),

    pass.

-endif.

test() ->
    %% Generate a stream of objects.
    {ok, ObjectStream} = derflow:declare(),
    derflow:thread(?MODULE, producer, [0, 10, ObjectStream]),

    %% Accumulate the objects into a set.
    {ok, ObjectSetStream} = derflow:declare(),
    {ok, ObjectSetId} = derflow:declare(riak_dt_gset),
    ObjectSetFun = fun(X) ->
            lager:info("~p set received: ~p", [self(), X]),
            {ok, Set0} = derflow:read(ObjectSetId),
            {ok, Set} = riak_dt_gset:update({add, X}, undefined, Set0),
            ok = derflow:bind(ObjectSetId, Set),
            lager:info("~p set bound to new set: ~p", [self(), Set]),
            Set
    end,
    derflow:thread(?MODULE, consumer,
                   [ObjectStream, ObjectSetFun, ObjectSetStream]),

    %% Block until all operations are complete, to ensure we don't shut
    %% the test harness down until everything is computed.
    ObjectSetStreamValues = derflow:get_stream(ObjectSetStream),
    lager:info("Retrieving set stream: ~p",
               [ObjectSetStreamValues]),

    {ok, ObjectSetStreamValues}.

%% @doc Stream producer, which generates a series of inputs on a stream.
producer(Init, N, Output) ->
    case N > 0 of
        true ->
            timer:sleep(1000),
            {ok, Next} = derflow:produce(Output, Init),
            producer(Init + 1, N-1,  Next);
        false ->
            derflow:bind(Output, undefined)
    end.

%% @doc Stream consumer, which accepts inputs on one stream, applies a
%%      function, and then produces inputs on another stream.
consumer(S1, F, S2) ->
    case derflow:consume(S1) of
        {ok, undefined, _} ->
            lager:info("~p consumed: ~p", [self(), undefined]),
            derflow:bind(S2, undefined);
        {ok, Value, Next} ->
            lager:info("~p consumed: ~p", [self(), Value]),
            Me = self(),
            spawn(fun() -> Me ! F(Value) end),
            NewValue = receive
                X ->
                    X
            end,
            {ok, NextOutput} = derflow:produce(S2, NewValue),
            consumer(Next, F, NextOutput)
    end.
