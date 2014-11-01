%% @doc Test that streaming bind of an insertion sort works.

-module(derflow_get_minimum_test).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-export([test/1,
         insort/2,
         insert/3]).

-define(TABLE, minimum).

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

    ok = derflow_test_helpers:wait_for_cluster(Nodes),

    lager:info("Remotely executing the test."),
    Result = rpc:call(Node, ?MODULE, test, [[1,2,3,4,5]]),
    ?assertEqual(1, Result),
    pass.

-endif.

test(List) ->
    {ok, S1} = derflow:declare(),
    ?TABLE = ets:new(?TABLE, [set, named_table, public, {write_concurrency, true}]),
    true = ets:insert(?TABLE, {count, 0}),
    spawn(derflow_get_minimum_test, insort, [List, S1]),
    {ok, V, _} = derflow:consume(S1),
    V.

insort(List, S) ->
    case List of
        [H|T] ->
            {ok, OutS} = derflow:declare(),
            insort(T, OutS),
            spawn(derflow_get_minimum_test, insert, [H, OutS, S]);
        [] ->
            derflow:bind(S, undefined)
    end.

insert(X, In, Out) ->
    [{Id, C}] = ets:lookup(?TABLE, count),
    true = ets:insert(?TABLE, {Id, C+1}),
    ok = derflow:wait_needed(Out),
    case derflow:consume(In) of
        {ok, undefined, _} ->
            {ok, Next} = derflow:produce(Out, X),
            derflow:bind(Next, undefined);
        {ok, V, SNext} ->
            if
                X < V ->
                    {ok, Next} = derflow:produce(Out, X),
                    derflow:bind(Next, In);
                true ->
                    {ok, Next} = derflow:produce(Out, V),
                    insert(X, SNext, Next)
            end
    end.
