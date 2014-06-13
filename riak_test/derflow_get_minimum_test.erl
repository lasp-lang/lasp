%% @doc Test that streaming bind of an insertion sort works.

-module(derflow_get_minimum_test).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-export([insort/3,
         insert/4]).

-define(TABLE, minimum).

-ifdef(TEST).

-export([confirm/0]).

-define(HARNESS, (rt_config:get(rt_harness))).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    [Nodes] = rt:build_clusters([1]), %% TODO: changeme
    lager:info("Nodes: ~p", [Nodes]),
    Node = hd(Nodes),

    lager:info("Remotely loading code on node ~p", [Node]),
    ok = derflow_test_helpers:load(Nodes),
    lager:info("Remote code loading complete."),

    lager:info("Declaring new dataflow variable."),
    {ok, S1} = derflow_test_helpers:declare(Node),

    Pids = rpc:call(Node, erlang, processes, []),
    EtsOwner = hd(Pids),

    lager:info("Creating ets table on node ~p owned by pid ~p", [Node, EtsOwner]),
    EtsOptions = [set, named_table, public,
                  {write_concurrency, true}, {heir, EtsOwner, undefined}],
    ?TABLE = rpc:call(Node, ets, new, [?TABLE, EtsOptions]),

    true = rpc:call(Node, ets, insert, [?TABLE, {count, 0}]),

    derflow_test_helpers:thread(Node,
                                derflow_get_minimum_test,
                                insort, [Node, [1,2,3,4], S1]),

    {ok, Value} = derflow_test_helpers:read(Node, S1),
    lager:info("Retrieved value: ~p", [Value]),

    pass.

-endif.

insert(Node, X, In, Out) ->
    [{Id, C}] = rpc:call(Node, ets, lookup, [?TABLE, count]),
    true = rpc:call(Node, ets, insert, [?TABLE, {Id, C+1}]),
    ok = derflow:wait_needed(Out),
    case derflow:read(In) of
        {ok, nil, _} ->
            {id, Next} = derflow:bind(Out, X),
            derflow:bind(Next, nil);
        {ok, V, SNext} ->
            if
                X < V ->
                    {id, Next} = derflow:bind(Out, X),
                    derflow:bind(Next, In);
                true ->
                    {id,Next} = derflow:bind(Out,V),
                    insert(Node, X, SNext, Next)
            end
    end.

insort(Node, List, S) ->
    case List of
        [H|T] ->
            {ok, OutS} = derflow:declare(),
            insort(Node, T, OutS),
            derflow:thread(derflow_get_minimum_test, insert, [Node, H, OutS, S]);
        [] ->
            derflow:bind(S, nil)
    end.
