%% @doc Test that a bind works on a distributed cluster of nodes.

-module(derflow_bind_test).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-ifdef(TEST).

-export([confirm/0]).

-define(HARNESS, (rt_config:get(rt_harness))).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    [Nodes] = rt:build_clusters([1]),
    lager:info("Nodes: ~p", [Nodes]),

    Node = hd(Nodes),

    {ok, Id} = derflow_test_helpers:declare(Node),

    ok = derflow_test_helpers:bind(Node, Id, 1),
    lager:info("Successful bind"),

    {ok, Value} = derflow_test_helpers:read(Node, Id),
    lager:info("Value: ~p", [Value]),

    ?assertEqual(1, Value),

    pass.

-endif.
