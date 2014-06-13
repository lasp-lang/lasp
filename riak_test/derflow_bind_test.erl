-module(derflow_bind_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    [Nodes] = rt:build_clusters([3]),
    lager:info("Nodes: ~p", [Nodes]),

    Node = hd(Nodes),

    {ok, Id} = derflow_helpers:declare(Node),

    {ok, NextId} = derflow_helpers:bind(Node, Id, 1),
    lager:info("NextId: ~p", [NextId]),

    {ok, Value, NextId} = derflow_helpers:read(Node, Id),
    lager:info("Value: ~p", [Value]),

    ?assertEqual(1, Value),

    pass.
