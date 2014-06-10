-module(derflow_bind_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    [Nodes] = rt:build_clusters([3]),
    lager:info("Nodes: ~p", [Nodes]),

    Node = hd(Nodes),

    {ok, Id} = rpc:call(Node, derflow, declare, []),

    {ok, NextId} = rpc:call(Node, derflow, bind, [Id, 1]),
    lager:info("NextId: ~p", [NextId]),

    {ok, Value, NextId} = rpc:call(Node, derflow, read, [Id]),
    lager:info("Value: ~p", [Value]),

    ?assertEqual(1, Value),

    pass.
