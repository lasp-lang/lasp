%% @doc Test that a bind works on a distributed cluster of nodes.

-module(derflow_bind_test).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-export([test/0]).

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
    ?assertEqual({ok, 1, 1}, rpc:call(Node, ?MODULE, test, [])),

    lager:info("Done!"),

    pass.

-endif.

test() ->
    {ok, Id} = derflow:declare(),

    {ok, _} = derflow:bind(Id, 1),
    lager:info("Successful bind."),

    {ok, Value1, _} = derflow:read(Id),
    lager:info("Value1: ~p", [Value1]),

    error = derflow:bind(Id, 2),
    lager:info("Unsuccessful bind."),

    {ok, Value2, _} = derflow:read(Id),
    lager:info("Value2: ~p", [Value2]),

    {ok, Value1, Value2}.
