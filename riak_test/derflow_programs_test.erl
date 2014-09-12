%% @doc Programs test.

-module(derflow_programs_test).
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

    lager:info("Remotely executing the test."),
    ?assertEqual([], rpc:call(Node, ?MODULE, test, [])),

    pass.

-endif.

test() ->
    lager:info("Registering program from the test."),

    ok = derflow:register(derflow_example_program,
                          "/Users/cmeiklejohn/SyncFree/derflow/riak_test/derflow_example_program.erl",
                         preflist),

    lager:info("Executing program from the test."),

    {ok, Result} = derflow:execute(derflow_example_program, preflist),

    Result.
