%% @doc Programs test.

-module(derflow_programs_test).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-export([test/1]).

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

    TestPaths = rt_config:get(test_paths, undefined),
    Program = hd(TestPaths) ++ "/../derflow_example_program.erl",
    lager:info("Program is: ~p", [Program]),

    lager:info("Remotely executing the test."),
    ?assertEqual([], rpc:call(Node, ?MODULE, test, [Program])),

    pass.

-endif.

test(Program) ->
    lager:info("Registering program from the test."),

    ok = derflow:register(derflow_example_program, Program, preflist),

    lager:info("Executing program from the test."),

    {ok, Result} = derflow:execute(derflow_example_program, preflist),

    Result.
