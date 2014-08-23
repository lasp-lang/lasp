%% @doc Helpers for remotely running derflow code.

-module(derflow_test_helpers).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-export([load/1]).

%% @doc Remotely load test code on a series of nodes.
load(Nodes) when is_list(Nodes) ->
    _ = [ok = load(Node) || Node <- Nodes],
    ok;
load(Node) ->
    TestGlob = rt_config:get(tests_to_remote_load, undefined),
    case TestGlob of
        undefined ->
            ok;
        TestGlob ->
            Tests = filelib:wildcard(TestGlob),
            lager:info("Found the following tests: ~p", [Tests]),
            [ok = remote_compile_and_load(Node, Test) || Test <- Tests],
            ok
    end.

%% @doc Remotely compile and load a test on a given node.
remote_compile_and_load(Node, F) ->
    lager:info("Compiling and loading file ~s on node ~s", [F, Node]),
    {ok, _, Bin} = rpc:call(Node, compile, file,
                            [F, [binary,
                                 {parse_transform, lager_transform}]]),
    ModName = list_to_atom(filename:basename(F, ".erl")),
    {module, _} = rpc:call(Node, code, load_binary, [ModName, F, Bin]),
    ok.
