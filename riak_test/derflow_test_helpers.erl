%% @doc Helpers for remotely running derflow code.

-module(derflow_test_helpers).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-export([load/1,
         declare/1,
         thread/4,
         read/2,
         bind/3]).

%% @doc Remotely load test code on a series of nodes.
load(Nodes) when is_list(Nodes) ->
    _ = [ok = load(Node) || Node <- Nodes],
    ok;
load(Node) ->
    TestGlob = "/Users/cmeiklejohn/SyncFree/derflow/riak_test/*.erl",
    Tests = filelib:wildcard(TestGlob),
    lager:info("Found the following tests: ~p", [Tests]),
    [ok = remote_compile_and_load(Node, Test) || Test <- Tests],
    ok.

%% @doc Remotely compile and load a test on a given node.
remote_compile_and_load(Node, F) ->
    lager:info("Compiling and loading file ~s on node ~s", [F, Node]),
    {ok, _, Bin} = rpc:call(Node, compile, file, [F, [binary]]),
    ModName = list_to_atom(filename:basename(F, ".erl")),
    {module, _} = rpc:call(Node, code, load_binary, [ModName, F, Bin]),
    ok.

%% @doc Remotely declare a dataflow variable at a given node.
declare(Node) ->
    rpc:call(Node, derflow, declare, []).

%% @doc Remotely bind a dataflow variable at a given node.
bind(Node, Id, Value) ->
    rpc:call(Node, derflow, bind, [Id, Value]).

%% @doc Remotely read a dataflow variable at a given node.
read(Node, Id) ->
    rpc:call(Node, derflow, read, [Id]).

%% @doc Remotely thread a dataflow function to a variable.
thread(Node, Module, Function, Args) ->
    rpc:call(Node, Module, Function, Args).
