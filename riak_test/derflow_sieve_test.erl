%% @doc Test sieve example.

-module(derflow_sieve_test).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-export([test/1,
         sieve/2,
         filter/3,
         generate/3]).

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
    Result = rpc:call(Node, derflow_sieve_test, test, [100]),
    ?assertEqual([2,3,5,7,11,13,17,19,23,29,31,37,41,43,47,53,59,
                 61,67,71,73,79,83,89,97], Result),
    pass.

-endif.

test(Max) ->
    {ok, S1} = derflow:declare(),
    derflow:thread(derflow_sieve_test, generate, [2, Max, S1]),
    {ok, S2} = derflow:declare(),
    derflow:thread(derflow_sieve_test, sieve, [S1, S2]),
    derflow:get_stream(S2).

sieve(S1, S2) ->
    case derflow:read(S1) of
        {ok, nil, _} ->
            derflow:bind(S2, nil);
        {ok, Value, Next} ->
            {ok, SN} = derflow:declare(),
            derflow:thread(derflow_sieve_test, filter,
                           [Next, fun(Y) -> Y rem Value =/= 0 end, SN]),
            {ok, NextOutput} = derflow:bind(S2, Value),
            sieve(SN, NextOutput)
    end.

filter(S1, F, S2) ->
    case derflow:read(S1) of
        {ok, nil, _} ->
            derflow:bind(S2, nil);
        {ok, Value, Next} ->
            case F(Value) of
                false ->
                    filter(Next, F, S2);
                true->
                    {ok, NextOutput} = derflow:bind(S2, Value),
                    filter(Next, F, NextOutput)
            end
    end.

generate(Init, N, Output) ->
    if
        (Init =< N) ->
            timer:sleep(250),
            {ok, Next} = derflow:bind(Output, Init),
            generate(Init + 1, N,  Next);
        true ->
            derflow:bind(Output, nil)
    end.
