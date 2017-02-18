#!/usr/bin/env escript

%%! -pa _build/default/lib/types/ebin/

-define(OPS, 1000000).

main(_) ->
    SetFun = fun() ->
                    lists:foldl(fun(_X, Acc) ->
                                        {ok, State} = state_gset:mutate({add, self()}, self(), Acc),
                                        State
                                end, state_gset:new(), lists:seq(1, ?OPS))
              end,
    {SetTime, _} = timer:tc(SetFun),
    io:format("G-Set Time: ~p~n", [SetTime]),

    BooleanFun = fun() ->
                    lists:foldl(fun(_X, Acc) ->
                                        {ok, State} = state_boolean:mutate(true, self(), Acc),
                                        State
                                end, state_boolean:new(), lists:seq(1, ?OPS))
              end,
    {BooleanTime, _} = timer:tc(BooleanFun),
    io:format("Boolean Time: ~p~n", [BooleanTime]),

    CounterFun = fun() ->
                    lists:foldl(fun(_X, Acc) ->
                                        {ok, State} = state_gcounter:mutate(increment, self(), Acc),
                                        State
                                end, state_gcounter:new(), lists:seq(1, ?OPS))
              end,
    {CounterTime, _} = timer:tc(CounterFun),
    io:format("G-Counter Time: ~p~n", [CounterTime]),

    ok.
