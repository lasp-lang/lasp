-module(derflow_program).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-export([execute/0]).

execute() ->
    {ok, Id} = derflow:declare(),

    ok = derflow:bind(Id, 1),
    lager:info("Successful bind."),

    {ok, Value1} = derflow:read(Id),
    lager:info("Value1: ~p", [Value1]),

    error = derflow:bind(Id, 2),
    lager:info("Unsuccessful bind."),

    {ok, Value2} = derflow:read(Id),
    lager:info("Value2: ~p", [Value2]),

    {ok, Value1, Value2}.
