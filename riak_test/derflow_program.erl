-module(derflow_program).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-export([execute/0]).

execute() ->
    {ok, Id1} = derflow:declare(),

    {ok, Id2} = derflow:declare(),

    ok = derflow:bind(Id1, 1),
    lager:info("Successful bind."),

    ok = derflow:bind(Id2, {id, Id1}),
    lager:info("Successful bind to variable."),

    {ok, Value1} = derflow:read(Id1),
    lager:info("Value1: ~p", [Value1]),

    error = derflow:bind(Id1, 2),
    lager:info("Unsuccessful bind."),

    {ok, Value2} = derflow:read(Id1),
    lager:info("Value2: ~p", [Value2]),

    {ok, Value3} = derflow:read(Id2),
    lager:info("Value3: ~p", [Value3]),

    {ok, Value1, Value2, Value3}.
