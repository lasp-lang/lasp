-module(derflow_program).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-export([execute/0,
         merge/1]).

execute() ->
    {ok, Id1} = derflow:declare(),

    {ok, _} = derflow:bind(Id1, 1),
    lager:info("Successful bind."),

    {ok, Value1, _} = derflow:read(Id1),
    lager:info("Value1: ~p", [Value1]),

    error = derflow:bind(Id1, 2),
    lager:info("Unsuccessful bind."),

    {ok, Value2, _} = derflow:read(Id1),
    lager:info("Value2: ~p", [Value2]),

    {ok, Value1, Value2}.

merge([Reply|_]) ->
    Reply.
