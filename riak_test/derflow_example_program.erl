-module(derflow_example_program).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-behavior(derflow_program).

-export([init/0,
         execute/1,
         execute/2,
         merge/1]).

init() ->
    {ok, riak_dt_gset:new()}.

execute(Acc) ->
    {ok, Acc}.

execute(Acc, _X) ->
    {ok, Id1} = derflow:declare(),

    {ok, _} = derflow:bind(Id1, 1),
    lager:info("Successful bind."),

    {ok, Value1, _} = derflow:read(Id1),
    lager:info("Value1: ~p", [Value1]),

    error = derflow:bind(Id1, 2),
    lager:info("Unsuccessful bind."),

    {ok, Value2, _} = derflow:read(Id1),
    lager:info("Value2: ~p", [Value2]),

    {ok, Acc}.

merge([Reply|_]) ->
    Reply.
