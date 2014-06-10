-module(prod_cons).
-export([test1/0, producer/3, consumer/3]).

test1() ->
    {id, S1}=derflow:declare(),
    derflow:thread(prod_cons,producer,[0,10,S1]),
    %derflow:thread(derflow,async_print_stream,[S1]).
    {id, S2}=derflow:declare(),
    derflow:thread(prod_cons,consumer,[S1,fun(X) -> X + 5 end,S2]),
    derflow:async_print_stream(S2).
    %L = derflow:get_stream(S2),
    %L.
    %io:format("Output: ~w~n",[L]).
    %S2.

producer(Init, N, Output) ->
    if (N>0) ->
	timer:sleep(1000),
	{id, Next} = derflow:bind(Output, Init),
	producer(Init + 1, N-1,  Next);
    true ->
	derflow:bind(Output, nil)
    end.

consumer(S1, F, S2) ->
    case derflow:read(S1) of
	{nil, _} ->
	    derflow:bind(S2, nil);
	{Value, Next} ->
	    {id, NextOutput} = derflow:bind(S2, F, Value),
	    consumer(Next, F, NextOutput)
    end.
