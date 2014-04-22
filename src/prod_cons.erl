-module(prod_cons).
-export([test1/0, producer/3, consumer/3]).

test1() ->
    {id, S1}=derflow:declare(),
    derflowdis:thread(prod_cons,producer,[0,10,S1]),
    {id, S2}=derflowdis:declare(),
    derflowdis:thread(prod_cons,consumer,[S1,fun(X) -> X + 5 end,S2]),
    derflowdis:async_print_stream(S2).
    %L = derflowdis:get_stream(S2),
    %L.
    %io:format("Output: ~w~n",[L]).
    %S2.

producer(Init, N, Output) ->
    if (N>0) ->
	timer:sleep(1000),
	{id, Next} = derflowdis:bind(Output, Init),
	producer(Init + 1, N-1,  Next);
    true ->
	derflowdis:bind(Output, nil)
    end.

consumer(S1, F, S2) ->
    case derflowdis:read(S1) of
	{nil, _} ->
	    derflowdis:bind(S2, nil);
	{Value, Next} ->
	    {id, NextOutput} = derflowdis:bind(S2, F, Value),
	    consumer(Next, F, NextOutput)
    end.
