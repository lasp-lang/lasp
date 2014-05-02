-module(bounded_buffer).
-export([test1/0, test2/0, producer/3, buffer/3, consumer/2]).

test1() ->
    {id, S1}=derflowdis:declare(),
    derflowdis:thread(bounded_buffer,producer,[0,10,S1]),
    {id, S2}=derflowdis:declare(),
    derflowdis:thread(bounded_buffer,buffer, [S1,2,S2]),
    consumer(S2, fun(X) -> X*2 end).

test2() ->
    {id, S1} = derflowdis:lazyDeclare(),
    derflowdis:thread(bounded_buffer,producer,[0,5,S1]),
    consumer(S1, fun(X) -> X*2 end).

producer(Init, N, Output) ->
    if (N>0) ->
        derflowdis:waitNeeded(Output),
	{id,Next} = derflowdis:bind(Output,Init),
	io:format("Prod:Bound for ~w next ~w Produced~w ~n",[Output, Next, 11-N]),
        producer(Init + 1, N-1,  Next);
    true ->
        derflowdis:bind(Output, nil)
    end.

loop(S1, S2, End) ->
    derflowdis:waitNeeded(S2),
    {Value1, S1Next} = derflowdis:read(S1),
    {id, S2Next} = derflowdis:bind(S2, Value1),
    io:format("Buff:Bound for consumer ~w-> ~w ~w~n",[S1,S2,Value1]),
    case derflowdis:read(End) of {nil, _} ->
	loop(S1Next, S2Next, End);
	{_V,EndNext} ->
       loop(S1Next, S2Next, EndNext)    
    end.

buffer(S1, Size, S2) ->
    End = drop_list(S1, Size),
    io:format("Buff:End of list ~w ~n",[End]),
    loop(S1, S2, End).

drop_list(S, Size) ->
    if Size == 0 ->
	S;
      true ->
       	{_Value,Next}=derflowdis:read(S),
    	drop_list(Next, Size-1)
    end.

consumer(S2,F) ->
    case derflowdis:read(S2) of
	{nil, _} ->
	   io:format("Cons:Reading end~n");
	{Value, Next} ->
	   io:format("Cons:Id ~w Consume ~w, Get ~w, Next~w ~n",[S2,Value, F(Value),Next]),
	   %timer:sleep(1000),
	   consumer(Next, F)
    end.


