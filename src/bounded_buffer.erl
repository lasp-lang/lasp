-module(bounded_buffer).
-export([test1/0,  producer/3, buffer/3, consumer/3]).

test1() ->
    {id, S1}=derflowdis:declare(),
    derflowdis:thread(bounded_buffer,producer,[0,10,S1]),
    {id, S2}=derflowdis:declare(),
    derflowdis:thread(bounded_buffer,buffer, [S1,2,S2]),
    consumer(S2, 5, fun(X) -> X*2 end).

producer(Value, N, Output) ->
    if (N>0) ->
        derflowdis:wait_needed(Output),
	{id,Next} = derflowdis:bind(Output, Value),
        producer(Value+1, N-1,  Next);
    true ->
        derflowdis:bind(Output, nil)
    end.

loop(S1, S2, End) ->
    derflowdis:wait_needed(S2),
    {S1Value, S1Next} = derflowdis:read(S1),
    {id, S2Next} = derflowdis:bind(S2, S1Value),
    {PS1, _} = S1,
    {PS2, _} = S2,
    io:format("Buff:Bound for consumer ~w-> ~w ~w~n",[PS1,PS2,S1Value]),
    case derflowdis:next(End) of {nil, _} ->
        ok;	
	EndNext ->
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
       	Next = derflowdis:next(S),
	io:format("Drop next ~w ~n",[S]),
    	drop_list(Next, Size-1)
    end.

consumer(S2, Size, F) ->
    if Size == 0 ->
	io:format("Finished~n");
	true ->
	    case derflowdis:read(S2) of
		{nil, _} ->
	   	io:format("Cons:Reading end~n");
		{Value, Next} ->
	   	{PS2,_} = S2,
	   	io:format("Cons:Id ~w Consume ~w, Get ~w, Next~w ~n",[PS2,Value, F(Value),Next]),
	   	consumer(Next, Size-1, F)
    	end
    end.


