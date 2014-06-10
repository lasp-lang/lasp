-module(b_test).
-export([test/0, test1/0, test2/0, bindVar/2, bindValue/2, producer/3, buffer/3, consumer/3]).

test() ->
    {id, S1}=derflow:declare(),
    {id, S2}=derflow:declare(),
    {id, S3}=derflow:declare(),
    {id, S4}=derflow:declare(),
    derflow:thread(b_test,bindVar, [S1, S2]),
    derflow:thread(b_test,bindValue, [S2, 100]),
    derflow:thread(b_test,bindValue, [S4, 20]),
    derflow:thread(b_test,bindVar, [S3, S4]).


test1() ->
    {id, S1}=derflow:declare(),
    {id, S2}=derflow:declare(),
    {id, S3}=derflow:declare(),
    derflow:thread(b_test,bindVar, [S2, S3]),
    derflow:thread(b_test,bindVar, [S1, S3]),
    derflow:thread(b_test,bindValue, [S3, 100]).

test2() ->
    {id, S1}=derflow:declare(),
    {id, S2}=derflow:declare(),
    {id, S3}=derflow:declare(),
    derflow:thread(b_test,bindVar, [S2, S3]),
    derflow:thread(b_test,bindVar, [S1, S2]),
    derflow:thread(b_test,bindValue, [S3, 100]).

bindVar(X1, Y1) ->
   Next = derflow_vnode:bind(X1, {id,Y1}),
   io:format("Bind finished ~w ~w Next ~w ~n", [X1, Y1,Next]),
   X2=derflow_vnode:read(X1),
   %Y2=derflow_vnode:read(Y1),
   io:format("Value of ~w: ~w~n", [X1,X2]).


bindValue(Y1, V) ->
   receive
   after 100 -> io:format("~n")
   end,
   _X = derflow_vnode:bind(Y1, V),
   io:format("Bind Value finished ~w ~w ~n",[Y1,V]).

producer(Value, N, Output) ->
    if (N>0) ->
        derflow:wait_needed(Output),
	{id,Next} = derflow:bind(Output, Value),
        producer(Value+1, N-1,  Next);
    true ->
        derflow:bind(Output, nil)
    end.

loop(S1, S2, End) ->
    derflow:wait_needed(S2),
    {S1Value, S1Next} = derflow:read(S1),
    {id, S2Next} = derflow:bind(S2, S1Value),
    {PS1, _} = S1,
    {PS2, _} = S2,
    io:format("Buff:Bound for consumer ~w-> ~w ~w~n",[PS1,PS2,S1Value]),
    case derflow:next(End) of {nil, _} ->
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
       	Next = derflow:next(S),
	io:format("Drop next ~w ~n",[S]),
    	drop_list(Next, Size-1)
    end.

consumer(S2, Size, F) ->
    if Size == 0 ->
	io:format("Finished~n");
	true ->
	    case derflow:read(S2) of
		{nil, _} ->
	   	io:format("Cons:Reading end~n");
		{Value, Next} ->
	   	{PS2,_} = S2,
	   	io:format("Cons:Id ~w Consume ~w, Get ~w, Next~w ~n",[PS2,Value, F(Value),Next]),
	   	consumer(Next, Size-1, F)
    	end
    end.


