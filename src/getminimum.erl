-module(getminimum).
-export([test/0, insert/3, insort/3]).

test() ->
    List = [1,0],
    {id, S1}=derflowdis:declare(),
    {id, TS}=derflowdis:declare(),
    derflowdis:thread(getminimum,insort,[List, TS, S1]),
    {V,_} = derflowdis:read(S1),
    io:format("Minimum ~w~n",[V]).


insert(X, S, Out) ->
    %derflowdis:waitNeeded(Out),
    case derflowdis:read(S) of 
	{nil,_} -> io:format("Reading end ~w~n",[X]), 
		   derflowdis:bind(Out,X); 
	{V, SNext} ->
		    io:format("The head is ~w, to insert is ~w ~n",[V, X]),
		if X<V ->
			{id, NextKey} = derflowdis:bind(Out,X), 
			copyList(NextKey, S);
		 true -> 
			{id,Next} = derflowdis:bind(Out,V),
			insert(X, SNext, Next)
		 end
    end.

insort(List, TS, S) ->
    case List of [H|T] ->
	   io:format("insort ~w~n",[H]),
	   insort(T, TS, S),
	   insert(H, TS, S);
	[] ->
	   derflowdis:bind(TS,nil)
    end.

copyList(Out, List) ->
   case List of [H|T] ->
	%derflowdis:waitNeeded(Out),
	{id,NextKey} = derflowdis:bind(Out,H),
	copyList(NextKey, T)
   end.


