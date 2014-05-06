-module(getminimum).
-export([test/0,  producer/3, buffer/3, consumer/3]).

test() ->
    List = [6,7,5,7,4,2,1,0],
    {id, S1}=derflowdis:declare(),
    derflowdis:thread(getminimum,insort,[List,S1]),
    {V,_} = derflow:read(S1),
    io:format("Minimum~w~n",[V]).


insert(X, S, Out) ->
    %derflowdis:waitNeeded(Out),
    case derflowdis:read(S) of 
	{V, SNext} ->
		if X<V ->
			{id, NextKey} = derflowdis:syncBind(Out,X), 
			copyList(NextKey, S),
		 true -> 
			{id,Next} = derflowdis:syncBind(Out,V),
			insert(X, SNext, Next)
		 end;
	{nil,_} -> derflowdis:syncBind(Out,X), 
    end.

insort(List, S) ->
    case List of [H|T] ->
	   S2 = derflowdis:declare(),
	   insort(H,S,S2),
	   insert(H, S2);
	[] ->
	  derflow:bind(List, S)
    end.

copyList(Out, List) ->
   case List of [H|T] ->
	%derflowdis:waitNeeded(Out),
	{id,NextKey} = derflowdis:syncBind(Out,H),
	copyList(NextKey, T),
   end.


