-module(getminimum).
-export([test/2, insert/4, insort/3, copyList/2]).

test(List, Lazy) ->
    %List = [1,0,2,3,4,8,9],
    {id, S1}=derflowdis:declare(),
    ets:new(haha, [set, named_table, public, {write_concurrency, true}]),
    ets:insert(haha,{count, 0}),
    derflowdis:thread(getminimum,insort,[List, S1, Lazy]),
    {V,_} = derflowdis:read(S1),
    io:format("Minimum ~w~n",[V]).


insert(X, In, Out, Lazy) ->
    [{Id, C}] =ets:lookup(haha, count),
    ets:insert(haha,{Id,C+1}),
    io:format("Insert ~w ~n",[C+1]),
    if Lazy == true -> 
    derflowdis:waitNeeded(Out);
     true->
	io:format("Not lazy")
    end,
    case derflowdis:read(In) of 
	{nil,_} -> %io:format("Reading end ~w~n",[X]), 
		   {id,Next} = derflowdis:bind(Out,X),
		   derflowdis:bind(Next, nil);
	{V, SNext} ->
		%io:format("The head is ~w, to insert is ~w ~n",[V, X]),
		if X<V ->
			{id, Next} = derflowdis:bind(Out,X), 
			derflowdis:bind(Next, In);
			%copyList(NextKey, In);
		 true -> 
			{id,Next} = derflowdis:bind(Out,V),
			insert(X, SNext, Next,Lazy)
		 end
    end.

insort(List, S, Lazy) ->
    case List of [H|T] ->
	   {id,OutS} = derflowdis:declare(),
	   insort(T, OutS, Lazy),
	   derflowdis:thread(getminimum, insert, [H, OutS, S, Lazy]);
	[] ->
	   %io:format("Binding to nothing~n"),
	   derflowdis:bind(S,nil)
    end.

copyList(Out, In) ->
   io:format("Reading ~w ~n",[In]),
   case derflowdis:read(In) of {nil,_} ->
	derflowdis:bind(Out,nil);
	{Value, Next} ->
	derflowdis:waitNeeded(Out),
        io:format("Got value ~w ~n",[Value]),
	{id, NextKey} = derflowdis:bind(Out,Value),
	copyList(NextKey, Next)
   end.


