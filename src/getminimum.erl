-module(getminimum).
-export([test/1, insert/3, insort/2, copyList/2]).

test(List) ->
    {id, S1}=derflowdis:declare(),
    ets:new(haha, [set, named_table, public, {write_concurrency, true}]),
    ets:insert(haha,{count, 0}),
    derflowdis:thread(getminimum,insort,[List, S1]),
    {V,_} = derflowdis:read(S1),
    io:format("Minimum ~w~n",[V]).

insert(X, In, Out) ->
    [{Id, C}] =ets:lookup(haha, count),
    ets:insert(haha,{Id,C+1}),
    io:format("Insert ~w ~n",[C+1]),
    derflowdis:waitNeeded(Out),
    case derflowdis:read(In) of 
	{nil,_} -> %io:format("Reading end ~w~n",[X]), 
		   {id,Next} = derflowdis:bind(Out,X),
		   derflowdis:bind(Next, nil);
	{V, SNext} ->
		if X<V ->
			{id, Next} = derflowdis:bind(Out,X), 
			derflowdis:bind(Next, In);
		 true -> 
			{id,Next} = derflowdis:bind(Out,V),
			insert(X, SNext, Next)
		 end
    end.

insort(List, S) ->
    case List of [H|T] ->
	   {id,OutS} = derflowdis:declare(),
	   insort(T, OutS),
	   derflowdis:thread(getminimum, insert, [H, OutS, S]);
	[] ->
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


