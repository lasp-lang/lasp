-module(getminimum).
-export([test/1, insert/3, insort/2, copyList/2]).

test(List) ->
    {id, S1}=derflow:declare(),
    ets:new(haha, [set, named_table, public, {write_concurrency, true}]),
    ets:insert(haha,{count, 0}),
    derflow:thread(getminimum,insort,[List, S1]),
    {V,_} = derflow:read(S1),
    io:format("Minimum ~w~n",[V]).

insert(X, In, Out) ->
    [{Id, C}] =ets:lookup(haha, count),
    ets:insert(haha,{Id,C+1}),
    io:format("Insert ~w ~n",[C+1]),
    derflow:wait_needed(Out),
    case derflow:read(In) of 
	{nil,_} -> %io:format("Reading end ~w~n",[X]), 
		   {id,Next} = derflow:bind(Out,X),
		   derflow:bind(Next, nil);
	{V, SNext} ->
		if X<V ->
			{id, Next} = derflow:bind(Out,X), 
			derflow:bind(Next, In);
		 true -> 
			{id,Next} = derflow:bind(Out,V),
			insert(X, SNext, Next)
		 end
    end.

insort(List, S) ->
    case List of [H|T] ->
	   {id,OutS} = derflow:declare(),
	   insort(T, OutS),
	   derflow:thread(getminimum, insert, [H, OutS, S]);
	[] ->
	   derflow:bind(S,nil)
    end.

copyList(Out, In) ->
   io:format("Reading ~w ~n",[In]),
   case derflow:read(In) of {nil,_} ->
	derflow:bind(Out,nil);
	{Value, Next} ->
	derflow:wait_needed(Out),
        io:format("Got value ~w ~n",[Value]),
	{id, NextKey} = derflow:bind(Out,Value),
	copyList(NextKey, Next)
   end.


