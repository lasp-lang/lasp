-module(sieve).
-export([test/1, test_opt/1, sieve/2, sieve_opt/3, filter/3, generate/3]).

test(Max) ->
    {id, S1}=derflow:declare(),
    derflow:thread(sieve,generate,[2,Max,S1]),
    {id, S2}=derflow:declare(),
    derflow:thread(sieve,sieve,[S1,S2]),
    derflow:async_print_stream(S2).

test_opt(Max) ->
    {id, S1}=derflow:declare(),
    derflow:thread(sieve,generate,[2,Max,S1]),
    {id, S2}=derflow:declare(),
    M = round(math:sqrt(Max)),
    derflow:thread(sieve,sieve_opt,[S1, M, S2]),
    derflow:async_print_stream(S2).

sieve(S1, S2) ->
    %io:format("Before read sieve~n"),
    case derflow:read(S1) of
    {nil, _} ->
        %io:format("After read sieve: nil~n"),
        derflow:bind(S2, nil);
    {Value, Next} ->
        %io:format("After read sieve: ~w~n",[Value]),
        {id, SN}=derflow:declare(),
	derflow:thread(sieve, filter, [Next, fun(Y) -> Y rem Value =/= 0 end, SN]),
        {id, NextOutput} = derflow:bind(S2, Value),
        sieve(SN, NextOutput)
    end.    

filter(S1, F, S2) ->
    %io:format("Before read filter~n"),
    case derflow:read(S1) of
    {nil, _} ->
        %io:format("After read filter: nil~n"),
        derflow:bind(S2, nil);
    {Value, Next} ->
        %io:format("After read filter: ~w~n",[Value]),
	case F(Value) of
	false ->
	    filter(Next, F, S2);
	true->
            {id, NextOutput} = derflow:bind(S2, Value),
	    filter(Next, F, NextOutput) 
	end
    end.    

generate(Init, N, Output) ->
    if (Init=<N) ->
	timer:sleep(250),
        {id, Next} = derflow:bind(Output, Init),
        generate(Init + 1, N,  Next);
    true ->
        derflow:bind(Output, nil)
    end.

sieve_opt(S1, M, S2) ->
    case derflow:read(S1) of
    {nil, _} ->
        derflow:bind(S2, nil);
    {Value, Next} ->
        {id, SN}=derflow:declare(),
	if Value=<M ->
	    derflow:thread(sieve, filter, [Next, fun(Y) -> Y rem Value =/= 0 end, SN]);
	true->
	    derflow:bind(SN, {id, Next})
	end,
        {id, NextOutput} = derflow:bind(S2, Value),
        sieve_opt(SN, M, NextOutput)
    end.
