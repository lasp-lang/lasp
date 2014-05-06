-module(sieve).
-export([test/1, test_opt/1, sieve/2, sieve_opt/3, filter/3, generate/3]).

test(Max) ->
    {id, S1}=derflowdis:declare(),
    %sieve:generate(2, Max, S1).
    derflowdis:thread(sieve,generate,[2,Max,S1]),
    {id, S2}=derflowdis:declare(),
    derflowdis:thread(sieve,sieve,[S1,S2]),
    %sieve:sieve(S1,S2),
    %io:format("Main: ~w, generate: ~w and sieve: ~n",[PIDM,PIDG]).
    derflowdis:async_print_stream(S2).

test_opt(Max) ->
    {id, S1}=derflowdis:declare(),
    derflowdis:thread(sieve,generate,[2,Max,S1]),
    {id, S2}=derflowdis:declare(),
    M = round(math:sqrt(Max)),
    derflowdis:thread(sieve,sieve_opt,[S1, M, S2]),
    derflowdis:async_print_stream(S2).

sieve(S1, S2) ->
    %io:format("Before read sieve~n"),
    case derflowdis:read(S1) of
    {nil, _} ->
        %io:format("After read sieve: nil~n"),
        derflowdis:syncBind(S2, nil);
    {Value, Next} ->
        %io:format("After read sieve: ~w~n",[Value]),
        {id, SN}=derflowdis:declare(),
	derflowdis:thread(sieve, filter, [Next, fun(Y) -> Y rem Value =/= 0 end, SN]),
    	io:format("Sieve created: ~w~n",[Value]),
        {id, NextOutput} = derflowdis:syncBind(S2, Value),
    	io:format("Sieve created 2: ~w~n",[Value]),
        sieve(SN, NextOutput)
    end.    

filter(S1, F, S2) ->
    %io:format("Before read filter~n"),
    case derflowdis:read(S1) of
    {nil, _} ->
        %io:format("After read filter: nil~n"),
        derflowdis:syncBind(S2, nil);
    {Value, Next} ->
        %io:format("After read filter: ~w~n",[Value]),
	case F(Value) of
	false ->
	    filter(Next, F, S2);
	true->
            {id, NextOutput} = derflowdis:syncBind(S2, Value),
	    filter(Next, F, NextOutput) 
	end
    end.    

generate(Init, N, Output) ->
    if (Init=<N) ->
	timer:sleep(250),
        {id, Next} = derflowdis:syncBind(Output, Init),
        generate(Init + 1, N,  Next);
    true ->
        derflowdis:syncBind(Output, nil)
    end.

sieve_opt(S1, M, S2) ->
    io:format("Before read~n"),
    case derflowdis:read(S1) of
    {nil, _} ->
        io:format("After read: nil~n"),
        derflowdis:syncBind(S2, nil);
    {Value, Next} ->
        io:format("After read: ~w~n",[Value]),
        {id, SN}=derflowdis:declare(),
	if Value=<M ->
	    derflowdis:thread(sieve, filter, [Next, fun(Y) -> Y rem Value =/= 0 end, SN]);
	true->
	    derflowdis:syncBind(SN, {id, Next})
	end,
        {id, NextOutput} = derflowdis:syncBind(S2, Value),
        sieve_opt(SN, M, NextOutput)
    end.
