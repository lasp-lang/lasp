-module(video_display).
-export([display/1, test/0, sender/3]).

test()->
    {id, S1}=derflow:declare(),
    derflow:thread(video_display,sender,[0,10,S1]),
    video_display:display(S1).

sender(Init, N, Output) ->
    if (N>0) ->
        timer:sleep(500),
        {id, Next} = derflow:bind(Output, Init),
        sender(Init + 1, N-1,  Next);
    true ->
        timer:sleep(500),
        derflow:bind(Output, Init)
    end.

skip1(Input, Output) ->
    case derflow:read(Input) of
    {nil, _} ->
	derflow:bind(Output, nil);
    {_Value, Next} ->
	Bound = derflow:isDet(Next),
	if Bound ->
	    skip1(Next, Output);
	true ->
	    derflow:bind(Output, {id, Input})
	end
    end.

display(Input) ->
    timer:sleep(1500),
    {id, Output} = derflow:declare(),
    skip1(Input, Output),
    case derflow:read(Output) of
    {Value, Next} ->
	display_frame(Value),
	display(Next)
    end.

display_frame(X) ->
    io:format("Frame: ~w~n",[X]).
