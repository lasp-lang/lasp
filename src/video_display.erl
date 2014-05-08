-module(video_display).
-export([display/1, test/0, sender/3]).

test()->
    {id, S1}=derflowdis:declare(),
    derflowdis:thread(video_display,sender,[0,10,S1]),
    video_display:display(S1).

sender(Init, N, Output) ->
    if (N>0) ->
        timer:sleep(500),
        {id, Next} = derflowdis:bind(Output, Init),
        sender(Init + 1, N-1,  Next);
    true ->
        timer:sleep(500),
        derflowdis:bind(Output, Init)
    end.

skip1(Input, Output) ->
    case derflowdis:read(Input) of
    {nil, _} ->
	derflowdis:bind(Output, nil);
    {_Value, Next} ->
	Bound = derflowdis:isDet(Next),
	if Bound ->
	    skip1(Next, Output);
	true ->
	    derflowdis:bind(Output, {id, Input})
	end
    end.

display(Input) ->
    timer:sleep(1500),
    {id, Output} = derflowdis:declare(),
    skip1(Input, Output),
    case derflowdis:read(Output) of
    {Value, Next} ->
	display_frame(Value),
	display(Next)
    end.

display_frame(X) ->
    io:format("Frame: ~w~n",[X]).
