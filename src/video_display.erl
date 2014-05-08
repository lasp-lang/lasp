-module(video_display).
-export([skip1/2, display/1, test/0]).

test()->
    ok.

skip1(Input, Output) ->
    case derflowdis:read(Input) of
    {nil, _} ->
	derflowdis:syncBind(Output, nil);
    {Value, Next} ->
	if derflowdis:isDet(Next) ->
	    skip1(Next, Output);
	true ->
	    derflowdis:syncBind(Output, {id, Input})
	end
    end.

display(Input) ->
    {id, Output} = derflowdis:declare(),
    skip1(Input, Output),
    case derflowdis:read(Output) of
    {Value, Next} ->
	display_frame(Value),
	display(Next)
    end.

display_frame(X) ->
    io:format("Frame: ~w~n").
