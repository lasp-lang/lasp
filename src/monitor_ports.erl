-module(monitor_ports).
-export([run_port/1, sensor/2, dcs_monitor/3, test/0, register_comfailure/2, acc_register_comfailure/3]).

test()->
    {id, S1}=derflow:declare(),
    Port = derflow:thread(monitor_ports,run_port,[S1]),
    derflow:thread(monitor_ports,sensor,[Port, dc_1]),
    derflow:thread(monitor_ports,sensor,[Port, dc_2]),
    derflow:thread(monitor_ports,sensor,[Port, dc_3]),
    {id, S2}=derflow:declare(),
    derflow:thread(monitor_ports,dcs_monitor,[S1,S2,[]]),
    derflow:async_print_stream(S2).

run_port(Stream) ->
    receive
	{Message, From} ->
	    {id, Next} = derflow:bind(Stream, {Message, From}),
	    run_port(Next)
    end.

sensor(Port, Identifier) ->
    Miliseconds = round(timer:seconds(random:uniform())),
    timer:sleep(Miliseconds*10),
    Port ! {computer_down, Identifier},
    sensor(Port, Identifier).

dcs_monitor(Input, Output, State) ->
    case derflow:read(Input) of
    {{computer_down, Identifier}, NextInput} ->
	NewState = register_comfailure(Identifier, State),
	{id, NextOutput} = derflow:bind(Output, NewState),
	dcs_monitor(NextInput, NextOutput, NewState);
    {_, NextInput} ->
	%ignore
	dcs_monitor(NextInput, Output, State)
    end.

register_comfailure(Identifier, State) ->
    acc_register_comfailure(Identifier, State, []).

acc_register_comfailure(Identifier, [], NewState) -> 
    lists:append(NewState, [{Identifier, 1}]);
    
acc_register_comfailure(Identifier, [Next|Rest], NewState) -> 
    case Next of
    {Identifier, Counter} ->
	UpdatedPartialList = lists:append(NewState,[{Identifier, Counter+1}]),
	lists:append(UpdatedPartialList, Rest);
    _ ->
	acc_register_comfailure(Identifier, Rest, lists:append(NewState, [Next]))
    end.
