-module(map_reduce).
-export([init/0, jobtracker/1, jobproxy/1, sendTask/5, spawnmap/4, wordCntMap/2, wordCntReduce/3]).

init() ->
    Map = {map_reduce, wordCntMap},
    Reduce = {map_reduce, wordCntReduce},
    Input = [[haha,good,bad],[stupid,good,bold],[stupid,good,good]],
    {id, Output} = derflowdis:declare(),
    {id, TaskStream} = derflowdis:declare(),
    Port = derflowdis:thread(map_reduce, jobproxy, [TaskStream]),
    derflowdis:thread(map_reduce, jobtracker, [TaskStream]),
    receive
     after 1000 -> io:format("Waited~n")
    end,
    sendTask(Port, Map, Reduce, Input, Output).

    

sendTask(Port, Map, Reduce, Input, Output) ->
    Port ! {Map, Reduce, Input, Output}.

jobproxy(TaskStream) ->
    receive Task ->
	{id, Next} = derflowdis:bind(TaskStream, Task),
	jobproxy(Next)
    end.

jobtracker(Tasks) -> %MapTasks, ReduceTasks, Inputs, Outputs) ->
    case derflowdis:read(Tasks)  %[MapTask|MT] ->
	of {nil, _} ->
	 io:format("All jobfinished!");
	{Value, Next} ->
	{MapTask, ReduceTask, Input, Output} = Value,
    	{Module, MapFun} = MapTask,
    	{Module2, ReduceFun} = ReduceTask,
    	MapOut = spawnmap(Input, Module, MapFun, []),
    	derflowdis:thread(Module2, ReduceFun, [MapOut, [], Output]),
        derflowdis:async_print_stream(Output),
	jobtracker(Next)
    end.

spawnmap(Inputs, Mod, Fun, Outputs) ->
    case Inputs of [H|T] ->
    	{id, S} = derflowdis:declare(),
    	derflowdis:thread(Mod, Fun, [H, S]),
    	spawnmap(T, Mod, Fun, lists:append(Outputs,[S]));
	[] ->
	Outputs
    end.

wordCntMap(Input, Output) ->
   case Input of [H|T] ->
	{id,Next} = derflowdis:bind(Output, H),
	wordCntMap(T, Next);
	[] ->
	 derflowdis:bind(Output, nil)
   end.
       
wordCntReduce(Input, Tempout, Output) ->
   case Input of [H|T] ->
	  AddedOutput = loop(H, Tempout),
	  wordCntReduce(T, AddedOutput, Output);
	[] ->
	  case Tempout of [H|T] ->
	  	{id, Next} = derflowdis:bind(Output, H),
		wordCntReduce([], T, Next);
		[] ->
		derflowdis:bind(Output, nil)
	 end
   end.

loop(Elem, Output) ->
	case  derflowdis:read(Elem) of 
	{nil, _} ->
		Output;
	{Value, Next} ->
		case lists:keysearch(Value, 1, Output) of {value, Tuple} ->
		    {_, Count} = Tuple,
		    NewTuple = {Value, Count+1},
		    NewOutput = lists:keyreplace(Value, 1, Output, NewTuple), 
	      	    loop(Next, NewOutput);
		false ->
		    NewOutput = lists:append(Output, [{Value, 1}]),
	      	    loop(Next, NewOutput)
		end
	 end.
	

