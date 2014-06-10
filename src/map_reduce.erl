-module(map_reduce).
-export([supervisor/1, init/0, jobtracker/2, jobproxy/1, sendTask/5, spawnmap/5, wordCntMap/2, wordCntReduce/3]).

init() ->
    Map = {map_reduce, wordCntMap},
    Reduce = {map_reduce, wordCntReduce},
    Input = [[haha,good,bad],[stupid,good,bold],[stupid,good,good]],
    {id, Output} = derflow:declare(),
    {id, TaskStream} = derflow:declare(),
    Port = derflow:thread(map_reduce, jobproxy, [TaskStream]),
    Supervisor = derflow:thread(map_reduce, supervisor, [dict:new()]),
    derflow:thread(map_reduce, jobtracker, [Supervisor, TaskStream]),
    receive
     after 1000 -> io:format("Waited~n")
    end,
    sendTask(Port, Map, Reduce, Input, Output).

sendTask(Port, Map, Reduce, Input, Output) ->
    Port ! {Map, Reduce, Input, Output}.

jobproxy(TaskStream) ->
    receive Task ->
	{id, Next} = derflow:bind(TaskStream, Task),
	jobproxy(Next)
    end.

jobtracker(Supervisor, Tasks) -> %MapTasks, ReduceTasks, Inputs, Outputs) ->
    case derflow:read(Tasks)  %[MapTask|MT] ->
	of {nil, _} ->
	 io:format("All jobfinished!");
	{Value, Next} ->
	{MapTask, ReduceTask, Input, Output} = Value,
    	{Module, MapFun} = MapTask,
    	{Module2, ReduceFun} = ReduceTask,
    	MapOut = spawnmap(Supervisor, Input, Module, MapFun, []),
    	derflow:thread_mon(Supervisor, Module2, ReduceFun, [MapOut, [], Output]),
        derflow:async_print_stream(Output),
	jobtracker(Supervisor, Next)
    end.

spawnmap(Supervisor, Inputs, Mod, Fun, Outputs) ->
    case Inputs of [H|T] ->
    	{id, S} = derflow:declare(),
    	derflow:thread_mon(Supervisor,Mod, Fun, [H, S]),
    	spawnmap(Supervisor, T, Mod, Fun, lists:append(Outputs,[S]));
	[] ->
	Outputs
    end.

supervisor(Dict) ->
    receive
	{'DOWN', Ref, process, _, noproc} ->
	    case dict:find(Ref, Dict) of
	    {ok, {Module, Function, Args}} ->
		derflow:thread_mon(self(), Module, Function, Args);
	    error ->
		supervisor(Dict)
	    end;
	{'DOWN', Ref, process, _, noconnection} ->
	    case dict:find(Ref, Dict) of
	    {ok, {Module, Function, Args}} ->
		derflow:thread_mon(self(), Module, Function, Args);
	    error ->
		supervisor(Dict)
	    end;
	{'DOWN', Ref, process, _, Reason} ->
	    io:format("Process killed, reference ~w, reason ~w,~n",[Ref, Reason]),
	    case dict:find(Ref, Dict) of
	    {ok, {Module, Function, Args}} ->
		derflow:thread_mon(self(), Module, Function, Args);
	    error ->
		supervisor(Dict)
	    end;
	{'SUPERVISE', PID, Module, Function, Args} ->
	    io:format("Supervise process ~w, that runs function ~w~n",[PID, Function]),
	    Ref = erlang:monitor(process, PID),
	    Dict2 = dict:store(Ref, {Module, Function, Args}, Dict),
	    supervisor(Dict2)
    end.

wordCntMap(Input, Output) ->
   io:format("MAP ~p~n",[self()]),
   timer:sleep(20000),
   case Input of [H|T] ->
	{id,Next} = derflow:bind(Output, H),
	wordCntMap(T, Next);
	[] ->
	 derflow:bind(Output, nil)
   end.
       
wordCntReduce(Input, Tempout, Output) ->
   case Input of [H|T] ->
	  AddedOutput = loop(H, Tempout),
	  wordCntReduce(T, AddedOutput, Output);
	[] ->
	  case Tempout of [H|T] ->
	  	{id, Next} = derflow:bind(Output, H),
		wordCntReduce([], T, Next);
		[] ->
		derflow:bind(Output, nil)
	 end
   end.

loop(Elem, Output) ->
	case  derflow:read(Elem) of 
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
	

