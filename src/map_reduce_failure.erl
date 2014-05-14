-module(map_reduce).
-export([test/0, test_1/0, jobtracker/5, spawnmap/5, wordCntMap/2, wordCntReduce/3, supervisor/1]).

test() ->
    Map = [{map_reduce, wordCntMap}],
    Reduce = [{map_reduce, wordCntReduce}],
    Input = [[[haha,good,bad],[stupid,good,bold],[stupid,good,good]]],
    {id, Output} = derflowdis:declare(),
    Supervisor = derflowdis:thread(map_reduce, supervisor, [dict:new()]),
    jobtracker(Supervisor, Map, Reduce, Input, [Output]),
    io:format("Final out ~w~n", [Output]),
    derflowdis:async_print_stream(Output).
test_1() ->
    Map = [{map_reduce, wordCntMap}],
    Reduce = [{map_reduce, wordCntReduce}],
    Input = [[[haha,good,bad],[stupid,good,bold],[stupid,good,good]]],
    {id, Output} = derflowdis:declare(),
    derflowdis:thread(derflowdis,async_print_stream,[Output]),
    Supervisor = derflowdis:thread(map_reduce, supervisor, [dict:new()]),
    jobtracker(Supervisor, Map, Reduce, Input, [Output]).


jobtracker(Supervisor, MapTasks, ReduceTasks, Inputs, Outputs) ->
    case MapTasks of [MapTask|MT] ->
	[ReduceTask|RT] = ReduceTasks,
	[Input|IT] = Inputs,
	[Output|OT] = Outputs,
    	{Module, MapFun} = MapTask,
    	{Module2, ReduceFun} = ReduceTask,
    	MapOut = spawnmap(Supervisor, Input, Module, MapFun, []),
    	derflowdis:thread_mon(Supervisor, Module2, ReduceFun, [MapOut, [], Output]),
	jobtracker(Supervisor, MT, RT, IT, OT);
	[] ->
	 io:format("All jobfinished!~n")
    end.

spawnmap(Supervisor, Inputs, Mod, Fun, Outputs) ->
    case Inputs of [H|T] ->
    	{id, S} = derflowdis:declare(),
    	derflowdis:thread_mon(Supervisor, Mod, Fun, [H, S]),
    	spawnmap(Supervisor, T, Mod, Fun, lists:append(Outputs,[S]));
	[] ->
	Outputs
    end.

supervisor(Dict) ->
    receive
	{'DOWN', Ref, process, _, noproc} ->
	    case dict:find(Ref, Dict) of
	    {ok, {Module, Function, Args}} ->
		derflowdis:thread_mon(self(), Module, Function, Args);
	    error ->
		supervisor(Dict)
	    end;
	{'DOWN', Ref, process, _, noconnection} ->
	    case dict:find(Ref, Dict) of
	    {ok, {Module, Function, Args}} ->
		derflowdis:thread_mon(self(), Module, Function, Args);
	    error ->
		supervisor(Dict)
	    end;
	{'DOWN', Ref, process, _, Reason} ->
	    io:format("Process killed, reference ~w, reason ~w,~n",[Ref, Reason]),
	    case dict:find(Ref, Dict) of
	    {ok, {Module, Function, Args}} ->
		derflowdis:thread_mon(self(), Module, Function, Args);
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
	

