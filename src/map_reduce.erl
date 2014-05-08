-module(map_reduce).
-export([test/0, jobtracker/4, spawnmap/4, wordCntMap/2, wordCntReduce/3]).

test() ->
    Map = [{map_reduce, wordCntMap}],
    Reduce = [{map_reduce, wordCntReduce}],
    Input = [[[haha,good,bad],[stupid,good,bold],[stupid,good,good]]],
    {id, Output} = derflowdis:declare(),
    jobtracker(Map, Reduce, Input, [Output]),
    io:format("Final out ~w~n", [Output]),
    derflowdis:async_print_stream(Output).


jobtracker(MapTasks, ReduceTasks, Inputs, Outputs) ->
    case MapTasks of [MapTask|MT] ->
	[ReduceTask|RT] = ReduceTasks,
	[Input|IT] = Inputs,
	[Output|OT] = Outputs,
    	{Module, MapFun} = MapTask,
    	{Module2, ReduceFun} = ReduceTask,
    	MapOut = spawnmap(Input, Module, MapFun, []),
    	derflowdis:thread(Module2, ReduceFun, [MapOut, [], Output]),
	jobtracker(MT, RT, IT, OT);
	[] ->
	 io:format("All jobfinished!")
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
	

