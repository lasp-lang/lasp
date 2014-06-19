%% @doc Test map / reduce example.

-module(derflow_map_reduce_failure_test).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-export([test/0,
         supervisor/1,
         jobtracker/5,
         spawnmap/5,
         word_count_map/2,
         word_count_reduce/3]).

-ifdef(TEST).

-export([confirm/0]).

-define(HARNESS, (rt_config:get(rt_harness))).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    [Nodes] = rt:build_clusters([1]),
    lager:info("Nodes: ~p", [Nodes]),
    Node = hd(Nodes),

    lager:info("Remotely loading code on node ~p", [Node]),
    ok = derflow_test_helpers:load(Nodes),
    lager:info("Remote code loading complete."),

    lager:info("Remotely executing the test."),
    rpc:call(Node, ?MODULE, test, []),
    pass.

-endif.

test() ->
    Map = [{map_reduce, word_count_map}],
    Reduce = [{map_reduce, word_count_reduce}],
    Input = [[[haha,good,bad], [stupid,good,bold], [stupid,good,good]]],
    {id, Output} = derflow:declare(),
    Supervisor = spawn(map_reduce, supervisor, [dict:new()]),
    jobtracker(Supervisor, Map, Reduce, Input, [Output]),
    lager:info("Final out ~w~n", [Output]).

jobtracker(Supervisor, MapTasks, ReduceTasks, Inputs, Outputs) ->
    case MapTasks of
        [MapTask|MT] ->
            [ReduceTask|RT] = ReduceTasks,
            [Input|IT] = Inputs,
            [Output|OT] = Outputs,
            {Module, MapFun} = MapTask,
            {Module2, ReduceFun} = ReduceTask,
            MapOut = spawnmap(Supervisor, Input, Module, MapFun, []),
            derflow:spawn_mon(Supervisor, Module2, ReduceFun, [MapOut, [], Output]),
            jobtracker(Supervisor, MT, RT, IT, OT);
        [] ->
            lager:info("All jobs finished!~n")
    end.

spawnmap(Supervisor, Inputs, Mod, Fun, Outputs) ->
    case Inputs of
        [H|T] ->
            {ok, S} = derflow:declare(),
            derflow:spawn_mon(Supervisor, Mod, Fun, [H, S]),
            spawnmap(Supervisor, T, Mod, Fun, lists:append(Outputs,[S]));
        [] ->
            Outputs
    end.

supervisor(Dict) ->
    receive
        {'DOWN', Ref, process, _, noproc} ->
            case dict:find(Ref, Dict) of
                {ok, {Module, Function, Args}} ->
                    derflow:spawn_mon(self(), Module, Function, Args);
                error ->
                    supervisor(Dict)
            end;
        {'DOWN', Ref, process, _, noconnection} ->
            case dict:find(Ref, Dict) of
                {ok, {Module, Function, Args}} ->
                    derflow:spawn_mon(self(), Module, Function, Args);
                error ->
                    supervisor(Dict)
            end;
        {'DOWN', Ref, process, _, _Reason} ->
            case dict:find(Ref, Dict) of
                {ok, {Module, Function, Args}} ->
                    derflow:spawn_mon(self(), Module, Function, Args);
                error ->
                    supervisor(Dict)
            end;
        {'SUPERVISE', Pid, Module, Function, Args} ->
            Ref = erlang:monitor(process, Pid),
            Dict2 = dict:store(Ref, {Module, Function, Args}, Dict),
            supervisor(Dict2)
          end.

word_count_map(Input, Output) ->
   timer:sleep(20000),
   case Input of
        [H|T] ->
            {ok, Next} = derflow:produce(Output, H),
            word_count_map(T, Next);
        [] ->
         derflow:bind(Output, nil)
    end.

word_count_reduce(Input, Tempout, Output) ->
    case Input of
        [H|T] ->
            AddedOutput = loop(H, Tempout),
            word_count_reduce(T, AddedOutput, Output);
        [] ->
            case Tempout of
                [H|T] ->
                    {ok, Next} = derflow:produce(Output, H),
                    word_count_reduce([], T, Next);
                [] ->
                    derflow:bind(Output, nil)
            end
    end.

loop(Elem, Output) ->
    case derflow:consume(Elem) of
        {ok, nil, _} ->
            Output;
        {ok, Value, Next} ->
            case lists:keysearch(Value, 1, Output) of
                {value, {_, Count}} ->
                    NewOutput = lists:keyreplace(Value, 1, Output,
                                                 {Value, Count + 1}),
                    loop(Next, NewOutput);
                false ->
                    NewOutput = lists:append(Output, [{Value, 1}]),
                    loop(Next, NewOutput)
            end
    end.
