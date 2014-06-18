%% @doc Test ports example.

-module(derflow_monitor_ports_test).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-export([test/0,
         run_port/1,
         sensor/2,
         dcs_monitor/3,
         register_comfailure/2,
         acc_register_comfailure/3]).

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

test()->
    {ok, S1} = derflow:declare(),
    Port = derflow:thread(?MODULE, run_port ,[S1]),
    derflow:thread(?MODULE, sensor, [Port, dc_1]),
    derflow:thread(?MODULE, sensor, [Port, dc_2]),
    derflow:thread(?MODULE, sensor, [Port, dc_3]),
    {ok, S2} = derflow:declare(),
    derflow:thread(?MODULE, dcs_monitor, [S1, S2, []]).

run_port(Stream) ->
    receive
        {Message, From} ->
            {ok, Next} = derflow:bind(Stream, {Message, From}),
            run_port(Next)
    end.

sensor(Port, Identifier) ->
    Miliseconds = round(timer:seconds(random:uniform())),
    timer:sleep(Miliseconds * 10),
    Port ! {computer_down, Identifier},
    sensor(Port, Identifier).

dcs_monitor(Input, Output, State) ->
    case derflow:read(Input) of
        {ok, {computer_down, Identifier}, NextInput} ->
            NewState = register_comfailure(Identifier, State),
            {ok, NextOutput} = derflow:bind(Output, NewState),
            dcs_monitor(NextInput, NextOutput, NewState);
        {ok, _, NextInput} ->
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
