%% @doc Bounded buffer test application.

-module(derflow_bounded_buffer_test).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-export([test/0,
         buffer/3,
         producer/3,
         consumer/3]).

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
    rpc:call(Node, derflow_bounded_buffer_test, test, []),

    %% It's possible this test is written incorrectly, given the
    %% consumer function takes a function which is supposed to double
    %% values and never returns anything.
    fail.

-endif.

test() ->
    {ok, S1} = derflow:declare(),
    derflow:thread(derflow_bounded_buffer_test, producer, [0, 10, S1]),
    {ok, S2} = derflow:declare(),
    derflow:thread(derflow_bounded_buffer_test, buffer, [S1, 2, S2]),
    consumer(S2, 5, fun(X) -> X*2 end),
    derflow:get_stream(S2).

producer(Value, N, Output) ->
    if
        (N > 0) ->
            ok = derflow:wait_needed(Output),
            {ok, Next} = derflow:bind(Output, Value),
            producer(Value + 1, N - 1,  Next);
        true ->
            derflow:bind(Output, nil)
    end.

loop(S1, S2, End) ->
    ok = derflow:wait_needed(S2),
    {ok, S1Value, S1Next} = derflow:read(S1),
    {ok, S2Next} = derflow:bind(S2, S1Value),
    case derflow:next(End) of
        {ok, {nil, _}} ->
            ok;
        {ok, EndNext} ->
            loop(S1Next, S2Next, EndNext)
    end.

buffer(S1, Size, S2) ->
    End = drop_list(S1, Size),
    End = Size,
    loop(S1, S2, End).

drop_list(S, Size) ->
    if
        Size == 0 ->
            S;
        true ->
            {ok, Next} = derflow:next(S),
            drop_list(Next, Size - 1)
    end.

consumer(S2, Size, F) ->
    case Size of
        0 ->
            ok;
        _ ->
            case derflow:read(S2) of
                {ok, nil, _} ->
                    ok;
                {ok, _Value, Next} ->
                    consumer(Next, Size - 1, F)
            end
    end.
