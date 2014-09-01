-module(derflow_threshold_read_test).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-export([test/0]).

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
    {GSet, GSet2} = rpc:call(Node, ?MODULE, test, []),
    ?assertEqual({ok, [1,2,3]}, GSet),
    ?assertEqual({ok, [1,2,3,4,5]}, GSet2),

    pass.

-endif.

test() ->
    %% Create new set-based CRDT.
    {ok, GSetId} = derflow:declare(riak_dt_gset),

    %% Determine my pid.
    Me = self(),

    %% Spawn fun which should block until lattice reaches [1,2,3].
    spawn(fun() -> Me ! derflow:read(GSetId, [1,2,3]) end),

    %% Perform 4 binds, each an inflation.
    {ok, _} = derflow:bind(GSetId, [1]),
    {ok, _} = derflow:bind(GSetId, [1, 2]),
    {ok, _} = derflow:bind(GSetId, [1, 2, 3]),
    {ok, _} = derflow:bind(GSetId, [1, 2, 3, 4]),

    %% Ensure we receive [1, 2, 3].
    GSet = receive
        {ok, [1, 2, 3], _} = V ->
            V
    end,

    %% Spawn fun which should block until lattice reaches [1,2,3,4,5].
    spawn(fun() -> Me ! derflow:read(GSetId, {greater, [1,2,3,4]}) end),

    %% Perform another inflation.
    {ok, _} = derflow:bind(GSetId, [1, 2, 3, 4, 5]),

    %% Ensure we receive [1, 2, 3, 4, 5].
    GSet2 = receive
        {ok, [1, 2, 3, 4, 5], _} = V1 ->
            V1
    end,

    lager:info("GSet: ~p GSet2: ~p", [GSet, GSet2]),

    {GSet, GSet2}.
