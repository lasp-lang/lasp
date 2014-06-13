%% @doc Test that a derflow cluster can be built and initialized.

-module(derflow_test).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-ifdef(TEST).

-export([confirm/0]).

-define(HARNESS, (rt_config:get(rt_harness))).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    Nodes = rt:build_clusters([3]),
    lager:info("Nodes: ~p", [Nodes]),
    pass.

-endif.
