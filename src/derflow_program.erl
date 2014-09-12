%% @doc Behavior for derflow programs, to ensure that they properly
%%      adhere to the right interface.
%%

-module(derflow_program).

-type crdt() :: riak_dt:crdt().

-callback init() -> {ok, crdt()} | {error, atom()}.
-callback execute(crdt()) -> {ok, crdt()} | {error, atom()}.
-callback execute(crdt(), term()) -> {ok, crdt()} | {error, atom()}.
-callback merge(list(crdt())) -> {ok, crdt()} | {error, atom()}.
