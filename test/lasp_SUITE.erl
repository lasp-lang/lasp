%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Christopher Meiklejohn.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
%%

-module(lasp_SUITE).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

%% common_test callbacks
-export([%% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0]).

%% tests
-compile([export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

%% ===================================================================
%% common_test callbacks
%% ===================================================================

init_per_suite(_Config) ->
    %% Start Lasp on the runner and enable instrumentation.
    lasp_support:start_runner(),

    _Config.

end_per_suite(_Config) ->
    %% Stop Lasp on the runner.
    lasp_support:stop_runner(),

    _Config.

init_per_testcase(Case, Config) ->
    ct:pal("Beginning case: ~p", [Case]),

    %% Runner must start and stop in between test runs as well, to
    %% ensure that we clear the membership list (otherwise, we could
    %% delete the data on disk, but this is cleaner.)
    lasp_support:start_runner(),

    Nodes = lasp_support:start_nodes(Case, Config),
    [{nodes, Nodes}|Config].

end_per_testcase(Case, Config) ->
    ct:pal("Case finished: ~p", [Case]),

    lasp_support:stop_nodes(Case, Config),

    %% Runner must start and stop in between test runs as well, to
    %% ensure that we clear the membership list (otherwise, we could
    %% delete the data on disk, but this is cleaner.)
    lasp_support:stop_runner().

all() ->
    [
     contracted_latency_test,
     uncontracted_latency_test,
     latency_with_reads_test,
     sql_parser_test,
     sql_combined_view_test,
     sql_simple_contracted_latency_test,
     sql_simple_uncontracted_latency_test,
     sql_join_contracted_latency_test,
     sql_join_uncontracted_latency_test,
     stream_test,
     query_test,
     ivar_test,
     orset_test,
     awset_ps_test,
     dynamic_ivar_test,
     monotonic_read_test,
     map_test,
     filter_test,
     union_test,
     product_test,
     intersection_test,
     membership_test,
     counter_enforce_once_test,
     counter_strict_enforce_once_test,
     awset_enforce_once_test,
     awset_strict_enforce_once_test,
     orset_enforce_once_test,
     orset_strict_enforce_once_test,
     interest_test
    ].

-include("lasp.hrl").

%% ===================================================================
%% tests
%% ===================================================================

-define(AWSET_PS, awset_ps).
-define(COUNTER, pncounter).
-define(LATENCY_ITERATIONS, 2200).
-define(TRIM, {trim, 100, 100}).

-define(ID, <<"myidentifier">>).

contracted_latency_test(_Config) ->
    case lasp_config:get(dag_enabled, true) of
        true ->
            Res = latency_test_case(5, contraction, no_read, ?LATENCY_ITERATIONS),
            write_csv(path_contraction, contraction, Res, ?TRIM);

        _ -> ok
    end.

uncontracted_latency_test(_Config) ->
    case lasp_config:get(dag_enabled, true) of
        true ->
            Res = latency_test_case(5, no_contraction, no_read, ?LATENCY_ITERATIONS),
            write_csv(path_contraction, no_contraction, Res, ?TRIM);

        _ -> ok
    end.

latency_with_reads_test(_Config) ->
    case lasp_config:get(dag_enabled, true) of
        true ->
            Res = latency_test_case(5, contraction, random_reads, ?LATENCY_ITERATIONS),
            write_csv(path_contraction, contraction_with_reads, Res, ?TRIM);

        _ -> ok
    end.

sql_parser_test(_Config) ->
    ok = lasp_sql_materialized_view:create_table_with_values(users, [
        [{name, "Foo"}, {age, 22}],
        [{name, "Bar"}, {age, 9}],
        [{name, "Baz"}, {age, 20}]
    ]),

    ?assertMatch([["Baz", 20], ["Bar", 9], ["Foo", 22]], lasp_sql_materialized_view:get_value(users, [name, age])),

    {ok, Id1} = lasp_sql_materialized_view:create("select name from users where age = 22 or age < 10"),

    %% Stabilize
    timer:sleep(100),

    ?assertMatch([["Bar"], ["Foo"]], lasp_sql_materialized_view:get_value(Id1, [name])),

    {ok, Id2} = lasp_sql_materialized_view:create("select name from users where age <= 22 and age => 10"),

    %% Stabilize
    timer:sleep(100),

    ?assertMatch([["Baz"], ["Foo"]], lasp_sql_materialized_view:get_value(Id2, [name])),

    {ok, Id3} = lasp_sql_materialized_view:create("select name from users where age < 22 and age > 19"),

    %% Stabilize
    timer:sleep(100),

    ?assertMatch([["Baz"]], lasp_sql_materialized_view:get_value(Id3, [name])),

    {ok, Id4} = lasp_sql_materialized_view:create("select name, age from users where age < 22 and age > 19"),

    %% Stabilize
    timer:sleep(100),

    ?assertMatch([["Baz", 20]], lasp_sql_materialized_view:get_value(Id4, [name, age])),

    {ok, Id5} = lasp_sql_materialized_view:create("select name, age from users where name = 'Foo' or name = 'Baz'"),

    %% Stabilize
    timer:sleep(100),

    ?assertMatch([["Baz", 20], ["Foo", 22]], lasp_sql_materialized_view:get_value(Id5, [name, age])),

    ok.

sql_combined_view_test(_Config) ->
    ok = lasp_sql_materialized_view:create_table_with_values(classics, [
        [{title, "Breathless"}, {year, 1960}, {rating, 80}],
        [{title, "A Woman Is a Woman"}, {year, 1961}, {rating, 76}],
        [{title, "Masculin Feminin"}, {year, 1966}, {rating, 77}],
        [{title, "La Chinoise"}, {year, 1967}, {rating, 73}]
    ]),

    {OutputId, Type}=Output = lasp_sql_materialized_view:generate_identifier(filtered),
    %% have to declare the value explicitly, otherwise query will fail on write
    lasp:declare(OutputId, Type),
    {ok, Id1} = lasp_sql_materialized_view:create(Output,
                                                  "select title, rating from classics where year => 1960 and year <= 1965"),

    %% Stabilize
    timer:sleep(100),

    ?assertMatch([["A Woman Is a Woman"], ["Breathless"]], lasp_sql_materialized_view:get_value(Id1, [title])),

    {ok, Id2} = lasp_sql_materialized_view:create("select title from filtered where rating => 80"),

    %% Stabilize
    timer:sleep(100),

    ?assertMatch([["Breathless"]], lasp_sql_materialized_view:get_value(Id2, [title])),

    ok.

latency_test_case(NVertices, Optimization, RandomReadsConfig, Iterations) ->
    Ids = generate_path(NVertices, ?COUNTER),
    case Optimization of
        no_contraction ->
            %% Disable automatic contraction
            lasp_config:set(automatic_contraction, false),
            %% Force a full cleaving before starting the test
            lasp_dependence_dag:cleave_all();
        _ ->
            %% Enable automatic contraction
            lasp_config:set(automatic_contraction, true),
            %% Force a contraction before starting the test
            lasp_dependence_dag:contract()
    end,
    First = lists:nth(1, Ids),
    Intermediate = case RandomReadsConfig of
        random_reads ->
            lists:sublist(Ids, 2, length(Ids) - 2);
        _ -> []
    end,
    Last = lists:last(Ids),
    latency_run_case(Iterations, [], RandomReadsConfig,
                     First, Intermediate, Last, increment, undefined).

latency_run_case(0, Acc, _, _, _, _, _, _) -> lists:reverse(Acc);
latency_run_case(Iterations, Acc, RandomReadsConfig,
                 From, Intermediate, To, Mutator, Threshold0) ->

    Threshold = {strict, Threshold0},
    MutateAndRead = fun(Src, Dst, Mutation, Thr) ->
        lasp:update(Src, Mutation, a),
        lasp:read(Dst, Thr)
    end,
    case RandomReadsConfig of
        no_read -> ok;
        random_reads ->
            %% Read from a random intermediate vertex every 10 iterations
            case (Iterations rem 10) =:= 0 of
                true ->
                    RPos = lasp_support:puniform(length(Intermediate)),
                    RandomChoice = lists:nth(RPos, Intermediate),
                    lasp:read(RandomChoice, undefined);
                _ -> ok
            end
    end,
    {Time, {ok, {_, _, _, NewThreshold}}} = timer:tc(MutateAndRead, [From, To, Mutator, Threshold]),

    latency_run_case(Iterations - 1, [Time | Acc], RandomReadsConfig,
                     From, Intermediate, To, Mutator, NewThreshold).


sql_simple_contracted_latency_test(_Config) ->
    case lasp_config:get(dag_enabled, true) of
        true ->
            %% Topology.
            Initial = lasp_sql_materialized_view:create_empty_table(initial),
            Middle = lasp_sql_materialized_view:create_empty_table(middle),
            Final = lasp_sql_materialized_view:create_empty_table(final),

            {ok, _} = lasp_sql_materialized_view:create(Middle,
                                                        "select a, b, c from initial where a > 0"),

            {ok, _} = lasp_sql_materialized_view:create(Final,
                                                        "select b, c from middle where b = 'foo'"),

            {ok, ResultTable} = lasp_sql_materialized_view:create("select c from final where c < 20"),

            Res = sql_latency_test_case(contraction, ?LATENCY_ITERATIONS, Initial, ResultTable, [{a, 1}, {b, "foo"}, {c, 0}]),
            write_csv(sql_simple_queries, contraction, Res, ?TRIM);

        _ -> ok
    end.

sql_simple_uncontracted_latency_test(_Config) ->
    case lasp_config:get(dag_enabled, true) of
        true ->
            %% Topology.
            Initial = lasp_sql_materialized_view:create_empty_table(initial),
            Middle = lasp_sql_materialized_view:create_empty_table(middle),
            Final = lasp_sql_materialized_view:create_empty_table(final),

            {ok, _} = lasp_sql_materialized_view:create(Middle,
                                                        "select a, b, c from initial where a > 0"),

            {ok, _} = lasp_sql_materialized_view:create(Final,
                                                        "select b, c from middle where b = 'foo'"),

            {ok, ResultTable} = lasp_sql_materialized_view:create("select c from final where c < 20"),

            Res = sql_latency_test_case(no_contraction, ?LATENCY_ITERATIONS, Initial, ResultTable, [{a, 1}, {b, "foo"}, {c, 0}]),
            write_csv(sql_simple_queries, no_contraction, Res, ?TRIM);

        _ -> ok
    end.

sql_join_contracted_latency_test(_Config) ->
    case lasp_config:get(dag_enabled, true) of
        true ->
            %% Topology.
            Initial = lasp_sql_materialized_view:create_empty_table(initial),
            Final = lasp_sql_materialized_view:create_empty_table(final),
            {ok, _} = lasp_sql_materialized_view:create(Final,
                                                        "select a, b, c from initial where a > 0 and b = 'foo'"),

            {ok, ResultTable} = lasp_sql_materialized_view:create("select c from final where c < 20"),

            Res = sql_latency_test_case(contraction, ?LATENCY_ITERATIONS, Initial, ResultTable, [{a, 1}, {b, "foo"}, {c, 0}]),
            write_csv(sql_join_queries, contraction, Res, ?TRIM);

        _ -> ok
    end.

sql_join_uncontracted_latency_test(_Config) ->
    case lasp_config:get(dag_enabled, true) of
        true ->
            %% Topology.
            Initial = lasp_sql_materialized_view:create_empty_table(initial),
            Final = lasp_sql_materialized_view:create_empty_table(final),
            {ok, _} = lasp_sql_materialized_view:create(Final,
                                                        "select a, b, c from initial where a > 0 and b = 'foo'"),

            {ok, ResultTable} = lasp_sql_materialized_view:create("select c from final where c < 20"),

            Res = sql_latency_test_case(no_contraction, ?LATENCY_ITERATIONS, Initial, ResultTable, [{a, 1}, {b, "foo"}, {c, 0}]),
            write_csv(sql_join_queries, no_contraction, Res, ?TRIM);

        _ -> ok
    end.

generate_path(N, Type) ->
    [_|Tail]=Ids = lists:map(fun(_) ->
        {ok, {Id, _, _, _}} = lasp:declare(Type),
        Id
    end, lists:seq(1, N)),
    zipwith(fun(L, R) ->
        lasp:bind_to(R, L)
    end, Ids, Tail),
    Ids.

zipwith(Fn, [X | Xs], [Y | Ys]) ->
    [Fn(X, Y) | zipwith(Fn, Xs, Ys)];

zipwith(Fn, _, _) when is_function(Fn, 2) -> [].

sql_latency_test_case(Optimization, Iterations, From, To, Mutation) ->
    case Optimization of
        no_contraction ->
            %% Disable automatic contraction
            lasp_config:set(automatic_contraction, false),
            %% Force a full cleaving before starting the test
            lasp_dependence_dag:cleave_all();
        contraction ->
            %% Enable automatic contraction
            lasp_config:set(automatic_contraction, true),
            %% Force a contraction before starting the test
            lasp_dependence_dag:contract()
    end,
    sql_run_case(Iterations, [], From, To, Mutation, undefined).

sql_run_case(0, Acc, _, _, _, _) -> lists:reverse(Acc);
sql_run_case(Iterations, Acc, From, To, Row, Threshold0) ->
    Threshold = {strict, Threshold0},
    MutateAndRead = fun(F, T, R, Th) ->
        lasp_sql_materialized_view:insert_row(F, R),
        lasp:read(T, Th)
    end,
    {Time, {ok, {_, _, _, NewThreshold}}} = timer:tc(MutateAndRead, [From, To, Row, Threshold]),
    sql_run_case(Iterations - 1, [Time | Acc], From, To, Row, NewThreshold).

write_csv(Dir, Option, Cases, Trim) ->
    Path = code:priv_dir(lasp)
           ++ "/evaluation/logs/"
           ++ atom_to_list(Dir) ++ "/"
           ++ atom_to_list(Option) ++ "/"
           ++ integer_to_list(timestamp()) ++ "/",
    ok = filelib:ensure_dir(Path),
    Trimmed = case Trim of
        {trim, Start, End} -> lists:sublist(Cases, Start + 1, (length(Cases) - (Start + End)));
        _ -> Cases
    end,
    lists:foreach(fun(Case) ->
        file:write_file(Path ++ "runner.csv", io_lib:fwrite("~p\n", [Case]), [append])
    end, Trimmed).

timestamp() ->
    {Mega, Sec, _Micro} = erlang:timestamp(),
    Mega * 1000000 + Sec.

%% @doc Increment counter and test stream behaviour.
stream_test(_Config) ->
    %% Declare a variable.
    {ok, {C1, _, _, _}} = lasp:declare(?COUNTER),

    %% Stream results.
    Self = self(),
    Function = fun(V) ->
                       Self ! {ok, V}
               end,
    ?assertMatch(ok, lasp:stream(C1, Function)),

    %% Increment.
    ?assertMatch({ok, _}, lasp:update(C1, increment, a)),

    %% Wait for message.
    receive
        {ok, 1} ->
            ok
    after
        300 ->
            ct:fail(missing_first_message)
    end,

    %% Increment again.
    ?assertMatch({ok, _}, lasp:update(C1, increment, b)),

    %% Wait for message.
    receive
        {ok, 2} ->
            ok
    after
        300 ->
            ct:fail(missing_second_message)
    end.

%% @doc Test query functionality.
query_test(_Config) ->
    %% Declare a variable.
    {ok, {I1, _, _, _}} = lasp:declare(ivar),

    %% Change it's value.
    ?assertMatch({ok, _}, lasp:update(I1, {set, 2}, a)),

    %% Threshold read just to create a synchronization point for the
    %% value to change.
    {ok, _} = lasp:read(I1, {strict, undefined}),

    %% Query it.
    ?assertMatch({ok, 2}, lasp:query(I1)),

    ok.

interest_test(Config) ->
    OtherTopic = other_updates,
    Topic = updates,
    Type = awset,
    Id = {<<"id">>, Type},

    %% Get nodes.
    [Node1, Node2 | _Nodes] = proplists:get_value(nodes, Config),

    %% Declare an interest on Node1.
    {ok, _} = rpc:call(Node1, lasp, interested, [Topic]),

    %% Create an object on Node1.
    {ok, {Id, _, _, _}} = rpc:call(Node1, lasp, declare, [Id, Type]),

    %% Create an object on Node2.
    {ok, {Id, _, _, _}} = rpc:call(Node2, lasp, declare, [Id, Type]),

    %% Set interest on Node1.
    ok = rpc:call(Node1, lasp, set_topic, [Id, OtherTopic]),

    %% Set interest on Node2.
    ok = rpc:call(Node2, lasp, set_topic, [Id, Topic]),

    %% Update object on Node2.
    ?assertMatch({ok, _}, rpc:call(Node2, lasp, update, [Id, {add, 2}, node1])),

    %% Sleep.
    timer:sleep(1000),

    %% Query on Node1.
    {ok, Node1Value} = rpc:call(Node1, lasp, query, [Id]),
    ct:pal("Node1Value: ~p", [Node1Value]),

    %% Query on Node2.
    {ok, Node2Value} = rpc:call(Node2, lasp, query, [Id]),
    ct:pal("Node1Value: ~p", [Node2Value]),

    ?assertNotEqual(Node1Value, Node2Value),

    ok.

%% @doc Single-assignment variable test.
ivar_test(_Config) ->
    %% Single-assignment variables.
    {ok, {I1, _, _, _}} = lasp:declare(ivar),
    {ok, {I2, _, _, _}} = lasp:declare(ivar),
    {ok, {I3, _, _, _}} = lasp:declare(ivar),

    V1 = 1,

    %% Attempt pre, and post- dataflow variable bind operations.
    ?assertMatch(ok, lasp:bind_to(I2, I1)),
    ?assertMatch({ok, _}, lasp:update(I1, {set, V1}, a)),
    ?assertMatch(ok, lasp:bind_to(I3, I1)),

    timer:sleep(4000),

    %% Verify the same value is contained by all.
    ?assertMatch({ok, {_, _, _, {_, V1}}}, lasp:read(I3, {strict, undefined})),
    ?assertMatch({ok, {_, _, _, {_, V1}}}, lasp:read(I2, {strict, undefined})),
    ?assertMatch({ok, {_, _, _, {_, V1}}}, lasp:read(I1, {strict, undefined})),

    ok.

%% @doc Map operation test.
map_test(_Config) ->
    %% Create initial set.
    {ok, {S1, _, _, _}} = lasp:declare(?SET),

    %% Add elements to initial set and update.
    ?assertMatch({ok, _}, lasp:update(S1, {add_all, [1,2,3]}, a)),

    %% Create second set.
    {ok, {S2, _, _, _}} = lasp:declare(?SET),

    %% Apply map.
    ?assertMatch(ok, lasp:map(S1, fun(X) -> X * 2 end, S2)),

    %% Wait.
    timer:sleep(4000),

    %% Bind again.
    ?assertMatch({ok, _}, lasp:update(S1, {add_all, [4,5,6]}, a)),

    %% Wait.
    timer:sleep(4000),

    %% Read resulting value.
    {ok, {_, _, _, S1V4}} = lasp:read(S1, {strict, undefined}),

    %% Read resulting value.
    {ok, {_, _, _, S2V1}} = lasp:read(S2, {strict, undefined}),

    ?assertEqual({ok, sets:from_list([1,2,3,4,5,6]), sets:from_list([2,4,6,8,10,12])},
                 {ok, lasp_type:query(?SET, S1V4), lasp_type:query(?SET, S2V1)}),

    ok.

%% @doc Filter operation test.
filter_test(_Config) ->
    %% Create initial set.
    {ok, {S1, _, _, _}} = lasp:declare(?SET),

    %% Add elements to initial set and update.
    ?assertMatch({ok, _}, lasp:update(S1, {add_all, [1,2,3]}, a)),

    %% Create second set.
    {ok, {S2, _, _, _}} = lasp:declare(?SET),

    %% Apply filter.
    ?assertMatch(ok, lasp:filter(S1, fun(X) -> X rem 2 == 0 end, S2)),

    %% Wait.
    timer:sleep(4000),

    %% Bind again.
    ?assertMatch({ok, _}, lasp:update(S1, {add_all, [4,5,6]}, a)),

    %% Wait.
    timer:sleep(4000),

    %% Read resulting value.
    {ok, {_, _, _, S1V4}} = lasp:read(S1, {strict, undefined}),

    %% Read resulting value.
    {ok, {_, _, _, S2V1}} = lasp:read(S2, {strict, undefined}),

    ?assertEqual({ok, sets:from_list([1,2,3,4,5,6]), sets:from_list([2,4,6])},
                 {ok, lasp_type:query(?SET, S1V4), lasp_type:query(?SET, S2V1)}),

    ok.

%% @doc Union operation test.
union_test(_Config) ->
    %% Create initial sets.
    {ok, {S1, _, _, _}} = lasp:declare(?SET),
    {ok, {S2, _, _, _}} = lasp:declare(?SET),

    %% Create output set.
    {ok, {S3, _, _, _}} = lasp:declare(?SET),

    %% Populate initial sets.
    ?assertMatch({ok, _}, lasp:update(S1, {add_all, [1,2,3]}, a)),
    ?assertMatch({ok, _}, lasp:update(S2, {add_all, [a,b,c]}, a)),

    %% Apply union.
    ?assertMatch(ok, lasp:union(S1, S2, S3)),

    %% Sleep.
    timer:sleep(400),

    %% Read union.
    {ok, {_, _, _, Union0}} = lasp:read(S3, undefined),

    %% Read union value.
    Union = lasp_type:query(?SET, Union0),

    ?assertEqual({ok, sets:from_list([1,2,3,a,b,c])}, {ok, Union}),

    ok.

%% @doc Intersection test.
intersection_test(_Config) ->
    %% Create initial sets.
    {ok, {S1, _, _, _}} = lasp:declare(?SET),
    {ok, {S2, _, _, _}} = lasp:declare(?SET),

    %% Create output set.
    {ok, {S3, _, _, _}} = lasp:declare(?SET),

    %% Populate initial sets.
    ?assertMatch({ok, _}, lasp:update(S1, {add_all, [1,2,3]}, a)),
    ?assertMatch({ok, _}, lasp:update(S2, {add_all, [a,b,3]}, a)),

    %% Apply intersection.
    ?assertMatch(ok, lasp:intersection(S1, S2, S3)),

    %% Sleep.
    timer:sleep(400),

    %% Read intersection.
    {ok, {_, _, _, Intersection0}} = lasp:read(S3, undefined),

    %% Read intersection value.
    Intersection = lasp_type:query(?SET, Intersection0),

    ?assertEqual({ok, sets:from_list([3])}, {ok, Intersection}),

    ok.

%% @doc Cartesian product test.
product_test(_Config) ->
    %% Create initial sets.
    {ok, {S1, _, _, _}} = lasp:declare(?SET),
    {ok, {S2, _, _, _}} = lasp:declare(?SET),

    %% Create output set.
    {ok, {S3, _, _, _}} = lasp:declare(?SET),

    %% Populate initial sets.
    ?assertMatch({ok, _}, lasp:update(S1, {add_all, [1,2,3]}, a)),
    ?assertMatch({ok, _}, lasp:update(S2, {add_all, [a,b,3]}, a)),

    %% Apply product.
    ?assertMatch(ok, lasp:product(S1, S2, S3)),

    %% Sleep.
    timer:sleep(400),

    %% Read product.
    {ok, {_, _, _, Product0}} = lasp:read(S3, undefined),

    %% Read product value.
    Product = lasp_type:query(?SET, Product0),

    ?assertEqual({ok,sets:from_list([{1,3},{1,a},{1,b},{2,3},{2,a},{2,b},{3,3},{3,a},{3,b}])}, {ok, Product}),

    ok.

%% @doc Monotonic read.
monotonic_read_test(_Config) ->
    %% Create new set-based CRDT.
    {ok, {SetId, _, _, _}} = lasp:declare(?SET),

    %% Determine my pid.
    Me = self(),

    %% Perform 4 binds, each an inflation.
    ?assertMatch({ok, _},
                 lasp:update(SetId, {add_all, [1]}, actor)),

    {ok, {_, _, _, V0}} = lasp:update(SetId, {add_all, [2]}, actor),

    ?assertMatch({ok, {_, _, _, _}},
                 lasp:update(SetId, {add_all, [3]}, actor)),

    %% Spawn fun which should block until lattice is strict inflation of
    %% V0.
    I1 = first_read,
    spawn(fun() -> Me ! {I1, lasp:read(SetId, {strict, V0})} end),

    %% Ensure we receive [1, 2, 3].
    Set1 = receive
        {I1, {ok, {_, _, _, X}}} ->
            lasp_type:query(?SET, X)
    end,

    %% Perform more inflations.
    {ok, {_, _, _, V1}} = lasp:update(SetId, {add_all, [4]}, actor),

    ?assertMatch({ok, _}, lasp:update(SetId, {add_all, [5]}, actor)),

    %% Spawn fun which should block until lattice is a strict inflation
    %% of V1.
    I2 = second_read,
    spawn(fun() -> Me ! {I2, lasp:read(SetId, {strict, V1})} end),

    %% Ensure we receive [1, 2, 3, 4].
    Set2 = receive
        {I2, {ok, {_, _, _, Y}}} ->
            lasp_type:query(?SET, Y)
    end,

    ?assertEqual({sets:from_list([1,2,3]), sets:from_list([1,2,3,4,5])},
                 {Set1, Set2}),

    ok.

%% @doc Dynamic variable test.
dynamic_ivar_test(Config) ->
    [Node1, Node2 | _Nodes] = proplists:get_value(nodes, Config),

    %% Setup a dynamic variable.
    {ok, {Id, _, _, Value}} = rpc:call(Node1, lasp, declare_dynamic,
                                       [?ID, ivar]),

    %% Now, the following action should be idempotent.
    {ok, {Id, _, _, Value}} = rpc:call(Node2, lasp, declare_dynamic,
                                       [?ID, ivar]),

    %% Bind node 1's name to the value on node 1: this should not
    %% trigger a broadcast message because the variable is dynamic.
    ?assertMatch({ok, {Id, _, _, {_, Node1}}},
                 rpc:call(Node1, lasp, bind, [{?ID, ivar}, {state_ivar, Node1}])),

    %% Bind node 2's name to the value on node 2: this should not
    %% trigger a broadcast message because the variable is dynamic.
    ?assertMatch({ok, {Id, _, _, {_, Node2}}},
                 rpc:call(Node2, lasp, bind, [{?ID, ivar}, {state_ivar, Node2}])),

    %% Verify variable has the correct value.
    {ok, Node1} = rpc:call(Node1, lasp, query, [{?ID, ivar}]),

    %% Verify variable has the correct value.
    {ok, Node2} = rpc:call(Node2, lasp, query, [{?ID, ivar}]),

    ok.

%% @doc Test of the orset.
orset_test(_Config) ->
    {ok, {L1, _, _, _}} = lasp:declare(?SET),
    {ok, {L2, _, _, _}} = lasp:declare(?SET),
    {ok, {L3, _, _, _}} = lasp:declare(?SET),

    %% Attempt pre, and post- dataflow variable bind operations.
    ?assertMatch(ok, lasp:bind_to(L2, L1)),
    {ok, {_, _, _, S2}} = lasp:update(L1, {add, 1}, a),
    ?assertMatch(ok, lasp:bind_to(L3, L1)),

    timer:sleep(4000),

    %% Verify the same value is contained by all.
    {ok, {_, _, _, S1}} = lasp:read(L3, {strict, undefined}),
    {ok, {_, _, _, S1}} = lasp:read(L2, {strict, undefined}),
    {ok, {_, _, _, S1}} = lasp:read(L1, {strict, undefined}),

    Self = self(),

    spawn_link(fun() ->
                  {ok, _} = lasp:wait_needed(L1, {strict, S1}),
                  Self ! threshold_met
               end),

    ?assertMatch({ok, _}, lasp:bind(L1, S2)),

    timer:sleep(4000),

    %% Verify the same value is contained by all.
    {ok, {_, _, _, S2L3}} = lasp:read(L3, {strict, undefined}),
    ?assertEqual(S2L3, lasp_type:merge(?SET, S2, S2L3)),
    {ok, {_, _, _, S2L2}} = lasp:read(L2, {strict, undefined}),
    ?assertEqual(S2L2, lasp_type:merge(?SET, S2, S2L2)),
    {ok, {_, _, _, S2L1}} = lasp:read(L1, {strict, undefined}),
    ?assertEqual(S2L1, lasp_type:merge(?SET, S2, S2L1)),

    %% Read at the S2 threshold level.
    {ok, {_, _, _, _}} = lasp:read(L1, S2),

    %% Wait for wait_needed to unblock.
    receive
        threshold_met ->
            ok
    end,

    {ok, {L5, _, _, _}} = lasp:declare(?SET),
    {ok, {L6, _, _, _}} = lasp:declare(?SET),

    spawn_link(fun() ->
                {ok, _} = lasp:read_any([{L5, {strict, undefined}}, {L6, {strict, undefined}}]),
                Self ! read_any
        end),

    {ok, _} = lasp:update(L5, {add, 1}, a),

    receive
        read_any ->
            ok
    end,

    ok.

%% @doc Enforce once test.
counter_enforce_once_test(Config) ->
    Manager = lasp_peer_service:manager(),
    lager:info("Manager: ~p", [Manager]),

    Nodes = proplists:get_value(nodes, Config),
    lager:info("Nodes: ~p", [Nodes]),
    Node = hd(lists:usort(Nodes)),

    lager:info("Waiting for cluster to stabilize."),
    timer:sleep(10000),

    %% Declare the object first: if not, invariant can't be registered.
    Id = {<<"object">>, ?COUNTER_TYPE},
    {ok, _} = rpc:call(Node, lasp, declare, [Id, ?COUNTER_TYPE]),

    %% Define an enforce-once invariant.
    Self = self(),
    Threshold = {value, 3},

    EnforceFun = fun(X) ->
                         lager:info("Enforce function fired with: ~p", [X]),
                         Self ! {ok, Threshold}
                 end,

    lager:info("Adding invariant on node: ~p!", [Node]),

    case rpc:call(Node, lasp, enforce_once, [Id, Threshold, EnforceFun]) of
        ok ->
            lager:info("Invariant configured!");
        Error ->
            lager:info("Invariant can't be configured: ~p", [Error]),
            ct:fail(failed)
    end,

    %% Increment counter twice to get trigger to fire.
    {ok, _} = rpc:call(Node, lasp, update, [Id, increment, self()]),
    {ok, _} = rpc:call(Node, lasp, update, [Id, increment, self()]),
    {ok, _} = rpc:call(Node, lasp, update, [Id, increment, self()]),

    lager:info("Waiting for response..."),
    receive
        {ok, Threshold} ->
            ok
    after
        10000 ->
            lager:info("Did not receive response!"),
            ct:fail(failed)
    end,

    lager:info("Finished counter_enforce_once_test."),

    ok.

%% @doc Enforce once test.
counter_strict_enforce_once_test(Config) ->
    %% Test doesn't complete in only the Travis environment;
    %% unclear why, it passes both on DigitalOcean and locally;
    %% Ignore for now.
    case os:getenv("TRAVIS", false) of
        "true" ->
            ok;
        _ ->
            Manager = lasp_peer_service:manager(),
            lager:info("Manager: ~p", [Manager]),

            Nodes = proplists:get_value(nodes, Config),
            lager:info("Nodes: ~p", [Nodes]),
            Node = hd(lists:usort(Nodes)),

            lager:info("Waiting for cluster to stabilize."),
            timer:sleep(10000),

            %% Declare the object first: if not, invariant can't be registered.
            Id = {<<"object">>, ?COUNTER_TYPE},
            {ok, _} = rpc:call(Node, lasp, declare, [Id, ?COUNTER_TYPE]),

            %% Define an enforce-once invariant.
            Self = self(),
            Threshold = {strict, {value, 2}},

            EnforceFun = fun(X) ->
                                ct:pal("Enforce function fired with: ~p", [X]),
                                Self ! {ok, Threshold}
                        end,

            lager:info("Adding invariant on node: ~p!", [Node]),

            case rpc:call(Node, lasp, enforce_once, [Id, Threshold, EnforceFun]) of
                ok ->
                    lager:info("Invariant configured!");
                Error ->
                    lager:info("Invariant can't be configured: ~p", [Error]),
                    ct:fail(failed)
            end,

            %% Wait.
            timer:sleep(1000),

            %% Increment counter twice to get trigger to fire.
            {ok, _} = rpc:call(Node, lasp, update, [Id, increment, self()]),
            {ok, _} = rpc:call(Node, lasp, update, [Id, increment, self()]),
            {ok, _} = rpc:call(Node, lasp, update, [Id, increment, self()]),

            lager:info("Waiting for response..."),
            receive
                {ok, Threshold} ->
                    ok;
                Other ->
                    ct:fail({received_other_message, Other})
            after
                10000 ->
                    lager:info("Did not receive response!"),
                    ct:fail(failed)
            end,

            lager:info("Finished counter_strict_enforce_once_test."),

            ok
    end.

%% @doc Enforce once test.
awset_enforce_once_test(Config) ->
    Manager = lasp_peer_service:manager(),
    lager:info("Manager: ~p", [Manager]),

    Nodes = proplists:get_value(nodes, Config),
    lager:info("Nodes: ~p", [Nodes]),
    Node = hd(lists:usort(Nodes)),

    lager:info("Waiting for cluster to stabilize."),
    timer:sleep(10000),

    %% Declare the object first: if not, invariant can't be registered.
    Id = {<<"object">>, ?AWSET_TYPE},
    {ok, _} = rpc:call(Node, lasp, declare, [Id, ?AWSET_TYPE]),

    %% Define an enforce-once invariant.
    Self = self(),
    Threshold = {cardinality, 1},

    EnforceFun = fun(X) ->
                         lager:info("Enforce function fired with: ~p", [X]),
                         Self ! {ok, Threshold}
                 end,

    lager:info("Adding invariant on node: ~p!", [Node]),

    case rpc:call(Node, lasp, enforce_once, [Id, Threshold, EnforceFun]) of
        ok ->
            lager:info("Invariant configured!");
        Error ->
            lager:info("Invariant can't be configured: ~p", [Error]),
            ct:fail(failed)
    end,

    %% Increment counter twice to get trigger to fire.
    {ok, _} = rpc:call(Node, lasp, update, [Id, {add, 1}, self()]),
    {ok, _} = rpc:call(Node, lasp, update, [Id, {add, 2}, self()]),
    {ok, _} = rpc:call(Node, lasp, update, [Id, {add, 3}, self()]),

    lager:info("Waiting for response..."),
    receive
        {ok, Threshold} ->
            ok
    after
        10000 ->
            lager:info("Did not receive response!"),
            ct:fail(failed)
    end,

    lager:info("Finished awset_enforce_once_test."),

    ok.

%% @doc Enforce once test.
awset_strict_enforce_once_test(Config) ->
    Manager = lasp_peer_service:manager(),
    lager:info("Manager: ~p", [Manager]),

    Nodes = proplists:get_value(nodes, Config),
    lager:info("Nodes: ~p", [Nodes]),
    Node = hd(lists:usort(Nodes)),

    lager:info("Waiting for cluster to stabilize."),
    timer:sleep(10000),

    %% Declare the object first: if not, invariant can't be registered.
    Id = {<<"object">>, ?AWSET_TYPE},
    {ok, _} = rpc:call(Node, lasp, declare, [Id, ?AWSET_TYPE]),

    %% Define an enforce-once invariant.
    Self = self(),
    Threshold = {strict, {cardinality, 1}},

    EnforceFun = fun(X) ->
                         lager:info("Enforce function fired with: ~p", [X]),
                         Self ! {ok, Threshold}
                 end,

    lager:info("Adding invariant on node: ~p!", [Node]),

    case rpc:call(Node, lasp, enforce_once, [Id, Threshold, EnforceFun]) of
        ok ->
            lager:info("Invariant configured!");
        Error ->
            lager:info("Invariant can't be configured: ~p", [Error]),
            ct:fail(failed)
    end,

    %% Increment counter twice to get trigger to fire.
    {ok, _} = rpc:call(Node, lasp, update, [Id, {add, 1}, self()]),
    {ok, _} = rpc:call(Node, lasp, update, [Id, {add, 2}, self()]),
    {ok, _} = rpc:call(Node, lasp, update, [Id, {add, 3}, self()]),

    lager:info("Waiting for response..."),
    receive
        {ok, Threshold} ->
            ok
    after
        10000 ->
            lager:info("Did not receive response!"),
            ct:fail(failed)
    end,

    lager:info("Finished awset_strict_enforce_once_test."),

    ok.

%% @doc Enforce once test.
orset_enforce_once_test(Config) ->
    Manager = lasp_peer_service:manager(),
    lager:info("Manager: ~p", [Manager]),

    Nodes = proplists:get_value(nodes, Config),
    lager:info("Nodes: ~p", [Nodes]),
    Node = hd(lists:usort(Nodes)),

    lager:info("Waiting for cluster to stabilize."),
    timer:sleep(10000),

    %% Declare the object first: if not, invariant can't be registered.
    Id = {<<"object">>, ?SET},
    {ok, _} = rpc:call(Node, lasp, declare, [Id, ?SET]),

    %% Define an enforce-once invariant.
    Self = self(),
    Threshold = {cardinality, 1},

    EnforceFun = fun(X) ->
                         lager:info("Enforce function fired with: ~p", [X]),
                         Self ! {ok, Threshold}
                 end,

    lager:info("Adding invariant on node: ~p!", [Node]),

    case rpc:call(Node, lasp, enforce_once, [Id, Threshold, EnforceFun]) of
        ok ->
            lager:info("Invariant configured!");
        Error ->
            lager:info("Invariant can't be configured: ~p", [Error]),
            ct:fail(failed)
    end,

    %% Increment counter twice to get trigger to fire.
    {ok, _} = rpc:call(Node, lasp, update, [Id, {add, 1}, self()]),
    {ok, _} = rpc:call(Node, lasp, update, [Id, {add, 2}, self()]),
    {ok, _} = rpc:call(Node, lasp, update, [Id, {add, 3}, self()]),

    lager:info("Waiting for response..."),
    receive
        {ok, Threshold} ->
            ok
    after
        10000 ->
            lager:info("Did not receive response!"),
            ct:fail(failed)
    end,

    lager:info("Finished orset_enforce_once_test."),

    ok.

%% @doc Enforce once test.
orset_strict_enforce_once_test(Config) ->
    Manager = lasp_peer_service:manager(),
    lager:info("Manager: ~p", [Manager]),

    Nodes = proplists:get_value(nodes, Config),
    lager:info("Nodes: ~p", [Nodes]),
    Node = hd(lists:usort(Nodes)),

    lager:info("Waiting for cluster to stabilize."),
    timer:sleep(10000),

    %% Declare the object first: if not, invariant can't be registered.
    Id = {<<"object">>, ?SET},
    {ok, _} = rpc:call(Node, lasp, declare, [Id, ?SET]),

    %% Define an enforce-once invariant.
    Self = self(),
    Threshold = {strict, {cardinality, 1}},

    EnforceFun = fun(X) ->
                         lager:info("Enforce function fired with: ~p", [X]),
                         Self ! {ok, Threshold}
                 end,

    lager:info("Adding invariant on node: ~p!", [Node]),

    case rpc:call(Node, lasp, enforce_once, [Id, Threshold, EnforceFun]) of
        ok ->
            lager:info("Invariant configured!");
        Error ->
            lager:info("Invariant can't be configured: ~p", [Error]),
            ct:fail(failed)
    end,

    %% Increment counter twice to get trigger to fire.
    {ok, _} = rpc:call(Node, lasp, update, [Id, {add, 1}, self()]),
    {ok, _} = rpc:call(Node, lasp, update, [Id, {add, 2}, self()]),
    {ok, _} = rpc:call(Node, lasp, update, [Id, {add, 3}, self()]),

    lager:info("Waiting for response..."),
    receive
        {ok, Threshold} ->
            ok
    after
        10000 ->
            lager:info("Did not receive response!"),
            ct:fail(failed)
    end,

    lager:info("Finished orset_strict_enforce_once_test."),

    ok.

%% @doc Membership test.
membership_test(Config) ->
    Manager = lasp_peer_service:manager(),
    ct:pal("Manager: ~p", [Manager]),

    Nodes = proplists:get_value(nodes, Config),
    lager:info("Nodes: ~p", [Nodes]),
    Sorted = lists:usort(Nodes),

    lager:info("Waiting for cluster to stabilize."),
    timer:sleep(10000),

    case Manager of
        partisan_hyparview_peer_service_manager ->
            lists:foreach(fun(Node) ->
                                {ok, Active} = rpc:call(Node, Manager, active, []),
                                {ok, Passive} = rpc:call(Node, Manager, passive, []),

                                case lists:usort([Name || {Name, _, _} <- sets:to_list(Active)]) of
                                    Sorted ->
                                        ok;
                                    WrongActive ->
                                        ct:fail("Incorrect nodes in active view on node ~p: ~p should be ~p!",
                                                [Node, WrongActive, Sorted])
                                end,

                                case sets:to_list(Passive) of
                                    [] ->
                                        ok;
                                    WrongPassive ->
                                        ct:fail("Incorrect nodes in passive view: ~p!",
                                                [WrongPassive])
                                end
                          end, Nodes);
        partisan_default_peer_service_manager ->
            lists:foreach(fun(Node) ->
                                {ok, Members} = rpc:call(Node, Manager, members, []),
                                lists:usort(Members) =:= lists:usort(Nodes)
                          end, Nodes);
        partisan_client_server_peer_service_manager ->
            lists:foreach(fun(Node) ->
                                {ok, Members} = rpc:call(Node, Manager, members, []),
                                lists:usort(Members) =:= lists:usort(Nodes)
                          end, Nodes)
    end,

    ok.

%% @doc Test of the add-wins set.
awset_ps_test(_Config) ->
    {ok, {L1, _, _, _}} = lasp:declare(?AWSET_PS),
    {ok, {L2, _, _, _}} = lasp:declare(?AWSET_PS),
    {ok, {L3, _, _, _}} = lasp:declare(?AWSET_PS),

    %% Attempt pre, and post- dataflow variable bind operations.
    ?assertMatch(ok, lasp:bind_to(L2, L1)),
    {ok, {_, _, _, S2}} = lasp:update(L1, {add, 1}, a),
    ?assertMatch(ok, lasp:bind_to(L3, L1)),

    timer:sleep(4000),

    %% Verify the same value is contained by all.
    {ok, {_, _, _, S1}} = lasp:read(L3, {strict, undefined}),
    {ok, {_, _, _, S1}} = lasp:read(L2, {strict, undefined}),
    {ok, {_, _, _, S1}} = lasp:read(L1, {strict, undefined}),

    Self = self(),

    spawn_link(fun() ->
                  {ok, _} = lasp:wait_needed(L1, {strict, S1}),
                  Self ! threshold_met
               end),

    ?assertMatch({ok, _}, lasp:bind(L1, S2)),

    timer:sleep(4000),

    %% Verify the same value is contained by all.
    {ok, {_, _, _, S2L3}} = lasp:read(L3, {strict, undefined}),
    ?assertEqual(S2L3, lasp_type:merge(?AWSET_PS, S2, S2L3)),
    {ok, {_, _, _, S2L2}} = lasp:read(L2, {strict, undefined}),
    ?assertEqual(S2L2, lasp_type:merge(?AWSET_PS, S2, S2L2)),
    {ok, {_, _, _, S2L1}} = lasp:read(L1, {strict, undefined}),
    ?assertEqual(S2L1, lasp_type:merge(?AWSET_PS, S2, S2L1)),

    %% Read at the S2 threshold level.
    {ok, {_, _, _, _}} = lasp:read(L1, S2),

    %% Wait for wait_needed to unblock.
    receive
        threshold_met ->
            ok
    end,

    {ok, {L5, _, _, _}} = lasp:declare(?AWSET_PS),
    {ok, {L6, _, _, _}} = lasp:declare(?AWSET_PS),

    spawn_link(fun() ->
                {ok, _} = lasp:read_any([{L5, {strict, undefined}}, {L6, {strict, undefined}}]),
                Self ! read_any
        end),

    {ok, _} = lasp:update(L5, {add, 1}, a),

    receive
        read_any ->
            ok
    end.
