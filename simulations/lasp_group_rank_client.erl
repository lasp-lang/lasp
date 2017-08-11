%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Christopher S. Meiklejohn.  All Rights Reserved.
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

-module(lasp_group_rank_client).

-author("Junghun Yoo <junghun.yoo@cs.ox.ac.uk>").

-behaviour(gen_server).

%% API
-export([start_link/0]).
%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-include("lasp.hrl").
-include("lasp_group_rank.hrl").

%% State record.
-record(
    state,
    {actor,
        client_num,
        input_updates,
        input_user_set_id,
        input_group_set_id,
        expected_result,
        completed_clients}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start and link to calling process.
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([term()]) -> {ok, #state{}}.
init([]) ->
    lager:info("Group rank calculator client initialized."),

    %% Generate actor identifier.
    Actor = node(),

    Node = atom_to_list(Actor),
    {ClientNum, _Rest} =
        string:to_integer(
            string:substr(Node, length("client_") + 1, length(Node))),

    lager:info("ClientNum: ~p", [ClientNum]),

    {IdInputUserSet, TypeInputUserSet} = ?SET_USER_ID_TO_POINTS,
    {ok, {SetUserIdToPoints, _, _, _}} =
        lasp:declare(IdInputUserSet, TypeInputUserSet),
    {IdInputGroupSet, TypeInputGroupSet} = ?SET_GROUP_ID_TO_USERS,
    {ok, {SetGroupIdToUsers, _, _, _}} =
        lasp:declare(IdInputGroupSet, TypeInputGroupSet),

    %% Schedule input CRDT updates.
    schedule_input_update(),

    %% Build DAG.
    case lasp_config:get(heavy_client, false) of
        true ->
            build_dag(SetUserIdToPoints, SetGroupIdToUsers);
        false ->
            ok
    end,

    %% Schedule logging.
    schedule_logging(),

    %% Calculate the expected result.
    {ok, ExpectedResult} = calc_expected_result(Actor),

    %% Create instance for clients completion tracking
    {Id, Type} = ?COUNTER_COMPLETED_CLIENTS,
    {ok, {CompletedClients, _, _, _}} = lasp:declare(Id, Type),

    {ok,
        #state{
            actor=Actor,
            client_num=ClientNum,
            input_updates=0,
            input_user_set_id=SetUserIdToPoints,
            input_group_set_id=SetGroupIdToUsers,
            expected_result=ExpectedResult,
            completed_clients=CompletedClients}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.
handle_call(Msg, _From, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {reply, ok, State}.

%% @private
-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
handle_cast(Msg, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info(
    log, #state{actor=Actor, input_updates=InputUpdates}=State) ->
    lasp_marathon_simulations:log_message_queue_size("log"),

    %% Get current value of the input and output CRDTs.
    {ok, SetGroupIdToUsers} = lasp:query(?SET_GROUP_ID_TO_USERS),
    {ok, SetAny} = lasp:query(?SET_GROUP_AND_ITS_RANK),

    lager:info(
        "InputUpdates: ~p, current number of groups: ~p, node: ~p, result: ~p",
        [InputUpdates, sets:size(SetGroupIdToUsers), Actor, SetAny]),

    %% Schedule logging.
    schedule_logging(),

    {noreply, State};

handle_info(
    input_update,
    #state{
        actor=Actor,
        client_num=ClientNum,
        input_updates=InputUpdates0,
        input_user_set_id=SetUserIdToPoints,
        input_group_set_id=SetGroupIdToUsers}=State) ->
    lasp_marathon_simulations:log_message_queue_size("input_update"),

    InputUpdates1 = InputUpdates0 + 1,

    lager:info("Current update: ~p, node: ~p", [InputUpdates1, Actor]),

    CurInput =
        lists:nth(InputUpdates1, lists:nth(ClientNum, ?INPUT_DATA)),
    {PrevInput, _} =
        lists:foldl(
            fun({Id, _}=Input, {AccPrevInput, AccIndex}) ->
                {CurId, _} = CurInput,
                NewPrevInput =
                    case AccIndex < InputUpdates1 andalso Id == CurId of
                        true ->
                            Input;
                        false ->
                            AccPrevInput
                    end,
                {NewPrevInput, AccIndex + 1}
            end,
            {undefined, 1},
            lists:nth(ClientNum, ?INPUT_DATA)),
    {CurId, _} = CurInput,
    case PrevInput of
        undefined ->
            case string:substr(CurId, 1, 1) of
                "u" ->
                    {ok, _} =
                        lasp:update(SetUserIdToPoints, {add, CurInput}, Actor);
                "g" ->
                    {ok, _} =
                        lasp:update(SetGroupIdToUsers, {add, CurInput}, Actor)
            end;
        _ ->
            case string:substr(CurId, 1, 1) of
                "u" ->
                    {ok, _} =
                        lasp:update(SetUserIdToPoints, {rmv, PrevInput}, Actor),
                    {ok, _} =
                        lasp:update(SetUserIdToPoints, {add, CurInput}, Actor);
                "g" ->
                    {ok, _} =
                        lasp:update(SetGroupIdToUsers, {rmv, PrevInput}, Actor),
                    {ok, _} =
                        lasp:update(SetGroupIdToUsers, {add, CurInput}, Actor)
            end
    end,

    MaxImpressions = lasp_config:get(max_impressions, ?MAX_IMPRESSIONS_DEFAULT),
    case InputUpdates1 == MaxImpressions of
        true ->
            lager:info("All updates are done. Node: ~p", [Actor]),
            log_convergence(),
            schedule_check_simulation_end();
        false ->
            schedule_input_update()
    end,

    {noreply, State#state{input_updates=InputUpdates1}};

handle_info(
    check_simulation_end,
    #state{
        actor=Actor,
        expected_result=ExpectedResult,
        completed_clients=CompletedClients}=State) ->
    lasp_marathon_simulations:log_message_queue_size("check_simulation_end"),

    %% A simulation ends for the server when the result CRDT is same as the
    %% expected result.
    {ok, SetGroupAndItsRank} = lasp:query(?SET_GROUP_AND_ITS_RANK),

    lager:info(
        "Checking for simulation end: current result: ~p.",
        [SetGroupAndItsRank]),

    {ok, CounterCompletedClients} = lasp:query(CompletedClients),

    lager:info(
        "Checking for simulation end: completed clients: ~p.",
        [CounterCompletedClients]),

    case SetGroupAndItsRank == ExpectedResult of
        true ->
            lager:info("This node gets the expected result. Node ~p", [Actor]),
            lasp_instrumentation:stop(),
            lasp_support:push_logs(),
            lasp:update(CompletedClients, increment, Actor);
        false ->
            schedule_check_simulation_end()
    end,

    {noreply, State};

handle_info(Msg, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec terminate(term(), #state{}) -> term().
terminate(_Reason, _State) ->
    ok.

%% @private
-spec code_change(term() | {down, term()}, #state{}, term()) -> {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
schedule_input_update() ->
    timer:send_after(?IMPRESSION_INTERVAL, input_update).

%% @private
schedule_logging() ->
    timer:send_after(?LOG_INTERVAL, log).

%% @private
schedule_check_simulation_end() ->
    timer:send_after(?STATUS_INTERVAL, check_simulation_end).

%% @private
log_convergence() ->
    case lasp_config:get(instrumentation, false) of
        true ->
            lasp_instrumentation:convergence();
        false ->
            ok
    end.

%% @private
calc_expected_result(Actor) ->
    %% [{user1, 1000}, {user2, 2000}, ...]
    {ok, {SetUserIdToPoints, _, _, _}} = lasp:declare(ps_aworset),
    %% [{group1, {user1, user2}}, {group2, {user3}}, ...]
    {ok, {SetGroupIdToUsers, _, _, _}} = lasp:declare(ps_aworset),
    %% [[5,15,30,100]] -> top 5%: Gold, next 10%: Silver. next 15%: Bronze and
    %%     next 70%: Black
    {ok, {RegisterDivider, _, _, _}} = lasp:declare(ps_lwwregister),

    %% Read INPUT_DATA
    ok =
        lists:foreach(
            fun(ListInput) ->
                lists:foldl(
                    fun(CurInput, AccCurIndex) ->
                        {PrevInput, _} =
                            lists:foldl(
                                fun({Id, _}=Input, {AccPrevInput, AccIndex}) ->
                                    {CurId, _} = CurInput,
                                    NewPrevInput =
                                        case AccIndex < AccCurIndex andalso
                                            Id == CurId of
                                            true ->
                                                Input;
                                            false ->
                                                AccPrevInput
                                        end,
                                    {NewPrevInput, AccIndex + 1}
                                end,
                                {undefined, 1},
                                ListInput),
                        {CurId, _} = CurInput,
                        case PrevInput of
                            undefined ->
                                case string:substr(CurId, 1, 1) of
                                    "u" ->
                                        {ok, _} =
                                            lasp:update(
                                                SetUserIdToPoints,
                                                {add, CurInput},
                                                Actor);
                                    "g" ->
                                        {ok, _} =
                                            lasp:update(
                                                SetGroupIdToUsers,
                                                {add, CurInput},
                                                Actor)
                                end;
                            _ ->
                                case string:substr(CurId, 1, 1) of
                                    "u" ->
                                        {ok, _} =
                                            lasp:update(
                                                SetUserIdToPoints,
                                                {rmv, PrevInput},
                                                Actor),
                                        {ok, _} =
                                            lasp:update(
                                                SetUserIdToPoints,
                                                {add, CurInput},
                                                Actor);
                                    "g" ->
                                        {ok, _} =
                                            lasp:update(
                                                SetGroupIdToUsers,
                                                {rmv, PrevInput},
                                                Actor),
                                        {ok, _} =
                                            lasp:update(
                                                SetGroupIdToUsers,
                                                {add, CurInput},
                                                Actor)
                                end
                        end,
                        AccCurIndex + 1
                    end,
                    1,
                    ListInput),
                ok
            end,
            ?INPUT_DATA),

    %% top 25%: Gold, next 25%: Silver. next 50%: Bronze
    {ok, _} =
        lasp:update(RegisterDivider, {set, [25, 50, 100]}, Actor),

    {ok, {SetGroupAndUsers, _, _, _}} = lasp:declare(ps_aworset),

    %% Apply product.
    ok = lasp:product(SetGroupIdToUsers, SetUserIdToPoints, SetGroupAndUsers),

    %% Sleep.
    timer:sleep(400),

    {ok, {SetGroupAndItsUsers, _, _, _}} = lasp:declare(ps_aworset),

    %% Apply filter.
    ok =
        lasp:filter(
            SetGroupAndUsers,
            fun({{_GroupId, UserSet}, {UserId, _Points}}) ->
                ordsets:is_element(UserId, UserSet)
            end,
            SetGroupAndItsUsers),

    %% Wait.
    timer:sleep(400),

    {ok, {GroupBySetGroupAndItsUsers, _, _, _}} = lasp:declare(ps_group_by_orset),

    %% Apply group_by.
    ok = lasp:group_by_first(SetGroupAndItsUsers, GroupBySetGroupAndItsUsers),

    %% Sleep.
    timer:sleep(400),

    {ok, {GroupBySetGroupAndItsPoints, _, _, _}} = lasp:declare(ps_group_by_orset),

    %% Apply map.
    ok =
        lasp:map(
            GroupBySetGroupAndItsUsers,
            fun({{GroupId, _UserSet}, UserIdAndPointsSet}) ->
                GroupPoints =
                    ordsets:fold(
                        fun({_UserId, Points}, AccGroupPoints) ->
                            AccGroupPoints + Points
                        end,
                        0,
                        UserIdAndPointsSet),
                {GroupId, GroupPoints}
            end,
            GroupBySetGroupAndItsPoints),

    %% Wait.
    timer:sleep(400),

    {ok, {SingletonGroupAndItsPoints, _, _, _}} =
        lasp:declare(ps_singleton_orset),

    ok =
        lasp:singleton(
            GroupBySetGroupAndItsPoints, SingletonGroupAndItsPoints),

    %% Sleep.
    timer:sleep(400),

    {ok, {SizeTSetGroupIdToUsers, _, _, _}} = lasp:declare(ps_size_t),

    %% Apply length.
    ok = lasp:length(SetGroupIdToUsers, SizeTSetGroupIdToUsers),

    %% Sleep.
    timer:sleep(400),

    {ok, {SetGroupSizeAndDivider, _, _, _}} = lasp:declare(ps_aworset),

    %% Apply product.
    ok =
        lasp:product(
            SizeTSetGroupIdToUsers, RegisterDivider, SetGroupSizeAndDivider),

    %% Sleep.
    timer:sleep(400),

    {ok, {SetDividerWithRank, _, _, _}} = lasp:declare(ps_aworset),

    %% Apply map.
    ok =
        lasp:map(
            SetGroupSizeAndDivider,
            fun({SizeTNumOfGroups, Divider}) ->
                NumOfGroups = SizeTNumOfGroups,
                RankDivider =
                    lists:foldl(
                        fun(Percents, AccNewDivider) ->
                            Rank = (Percents * NumOfGroups) div 100,
                            AccNewDivider ++ [Rank]
                        end,
                        [],
                        Divider),
                RankDivider
            end,
            SetDividerWithRank),

    %% Wait.
    timer:sleep(400),

    {ok, {SetGroupPointsAndDivider, _, _, _}} = lasp:declare(ps_aworset),

    %% Apply product.
    ok =
        lasp:product(
            SingletonGroupAndItsPoints,
            SetDividerWithRank,
            SetGroupPointsAndDivider),

    %% Sleep.
    timer:sleep(400),

    {ok, {SetGroupAndItsRank, _, _, _}} = lasp:declare(ps_aworset),

    %% Apply map.
    ok =
        lasp:map(
            SetGroupPointsAndDivider,
            fun({ListGroupAndItsPoints, DividerWithRank}) ->
                SortedListGroupAndItsPoints =
                    lists:sort(
                        fun({_GroupIdA, GroupPointsA},
                            {_GroupIdB, GroupPointsB}) ->
                            GroupPointsA =< GroupPointsB
                        end,
                        ListGroupAndItsPoints),
                {GroupAndItsRank, _Index, _Rank} =
                    lists:foldl(
                        fun({GroupId, _GroupPoints},
                            {AccGroupAndItsRank, AccIndex, AccRank}) ->
                            CurRank = lists:nth(AccRank, DividerWithRank),
                            NewGroupAndItsRank =
                                ordsets:add_element(
                                    {GroupId, AccRank}, AccGroupAndItsRank),
                            NewRank =
                                case AccIndex >= CurRank of
                                    true ->
                                        AccRank + 1;
                                    false ->
                                        AccRank
                                end,
                            NewIndex = AccIndex + 1,
                            {NewGroupAndItsRank, NewIndex, NewRank}
                        end,
                        {ordsets:new(), 1, 1},
                        SortedListGroupAndItsPoints),
                GroupAndItsRank
            end,
            SetGroupAndItsRank),

    %% Wait.
    timer:sleep(400),

    %% Read.
    {ok, {_, _, _, SetGroupAndItsRank0}} =
        lasp:read(SetGroupAndItsRank, undefined),

    %% Read value.
    SetGroupAndItsRankV =
        lasp_type:query(ps_aworset, SetGroupAndItsRank0),

    lager:info("Expected result: ~p", [SetGroupAndItsRankV]),

    {ok, SetGroupAndItsRankV}.

%% @private
build_dag(SetUserIdToPoints, SetGroupIdToUsers) ->
%%    %% [{user1, 1000}, {user2, 2000}, ...]
%%    {Id1, Type1} = ?SET_USER_ID_TO_POINTS,
%%    {ok, {SetUserIdToPoints, _, _, _}} = lasp:declare(Id1, Type1),
%%    %% [{group1, {user1, user2}}, {group2, {user3}}, ...]
%%    {Id2, Type2} = ?SET_GROUP_ID_TO_USERS,
%%    {ok, {SetGroupIdToUsers, _, _, _}} = lasp:declare(Id2, Type2),
    %% [[5,15,30,100]] -> top 5%: Gold, next 10%: Silver. next 15%: Bronze and
    %%     next 70%: Black
    {Id3, Type3} = ?REGISTER_DIVIDER,
    {ok, {RegisterDivider, _, _, _}} = lasp:declare(Id3, Type3),

    {Id4, Type4} = ?SET_GROUP_AND_USERS,
    {ok, {SetGroupAndUsers, _, _, _}} = lasp:declare(Id4, Type4),

    %% Apply product.
    ok = lasp:product(SetGroupIdToUsers, SetUserIdToPoints, SetGroupAndUsers),

    {Id5, Type5} = ?SET_GROUP_AND_ITS_USERS,
    {ok, {SetGroupAndItsUsers, _, _, _}} = lasp:declare(Id5, Type5),

    %% Apply filter.
    ok =
        lasp:filter(
            SetGroupAndUsers,
            fun({{_GroupId, UserSet}, {UserId, _Points}}) ->
                ordsets:is_element(UserId, UserSet)
            end,
            SetGroupAndItsUsers),

    {Id6, Type6} = ?GROUP_BY_SET_GROUP_AND_ITS_USERS,
    {ok, {GroupBySetGroupAndItsUsers, _, _, _}} = lasp:declare(Id6, Type6),

    %% Apply group_by.
    ok = lasp:group_by_first(SetGroupAndItsUsers, GroupBySetGroupAndItsUsers),

    {Id7, Type7} = ?GROUP_BY_SET_GROUP_AND_ITS_POINTS,
    {ok, {GroupBySetGroupAndItsPoints, _, _, _}} = lasp:declare(Id7, Type7),

    %% Apply map.
    ok =
        lasp:map(
            GroupBySetGroupAndItsUsers,
            fun({{GroupId, _UserSet}, UserIdAndPointsSet}) ->
                GroupPoints =
                    ordsets:fold(
                        fun({_UserId, Points}, AccGroupPoints) ->
                            AccGroupPoints + Points
                        end,
                        0,
                        UserIdAndPointsSet),
                {GroupId, GroupPoints}
            end,
            GroupBySetGroupAndItsPoints),

    {Id8, Type8} = ?SINGLETON_GROUP_AND_ITS_POINTS,
    {ok, {SingletonGroupAndItsPoints, _, _, _}} =
        lasp:declare(Id8, Type8),

    ok =
        lasp:singleton(
            GroupBySetGroupAndItsPoints, SingletonGroupAndItsPoints),

    {Id9, Type9} = ?SIZE_T_SET_GROUP_ID_TO_USERS,
    {ok, {SizeTSetGroupIdToUsers, _, _, _}} = lasp:declare(Id9, Type9),

    %% Apply length.
    ok = lasp:length(SetGroupIdToUsers, SizeTSetGroupIdToUsers),

    {Id10, Type10} = ?SET_GROUP_SIZE_AND_DIVIDER,
    {ok, {SetGroupSizeAndDivider, _, _, _}} = lasp:declare(Id10, Type10),

    %% Apply product.
    ok =
        lasp:product(
            SizeTSetGroupIdToUsers, RegisterDivider, SetGroupSizeAndDivider),

    {Id11, Type11} = ?SET_DIVIDER_WITH_RANK,
    {ok, {SetDividerWithRank, _, _, _}} = lasp:declare(Id11, Type11),

    %% Apply map.
    ok =
        lasp:map(
            SetGroupSizeAndDivider,
            fun({SizeTNumOfGroups, Divider}) ->
                NumOfGroups = SizeTNumOfGroups,
                RankDivider =
                    lists:foldl(
                        fun(Percents, AccNewDivider) ->
                            Rank = (Percents * NumOfGroups) div 100,
                            AccNewDivider ++ [Rank]
                        end,
                        [],
                        Divider),
                RankDivider
            end,
            SetDividerWithRank),

    {Id12, Type12} = ?SET_GROUP_POINTS_AND_DIVIDER,
    {ok, {SetGroupPointsAndDivider, _, _, _}} = lasp:declare(Id12, Type12),

    %% Apply product.
    ok =
        lasp:product(
            SingletonGroupAndItsPoints,
            SetDividerWithRank,
            SetGroupPointsAndDivider),

    {Id13, Type13} = ?SET_GROUP_AND_ITS_RANK,
    {ok, {SetGroupAndItsRank, _, _, _}} = lasp:declare(Id13, Type13),

    %% Apply map.
    ok =
        lasp:map(
            SetGroupPointsAndDivider,
            fun({ListGroupAndItsPoints, DividerWithRank}) ->
                SortedListGroupAndItsPoints =
                    lists:sort(
                        fun({_GroupIdA, GroupPointsA},
                            {_GroupIdB, GroupPointsB}) ->
                            GroupPointsA =< GroupPointsB
                        end,
                        ListGroupAndItsPoints),
                {GroupAndItsRank, _Index, _Rank} =
                    lists:foldl(
                        fun({GroupId, _GroupPoints},
                            {AccGroupAndItsRank, AccIndex, AccRank}) ->
                            CurRank = lists:nth(AccRank, DividerWithRank),
                            NewGroupAndItsRank =
                                ordsets:add_element(
                                    {GroupId, AccRank}, AccGroupAndItsRank),
                            NewRank =
                                case AccIndex >= CurRank of
                                    true ->
                                        AccRank + 1;
                                    false ->
                                        AccRank
                                end,
                            NewIndex = AccIndex + 1,
                            {NewGroupAndItsRank, NewIndex, NewRank}
                        end,
                        {ordsets:new(), 1, 1},
                        SortedListGroupAndItsPoints),
                GroupAndItsRank
            end,
            SetGroupAndItsRank),

    ok.
