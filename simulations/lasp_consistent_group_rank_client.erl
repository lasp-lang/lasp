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

-module(lasp_consistent_group_rank_client).

-author("Junghun Yoo <junghun.yoo@cs.ox.ac.uk>").

-behaviour(gen_server).

%% API
-export([
    start_link/0]).
%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-include("lasp.hrl").
-include("lasp_ext_group_rank.hrl").

%% State record.
-record(state, {actor, client_number, input_processing, value, subset, cds, expected}).

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
    lager:info("Consistent group rank client initialized."),

    %% Generate actor identifier.
    Actor = erlang:atom_to_list(node()),

    ClientNum = lasp_config:get(group_rank_client_index, 1),
    lager:info("ClientNum: ~p", [ClientNum]),

    %% Schedule group rank input processing.
    schedule_input_processing(),

    %% Build DAG.
    case lasp_config:get(ext_type_version, ext_type_orset_base_v1) of
        ext_type_orset_base_v7 ->
            build_dag_simple();
        _ ->
            build_dag()
    end,

    %% Schedule logging.
    schedule_logging(),

    {AggGroupRankId, AggGroupRankType} =
        case lasp_config:get(ext_type_version, ext_type_orset_base_v1) of
            ext_type_orset_base_v7 ->
                ?S_GROUP_X_USER_F_G_O__X__GROUP_C_X_DIVIDER_M__M;
            _ ->
                ?GROUP_X_USER_F_G_O__X__GROUP_C_X_DIVIDER_M__M
        end,
    InitSubset = ordsets:new(),
    InitCDS = ordsets:new(),
    InitValue = lasp_type:new(AggGroupRankType, AggGroupRankId),

    inflation_check({strict, undefined}),

    ExpectedResult = calcExpectedResult(),

    {
        ok,
        #state{
            actor=Actor,
            client_number=ClientNum,
            input_processing=0,
            value=InitValue,
            subset=InitSubset,
            cds=InitCDS,
            expected=ExpectedResult}}.

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
    log,
    #state{
        actor=Actor,
        input_processing=InputProcessing,
        value=Value,
        subset=Subset,
        cds=CDS}=State) ->
    lasp_marathon_simulations:log_message_queue_size("log"),

    %% Get current value.
    {NewSubset, NewCDS, ReadResult} =
        lasp_type:query_ext_consistent(?EXT_AWORSET_INTERMEDIATE_TYPE, {Subset, CDS, Value}),

    lager:info(
        "InputProcessing: ~p, consistent read result: ~p, node: ~p",
        [InputProcessing, ReadResult, Actor]),

    %% Schedule advertisement counter impression.
    schedule_logging(),

    {noreply, State#state{subset=NewSubset, cds=NewCDS}};

handle_info({inflated_value, Value}, #state{actor=Actor, subset=Subset, cds=CDS}=State) ->
    lasp_marathon_simulations:log_message_queue_size("inflated_value"),

    %% Get current value.
    {NewSubset, NewCDS, ReadResult} =
        lasp_type:query_ext_consistent(?EXT_AWORSET_INTERMEDIATE_TYPE, {Subset, CDS, Value}),

    lager:info("Inflated value: ~p, node: ~p", [ReadResult, Actor]),

    {ResultId, _ResultType} =
        case lasp_config:get(ext_type_version, ext_type_orset_base_v1) of
            ext_type_orset_base_v7 ->
                ?S_GROUP_X_USER_F_G_O__X__GROUP_C_X_DIVIDER_M__M;
            _ ->
                ?GROUP_X_USER_F_G_O__X__GROUP_C_X_DIVIDER_M__M
        end,
    lasp_instrumentation:meta_size(ResultId, Value),

    inflation_check({strict, Value}),

    {noreply, State#state{value=Value, subset=NewSubset, cds=NewCDS}};

handle_info(
    input_process,
    #state{
        actor=Actor,
        client_number=ClientNum,
        input_processing=InputProcessing0}=State) ->
    lasp_marathon_simulations:log_message_queue_size("input_process"),

    case (InputProcessing0 * client_number()) + ClientNum =< ?INPUT_SIZE of
        true ->
            {InputId, InputData} =
                case lasp_config:get(ext_type_version, ext_type_orset_base_v1) of
                    ext_type_orset_base_v7 ->
                        lists:nth(
                            (InputProcessing0 * client_number()) + ClientNum,
                            ?S_INPUT_DATA);
                    _ ->
                        lists:nth(
                            (InputProcessing0 * client_number()) + ClientNum,
                            ?INPUT_DATA)
                end,
            lager:info(
                "InputProcessing: ~p, current input: ~p, node: ~p",
                [InputProcessing0, {InputId, InputData}, Actor]),
            {ok, _} = lasp:update(InputId, InputData, Actor),
            schedule_input_processing();
        false ->
            lager:info("All inputs are processed. Node: ~p", [Actor]),

            %% Update Simulation Status Instance
            lasp:update(?SIM_STATUS_ID, {apply, Actor, {fst, true}}, Actor),
            log_convergence(),
            schedule_check_simulation_end()
    end,

    {noreply, State#state{input_processing=InputProcessing0 + 1}};

handle_info(
    check_simulation_end,
    #state{
        actor=Actor,
        input_processing=InputProcessing,
        value=Value,
        subset=Subset,
        cds=CDS,
        expected=ExpectedResult}=State) ->
    lasp_marathon_simulations:log_message_queue_size("check_simulation_end"),

    %% Get current value.
    {NewSubset, NewCDS, ReadResult} =
        lasp_type:query_ext_consistent(?EXT_AWORSET_INTERMEDIATE_TYPE, {Subset, CDS, Value}),

    lager:info(
        "InputProcessing: ~p, consistent read result: ~p, node: ~p",
        [InputProcessing, ReadResult, Actor]),

    %% A simulation ends for clients when all clients have
    %% observed all ads disabled (first component of the map in
    %% the simulation status instance is true for all clients)
    {ok, InputProcessedAndLogs} = lasp:query(?SIM_STATUS_ID),

    NodesWithInputProcessed =
        lists:filter(
            fun({_Node, {InputProcessed, _LogsPushed}}) ->
                InputProcessed
            end,
            InputProcessedAndLogs),

    case ReadResult == ExpectedResult andalso
        length(NodesWithInputProcessed) == client_number() of
        true ->
            lager:info("All nodes processed their inputs. Node ~p", [Actor]),
            lasp_instrumentation:stop(),
            lasp_support:push_logs(),
            lasp:update(?SIM_STATUS_ID, {apply, Actor, {snd, true}}, Actor);
        false ->
            schedule_check_simulation_end()
    end,

    {noreply, State#state{subset=NewSubset, cds=NewCDS}};

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
inflation_check(Threshold) ->
    Me = self(),
    spawn(
        fun() ->
            {ok, {_, _, _, Value}} =
                case lasp_config:get(ext_type_version, ext_type_orset_base_v1) of
                    ext_type_orset_base_v7 ->
                        lasp:read(?S_GROUP_X_USER_F_G_O__X__GROUP_C_X_DIVIDER_M__M, Threshold);
                    _ ->
                        lasp:read(?GROUP_X_USER_F_G_O__X__GROUP_C_X_DIVIDER_M__M, Threshold)
                end,
            Me ! {inflated_value, Value}
        end).

%% @private
schedule_input_processing() ->
    timer:send_after(?INPUT_INTERVAL, input_process).

%% @private
schedule_logging() ->
    timer:send_after(?LOG_INTERVAL, log).

%% @private
schedule_check_simulation_end() ->
    timer:send_after(?STATUS_INTERVAL, check_simulation_end).

%% @private
client_number() ->
    lasp_config:get(client_number, 3).

%% @private
log_convergence() ->
    case lasp_config:get(instrumentation, false) of
        true ->
            lasp_instrumentation:convergence();
        false ->
            ok
    end.

%% @private
build_dag() ->
    {InputSetUserId, InputSetUserType} = ?USER_INFO,
    {ok, _} = lasp:declare(InputSetUserId, InputSetUserType),
    {InputSetGroupId, InputSetGroupType} = ?GROUP_INFO,
    {ok, _} = lasp:declare(InputSetGroupId, InputSetGroupType),
    {InputRegisterDividerId, InputRegisterDividerType} = ?DIVIDER,
    {ok, _} = lasp:declare(InputRegisterDividerId, InputRegisterDividerType),

    {InterSetGroupXUserId, InterSetGroupXUserType} = ?GROUP_X_USER,
    {ok, _} = lasp:declare(InterSetGroupXUserId, InterSetGroupXUserType),

    ok = lasp:product(?GROUP_INFO, ?USER_INFO, ?GROUP_X_USER),

    {InterSetGroupXUserMatchId, InterSetGroupXUserMatchType} = ?GROUP_X_USER_F,
    {ok, _} = lasp:declare(InterSetGroupXUserMatchId, InterSetGroupXUserMatchType),

    ok =
        lasp:filter(
            ?GROUP_X_USER,
            fun({{_GroupId, UserIdSet}, {UserId, _UserPoints}}) ->
                ordsets:is_element(UserId, UserIdSet)
            end,
            ?GROUP_X_USER_F),

    {GroupBySumGroupXPointsId, GroupBySumGroupXPointsType} = ?GROUP_X_USER_F_G,
    {ok, _} = lasp:declare(GroupBySumGroupXPointsId, GroupBySumGroupXPointsType),

    ok =
        lasp:group_by_sum(
            ?GROUP_X_USER_F,
            fun(undefined, {UserIdR, UserPointsR}) ->
                    {
                        ordsets:add_element(UserIdR, ordsets:new()),
                        0 + UserPointsR};
                ({UserIdL, UserPointsL}, {UserIdR, UserPointsR}) ->
                    {
                        ordsets:add_element(UserIdR, UserIdL),
                        UserPointsL + UserPointsR}
            end,
            ?GROUP_X_USER_F_G),

    {OrderedGroupBySumGroupXPointsId, OrderedGroupBySumGroupXPointsType} = ?GROUP_X_USER_F_G_O,
    {ok, _} = lasp:declare(OrderedGroupBySumGroupXPointsId, OrderedGroupBySumGroupXPointsType),

    ok =
        lasp:order_by(
            ?GROUP_X_USER_F_G,
            fun({{_GroupIdL, _GroupUsersL}, {_GroupUsersSumedL, GroupPointsL}}=_ElemL,
                {{_GroupIdR, _GroupUsersR}, {_GroupUsersSumedR, GroupPointsR}}=_ElemR) ->
                GroupPointsL >= GroupPointsR
            end,
            ?GROUP_X_USER_F_G_O),

    {SetCountGroupId, SetCountGroupType} = ?GROUP_C,
    {ok, _} = lasp:declare(SetCountGroupId, SetCountGroupType),

    ok = lasp:set_count(?GROUP_INFO, ?GROUP_C),

    {AggResultGroupCountXDividerId, AggResultGroupCountXDividerType} = ?GROUP_C_X_DIVIDER,
    {ok, _} = lasp:declare(AggResultGroupCountXDividerId, AggResultGroupCountXDividerType),

    ok = lasp:product(?GROUP_C, ?DIVIDER, ?GROUP_C_X_DIVIDER),

    {AggGroupCountXDividerRankId, AggGroupCountXDividerRankType} = ?GROUP_C_X_DIVIDER_M,
    {ok, _} = lasp:declare(AggGroupCountXDividerRankId, AggGroupCountXDividerRankType),

    ok =
        lasp:map(
            ?GROUP_C_X_DIVIDER,
            fun({GroupCount, Divider}) ->
                {RankDivider, 100} =
                    lists:foldl(
                        fun(Border, {AccInRankDivider, AccInBorderSum}) ->
                            NewBorderSum = AccInBorderSum + Border,
                            Rank = GroupCount * NewBorderSum div 100,
                            {AccInRankDivider ++ [Rank], NewBorderSum}
                        end,
                        {[], 0},
                        Divider),
                RankDivider
            end,
            ?GROUP_C_X_DIVIDER_M),

    {
        AggResultGroupPointsOrderedXRankDividerId,
        AggResultGroupPointsOrderedXRankDividerType} = ?GROUP_X_USER_F_G_O__X__GROUP_C_X_DIVIDER_M,
    {ok, _} =
        lasp:declare(
            AggResultGroupPointsOrderedXRankDividerId,
            AggResultGroupPointsOrderedXRankDividerType),

    ok =
        lasp:product(
            ?GROUP_X_USER_F_G_O,
            ?GROUP_C_X_DIVIDER_M,
            ?GROUP_X_USER_F_G_O__X__GROUP_C_X_DIVIDER_M),

    {
        AggGroupRankId,
        AggGroupRankType} = ?GROUP_X_USER_F_G_O__X__GROUP_C_X_DIVIDER_M__M,
    {ok, _} = lasp:declare(AggGroupRankId, AggGroupRankType),

    ok =
        lasp:map(
            ?GROUP_X_USER_F_G_O__X__GROUP_C_X_DIVIDER_M,
            fun({GroupPointsOrdered, RankDivider}) ->
                {_Index, GroupRanked, _CurRank, _CurRankDivider} =
                    lists:foldl(
                        fun({{GroupId, _UserSet}, {_SumedUsers, _Points}},
                            {
                                AccInIndex,
                                AccInGroupRanked,
                                AccInCurRank,
                                [CurDivider | RestDividers]=AccInRankDivider}) ->
                            {NewCurRank, NewRankDivider} =
                                case AccInIndex == CurDivider of
                                    true ->
                                        {AccInCurRank + 1, RestDividers};
                                    false ->
                                        {AccInCurRank, AccInRankDivider}
                                end,
                            {
                                AccInIndex + 1,
                                ordsets:add_element(
                                    {GroupId, NewCurRank},
                                    AccInGroupRanked),
                                NewCurRank,
                                NewRankDivider};
                        (
                            {{GroupId, _UserSet}, {_SumedUsers, _Points}},
                            {AccInIndex, AccInGroupRanked, AccInCurRank, []}) ->
                            {
                                AccInIndex + 1,
                                ordsets:add_element(
                                    {GroupId, AccInCurRank},
                                    AccInGroupRanked),
                                AccInCurRank,
                                []}
                        end,
                        {0, ordsets:new(), 1, RankDivider},
                        GroupPointsOrdered),
                GroupRanked
            end,
            ?GROUP_X_USER_F_G_O__X__GROUP_C_X_DIVIDER_M__M),

    ok.

%% @private
build_dag_simple() ->
    {InputSetUserId, InputSetUserType} = ?S_USER_INFO,
    {ok, _} = lasp:declare(InputSetUserId, InputSetUserType),
    {InputSetGroupId, InputSetGroupType} = ?S_GROUP_INFO,
    {ok, _} = lasp:declare(InputSetGroupId, InputSetGroupType),
    {InputRegisterDividerId, InputRegisterDividerType} = ?S_DIVIDER,
    {ok, _} = lasp:declare(InputRegisterDividerId, InputRegisterDividerType),

    {InterSetGroupXUserId, InterSetGroupXUserType} = ?S_GROUP_X_USER,
    {ok, _} = lasp:declare(InterSetGroupXUserId, InterSetGroupXUserType),

    ok = lasp:product(?S_GROUP_INFO, ?S_USER_INFO, ?S_GROUP_X_USER),

    {InterSetGroupXUserMatchId, InterSetGroupXUserMatchType} = ?S_GROUP_X_USER_F,
    {ok, _} = lasp:declare(InterSetGroupXUserMatchId, InterSetGroupXUserMatchType),

    ok =
        lasp:filter(
            ?S_GROUP_X_USER,
            fun({{_GroupId, UserIdSet}, {UserId, _UserPoints}}) ->
                ordsets:is_element(UserId, UserIdSet)
            end,
            ?S_GROUP_X_USER_F),

    {GroupBySumGroupXPointsId, GroupBySumGroupXPointsType} = ?S_GROUP_X_USER_F_G,
    {ok, _} = lasp:declare(GroupBySumGroupXPointsId, GroupBySumGroupXPointsType),

    ok =
        lasp:group_by_sum(
            ?S_GROUP_X_USER_F,
            fun(undefined, {UserIdR, UserPointsR}) ->
                {
                    ordsets:add_element(UserIdR, ordsets:new()),
                    0 + UserPointsR};
                ({UserIdL, UserPointsL}, {UserIdR, UserPointsR}) ->
                    {
                        ordsets:add_element(UserIdR, UserIdL),
                        UserPointsL + UserPointsR}
            end,
            ?S_GROUP_X_USER_F_G),

    {OrderedGroupBySumGroupXPointsId, OrderedGroupBySumGroupXPointsType} = ?S_GROUP_X_USER_F_G_O,
    {ok, _} = lasp:declare(OrderedGroupBySumGroupXPointsId, OrderedGroupBySumGroupXPointsType),

    ok =
        lasp:order_by(
            ?S_GROUP_X_USER_F_G,
            fun({{_GroupIdL, _GroupUsersL}, {_GroupUsersSumedL, GroupPointsL}}=_ElemL,
                {{_GroupIdR, _GroupUsersR}, {_GroupUsersSumedR, GroupPointsR}}=_ElemR) ->
                GroupPointsL >= GroupPointsR
            end,
            ?S_GROUP_X_USER_F_G_O),

    {SetCountGroupId, SetCountGroupType} = ?S_GROUP_C,
    {ok, _} = lasp:declare(SetCountGroupId, SetCountGroupType),

    ok = lasp:set_count(?S_GROUP_INFO, ?S_GROUP_C),

    {AggResultGroupCountXDividerId, AggResultGroupCountXDividerType} = ?S_GROUP_C_X_DIVIDER,
    {ok, _} = lasp:declare(AggResultGroupCountXDividerId, AggResultGroupCountXDividerType),

    ok = lasp:product(?S_GROUP_C, ?S_DIVIDER, ?S_GROUP_C_X_DIVIDER),

    {AggGroupCountXDividerRankId, AggGroupCountXDividerRankType} = ?S_GROUP_C_X_DIVIDER_M,
    {ok, _} = lasp:declare(AggGroupCountXDividerRankId, AggGroupCountXDividerRankType),

    ok =
        lasp:map(
            ?S_GROUP_C_X_DIVIDER,
            fun({GroupCount, Divider}) ->
                {RankDivider, 100} =
                    lists:foldl(
                        fun(Border, {AccInRankDivider, AccInBorderSum}) ->
                            NewBorderSum = AccInBorderSum + Border,
                            Rank = GroupCount * NewBorderSum div 100,
                            {AccInRankDivider ++ [Rank], NewBorderSum}
                        end,
                        {[], 0},
                        Divider),
                RankDivider
            end,
            ?S_GROUP_C_X_DIVIDER_M),

    {
        AggResultGroupPointsOrderedXRankDividerId,
        AggResultGroupPointsOrderedXRankDividerType} =
        ?S_GROUP_X_USER_F_G_O__X__GROUP_C_X_DIVIDER_M,
    {ok, _} =
        lasp:declare(
            AggResultGroupPointsOrderedXRankDividerId,
            AggResultGroupPointsOrderedXRankDividerType),

    ok =
        lasp:product(
            ?S_GROUP_X_USER_F_G_O,
            ?S_GROUP_C_X_DIVIDER_M,
            ?S_GROUP_X_USER_F_G_O__X__GROUP_C_X_DIVIDER_M),

    {
        AggGroupRankId,
        AggGroupRankType} = ?S_GROUP_X_USER_F_G_O__X__GROUP_C_X_DIVIDER_M__M,
    {ok, _} = lasp:declare(AggGroupRankId, AggGroupRankType),

    ok =
        lasp:map(
            ?S_GROUP_X_USER_F_G_O__X__GROUP_C_X_DIVIDER_M,
            fun({GroupPointsOrdered, RankDivider}) ->
                {_Index, GroupRanked, _CurRank, _CurRankDivider} =
                    lists:foldl(
                        fun({{GroupId, _UserSet}, {_SumedUsers, _Points}},
                            {
                                AccInIndex,
                                AccInGroupRanked,
                                AccInCurRank,
                                [CurDivider | RestDividers]=AccInRankDivider}) ->
                            {NewCurRank, NewRankDivider} =
                                case AccInIndex == CurDivider of
                                    true ->
                                        {AccInCurRank + 1, RestDividers};
                                    false ->
                                        {AccInCurRank, AccInRankDivider}
                                end,
                            {
                                AccInIndex + 1,
                                ordsets:add_element(
                                    {GroupId, NewCurRank},
                                    AccInGroupRanked),
                                NewCurRank,
                                NewRankDivider};
                            (
                                {{GroupId, _UserSet}, {_SumedUsers, _Points}},
                                {AccInIndex, AccInGroupRanked, AccInCurRank, []}) ->
                                {
                                    AccInIndex + 1,
                                    ordsets:add_element(
                                        {GroupId, AccInCurRank},
                                        AccInGroupRanked),
                                    AccInCurRank,
                                    []}
                        end,
                        {0, ordsets:new(), 1, RankDivider},
                        GroupPointsOrdered),
                GroupRanked
            end,
            ?S_GROUP_X_USER_F_G_O__X__GROUP_C_X_DIVIDER_M__M),

    ok.

%% @private
calcExpectedResult() ->
    case lasp_config:get(group_rank_input_size, 9) of
        9 ->
            sets:from_list(
                [
                    ordsets:from_list(
                        [
                            {group_1, 2},
                            {group_3, 3},
                            {group_2, 3}])]);
        12 ->
            sets:from_list(
                [
                    ordsets:from_list(
                        [
                            {group_1, 2},
                            {group_3, 3},
                            {group_4, 3},
                            {group_2, 3}])]);
        15 ->
            sets:from_list(
                [
                    ordsets:from_list(
                        [
                            {group_1, 1},
                            {group_3, 2},
                            {group_4, 3},
                            {group_2, 3},
                            {group_5, 3}])])
    end.
