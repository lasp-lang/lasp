-include("lasp_ext.hrl").

-define(ATPS_INPUT, []).
-define(USER_INFO, {<<"u">>, {?EXT_AWORSET_INPUT_TYPE, [?ATPS_INPUT, {undefined, undefined}]}}).
-define(GROUP_INFO, {<<"g">>, {?EXT_AWORSET_INPUT_TYPE, [?ATPS_INPUT, {undefined, undefined}]}}).
-define(DIVIDER, {<<"d">>, {?EXT_LWWREGISTER_INPUT_TYPE, [?ATPS_INPUT, {undefined, undefined}]}}).

-define(
    ATPS_GROUP_AND_USER,
    [
        {<<"g">>, {undefined, undefined}},
        {<<"u">>, {undefined, undefined}}]).
% product(<<"g">>, <<"u">>)
-define(
    GROUP_X_USER,
    {<<"gXu">>, {?EXT_AWORSET_INTERMEDIATE_TYPE, [?ATPS_GROUP_AND_USER, {<<"g">>, <<"u">>}]}}).
-define(
    ATPS_GROUP_X_USER,
    [
        {<<"gXu">>, {<<"g">>, <<"u">>}},
        {<<"g">>, {undefined, undefined}},
        {<<"u">>, {undefined, undefined}}]).
% filter(<<"gXu">>)
-define(
    GROUP_X_USER_F,
    {<<"gXuF">>, {?EXT_AWORSET_INTERMEDIATE_TYPE, [?ATPS_GROUP_X_USER, {<<"gXu">>, undefined}]}}).
-define(
    ATPS_GROUP_X_USER_F,
    [
        {<<"gXuF">>, {<<"gXu">>, undefined}},
        {<<"gXu">>, {<<"g">>, <<"u">>}},
        {<<"g">>, {undefined, undefined}},
        {<<"u">>, {undefined, undefined}}]).
% group_by_sum(<<"gXuF">>)
-define(
    GROUP_X_USER_F_G,
    {<<"gXuFG">>, {?EXT_AWORSET_AGGRESULT_TYPE, [?ATPS_GROUP_X_USER_F, {<<"gXuF">>, undefined}]}}).
-define(
    ATPS_GROUP_X_USER_F_G,
    [
        {<<"gXuFG">>, {<<"gXuF">>, undefined}},
        {<<"gXuF">>, {<<"gXu">>, undefined}},
        {<<"gXu">>, {<<"g">>, <<"u">>}},
        {<<"g">>, {undefined, undefined}},
        {<<"u">>, {undefined, undefined}}]).
% order_by(<<"gXuFG">>)
-define(
    GROUP_X_USER_F_G_O,
    {<<"gXuFGO">>, {?EXT_AWORSET_AGGRESULT_TYPE, [?ATPS_GROUP_X_USER_F_G, {<<"gXuFG">>, undefined}]}}).
-define(
    ATPS_GROUP_X_USER_F_G_O,
    [
        {<<"gXuFGO">>, {<<"gXuFG">>, undefined}},
        {<<"gXuFG">>, {<<"gXuF">>, undefined}},
        {<<"gXuF">>, {<<"gXu">>, undefined}},
        {<<"gXu">>, {<<"g">>, <<"u">>}},
        {<<"g">>, {undefined, undefined}},
        {<<"u">>, {undefined, undefined}}]).

-define(
    ATPS_GROUP,
    [
        {<<"g">>, {undefined, undefined}}]).
% set_count(<<"g">>)
-define(
    GROUP_C,
    {<<"gC">>, {?EXT_AWORSET_AGGRESULT_TYPE, [?ATPS_GROUP, {<<"g">>, undefined}]}}).
-define(
    ATPS_GROUP_C,
    [
        {<<"gC">>, {<<"g">>, undefined}},
        {<<"g">>, {undefined, undefined}}]).
-define(
    ATPS_GROUP_C_AND_DIVIDER,
    [
        {<<"gC">>, {<<"g">>, undefined}},
        {<<"g">>, {undefined, undefined}},
        {<<"d">>, {undefined, undefined}}]).
% product(<<"gC">>, <<"d">>)
-define(
    GROUP_C_X_DIVIDER,
    {<<"gCXd">>, {?EXT_AWORSET_INTERMEDIATE_TYPE, [?ATPS_GROUP_C_AND_DIVIDER, {<<"gC">>, <<"d">>}]}}).
-define(
    ATPS_GROUP_C_X_DIVIDER,
    [
        {<<"gCXd">>, {<<"gC">>, <<"d">>}},
        {<<"gC">>, {<<"g">>, undefined}},
        {<<"g">>, {undefined, undefined}},
        {<<"d">>, {undefined, undefined}}]).
% map(<<"gCXd">>)
-define(
    GROUP_C_X_DIVIDER_M,
    {<<"gCXdM">>, {?EXT_AWORSET_INTERMEDIATE_TYPE, [?ATPS_GROUP_C_X_DIVIDER, {<<"gCXd">>, undefined}]}}).

-define(
    ATPS_GROUP_X_USER_F_G_O__AND__GROUP_C_X_DIVIDER_M,
    [
        {<<"gXuFGO">>, {<<"gXuFG">>, undefined}},
        {<<"gXuFG">>, {<<"gXuF">>, undefined}},
        {<<"gXuF">>, {<<"gXu">>, undefined}},
        {<<"gXu">>, {<<"g">>, <<"u">>}},
        {<<"g">>, {undefined, undefined}},
        {<<"u">>, {undefined, undefined}},
        {<<"gCXdM">>, {<<"gCXd">>, undefined}},
        {<<"gCXd">>, {<<"gC">>, <<"d">>}},
        {<<"gC">>, {<<"g">>, undefined}},
        {<<"g">>, {undefined, undefined}},
        {<<"d">>, {undefined, undefined}}]).
-define(
    GROUP_X_USER_F_G_O__X__GROUP_C_X_DIVIDER_M,
    {
        <<"gXuFGOXgCXdM">>,
        {
            ?EXT_AWORSET_INTERMEDIATE_TYPE,
            [?ATPS_GROUP_X_USER_F_G_O__AND__GROUP_C_X_DIVIDER_M, {<<"gXuFGO">>, <<"gCXdM">>}]}}).
-define(
    ATPS_GROUP_X_USER_F_G_O__X__GROUP_C_X_DIVIDER_M,
    [
        {<<"gXuFGOXgCXdM">>, {<<"gXuFGO">>, <<"gCXdM">>}},
        {<<"gXuFGO">>, {<<"gXuFG">>, undefined}},
        {<<"gXuFG">>, {<<"gXuF">>, undefined}},
        {<<"gXuF">>, {<<"gXu">>, undefined}},
        {<<"gXu">>, {<<"g">>, <<"u">>}},
        {<<"g">>, {undefined, undefined}},
        {<<"u">>, {undefined, undefined}},
        {<<"gCXdM">>, {<<"gCXd">>, undefined}},
        {<<"gCXd">>, {<<"gC">>, <<"d">>}},
        {<<"gC">>, {<<"g">>, undefined}},
        {<<"g">>, {undefined, undefined}},
        {<<"d">>, {undefined, undefined}}]).
% map(<<"gXuFGOXgCXdM">>)
-define(
    GROUP_X_USER_F_G_O__X__GROUP_C_X_DIVIDER_M__M,
    {
        <<"gXuFGOXgCXdMM">>,
        {
            ?EXT_AWORSET_INTERMEDIATE_TYPE,
            [?ATPS_GROUP_X_USER_F_G_O__X__GROUP_C_X_DIVIDER_M, {<<"gXuFGOXgCXdM">>, undefined}]}}).
-define(
    ATPS_GROUP_X_USER_F_G_O__X__GROUP_C_X_DIVIDER_M__M,
    [
        {<<"gXuFGOXgCXdMM">>, {<<"gXuFGOXgCXdM">>, undefined}},
        {<<"gXuFGOXgCXdM">>, {<<"gXuFGO">>, <<"gCXdM">>}},
        {<<"gXuFGO">>, {<<"gXuFG">>, undefined}},
        {<<"gXuFG">>, {<<"gXuF">>, undefined}},
        {<<"gXuF">>, {<<"gXu">>, undefined}},
        {<<"gXu">>, {<<"g">>, <<"u">>}},
        {<<"g">>, {undefined, undefined}},
        {<<"u">>, {undefined, undefined}},
        {<<"gCXdM">>, {<<"gCXd">>, undefined}},
        {<<"gCXd">>, {<<"gC">>, <<"d">>}},
        {<<"gC">>, {<<"g">>, undefined}},
        {<<"g">>, {undefined, undefined}},
        {<<"d">>, {undefined, undefined}}]).

-define(
    INPUT_DATA,
    [
        {?USER_INFO, {add,{user_1,1000}}},
        {?USER_INFO, {add,{user_2,2000}}},
        {?GROUP_INFO, {add,{group_1,ordsets:from_list([user_1,user_2])}}},
        {?USER_INFO, {add,{user_3,8000}}},
        {?GROUP_INFO, {add,{group_2,ordsets:from_list([user_3])}}},
        {?USER_INFO, {add,{user_2,9000}}},
        {?GROUP_INFO, {add,{group_3,ordsets:from_list([user_2])}}},
        {?USER_INFO, {add,{user_2,5000}}},
        {?USER_INFO, {add,{user_3,3000}}},
        {?USER_INFO, {add,{user_3,1000}}},
        {?GROUP_INFO, {add,{group_4,ordsets:from_list([user_1,user_3])}}},
        {?USER_INFO, {add,{user_1,2000}}},
        {?USER_INFO, {add,{user_2,6000}}},
        {?USER_INFO, {add,{user_1,3000}}},
        {?GROUP_INFO, {add,{group_5,ordsets:from_list([user_1])}}}]).

-define(INPUT_SIZE, lasp_config:get(group_rank_input_size, 9)).

-define(INPUT_DATA_DIVIDER, [20, 20, 60]).

-define(INPUT_INTERVAL, 10000). %% 10 seconds
