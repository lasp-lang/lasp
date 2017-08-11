-define(
INPUT_DATA,
    [
        [{"user1", 1000}, {"user4", 1000}, {"user7", 1000},
            {"group1", ["user1", "user4"]}, {"user1", 2000}, {"user4", 3000},
            {"group1", ["user1", "user4", "user7"]},
            {"group1", ["user1", "user7"]}, {"user10", 5000},
            {"group4", ["user10"]}],
        [{"user2", 1000}, {"user5", 1000}, {"user8", 1000},
            {"group2", ["user2", "user5"]}, {"user2", 7000}, {"user5", 2000},
            {"group2", ["user2", "user5", "user8"]},
            {"group2", ["user5", "user8"]}, {"user11", 8000},
            {"group5", ["user11"]}],
        [{"user3", 1000}, {"user6", 1000}, {"user9", 1000},
            {"group3", ["user3", "user6"]}, {"user3", 6000}, {"user6", 9000},
            {"group3", ["user3", "user6", "user9"]},
            {"group3", ["user3", "user6"]}, {"user12", 3000},
            {"group6", ["user12"]}]]).
-define(INPUT_DATA_DIVIDER, [25, 50, 100]).

%% Input type ids.
-define(SET_USER_ID_TO_POINTS, {<<"set_user_id_to_points">>, ps_aworset}).
-define(SET_GROUP_ID_TO_USERS, {<<"set_group_id_to_users">>, ps_aworset}).
-define(REGISTER_DIVIDER, {<<"register_divider">>, ps_lwwregister}).

%% Intermediate set ids.
-define(SET_GROUP_AND_USERS, {<<"set_group_and_users">>, ps_aworset}).
-define(SET_GROUP_AND_ITS_USERS, {<<"set_group_and_its_users">>, ps_aworset}).
-define(
    GROUP_BY_SET_GROUP_AND_ITS_USERS,
    {<<"group_by_set_group_and_its_users">>, ps_group_by_orset}).
-define(
    GROUP_BY_SET_GROUP_AND_ITS_POINTS,
    {<<"group_by_set_group_and_its_points">>, ps_group_by_orset}).
-define(
    SINGLETON_GROUP_AND_ITS_POINTS,
    {<<"singleton_group_and_its_points">>, ps_singleton_orset}).
-define(
    SIZE_T_SET_GROUP_ID_TO_USERS,
    {<<"size_t_set_group_id_to_users">>, ps_size_t}).
-define(
    SET_GROUP_SIZE_AND_DIVIDER, {<<"set_group_size_and_divider">>, ps_aworset}).
-define(SET_DIVIDER_WITH_RANK, {<<"set_divider_with_rank">>, ps_aworset}).
-define(
    SET_GROUP_POINTS_AND_DIVIDER,
    {<<"set_group_points_and_divider">>, ps_aworset}).

%% Result set id.
-define(SET_GROUP_AND_ITS_RANK, {<<"set_group_and_its_rank">>, ps_aworset}).

%% A counter for completed clients.
-define(
    COUNTER_COMPLETED_CLIENTS, {<<"counter_completed_clients">>, ps_gcounter}).
