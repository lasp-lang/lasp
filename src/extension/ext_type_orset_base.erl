-module(ext_type_orset_base).

-author("Junghun Yoo <junghun.yoo@cs.ox.ac.uk>").

%% API
-export([
    is_strict_inflation/2,
    print_value/1]).

-export_type([
    ext_node_type/0,
    orset_base/0]).

-type ext_node_type() :: input | intermediate.
-type orset_base() :: term().
-type element() :: term().
-type ext_node_id() :: term().
-type ext_replica_id() :: term().
-type ext_crdt() :: term().

-callback new(ext_type_path:ext_path_info_list()) -> orset_base().

-callback equal(orset_base(), orset_base()) -> boolean().

-callback insert(ext_type_event_history:ext_event_history(), element(), orset_base()) -> orset_base().

-callback read(ext_type_cover:ext_subset_in_cover(), orset_base()) ->
    {ext_type_cover:ext_subset_in_cover(), sets:set()}.

-callback join(ext_node_type(), orset_base(), orset_base()) -> orset_base().

-callback is_inflation(orset_base(), orset_base()) -> boolean().

-callback map(
    {ext_node_id(), ext_replica_id()}, function(), ext_type_path:ext_path_info(), orset_base()) ->
    orset_base().

-callback filter(
    {ext_node_id(), ext_replica_id()}, function(), ext_type_path:ext_path_info(), orset_base()) ->
    orset_base().

-callback product(
    {ext_node_id(), ext_replica_id()}, ext_type_path:ext_path_info(), orset_base(), orset_base()) ->
    orset_base().

-callback consistent_read(
    ext_type_path:ext_path_info_list(),
    ext_type_cover:ext_subset_in_cover(),
    ext_type_provenance:ext_dot_set(),
    orset_base()) ->
    {ext_type_cover:ext_subset_in_cover(), ext_type_provenance:ext_dot_set(), sets:set()}.

-callback set_count(
    {ext_node_id(), ext_replica_id()},
    ext_type_path:ext_path_info_list(),
    ext_type_path:ext_path_info(),
    orset_base()) -> orset_base().

-callback group_by_sum(
    {ext_node_id(), ext_replica_id()},
    function(),
    ext_type_path:ext_path_info_list(),
    ext_type_path:ext_path_info(),
    orset_base()) -> orset_base().

-callback order_by(
    {ext_node_id(), ext_replica_id()},
    function(),
    ext_type_path:ext_path_info_list(),
    ext_type_path:ext_path_info(),
    orset_base()) -> orset_base().

-callback next_event_history(
    ext_type_event_history:ext_event_history_type(),
    ext_node_id(),
    ext_replica_id(),
    orset_base()) -> ext_type_event_history:ext_event_history().

-spec is_strict_inflation(orset_base(), orset_base()) -> boolean().
is_strict_inflation(ORSetL, ORSetR) ->
    ORSetBase = lasp_config:get(ext_type_version, ext_type_orset_base_v1),
    (not ORSetBase:equal(ORSetL, ORSetR)) andalso
        ORSetBase:is_inflation(ORSetL, ORSetR).

%%case Id of
%%    ?GROUP_X_USER_F_G_O__X__GROUP_C_X_DIVIDER_M__M ->
%%        lager:info("Value0"),
%%        _Ignore0 = print_value(Value0),
%%        lager:info("Value"),
%%        _Ignore1 = print_value(Value),
%%        lager:info("Merged"),
%%        _Ignore2 = print_value(Merged);
%%    _ ->
%%        lager:info("Id: ~p", [Id])
%%end,
%%case Id of
%%    ?GROUP_X_USER_F_G_O__X__GROUP_C_X_DIVIDER_M__M ->
%%        lager:info("InflationT");
%%    _ ->
%%        lager:info("Id: ~p", [Id])
%%end,
-spec print_value(ext_crdt()) -> non_neg_integer().
print_value({_Type, {_NodeType, _AllPathInfoList, {EventHistoryAll, EventHistorySurvived, Cover, DataStore}}}=_Value) ->
    print_value_internal(EventHistoryAll, EventHistorySurvived, Cover, DataStore);
print_value({_Type, {_NodeType, _AllPathInfoList, {EventHistoryAll, EventRemoved, DataStore}}}=_Value) ->
    print_value_internal(EventHistoryAll, EventRemoved, ordsets:new(), DataStore).

%% @private
print_value_internal(EventHistoryAll, EventHistorySurvived, Cover, DataStore) ->
    lager:info("EventHistoryAll"),
    _ =
        ordsets:fold(
            fun(EventHistory, Index0) ->
                lager:info("EventHistory ~p: ~p", [Index0, EventHistory]),
                Index0 + 1
            end,
            0,
            EventHistoryAll),
    lager:info("EventHistorySurvived"),
    _ =
        ordsets:fold(
            fun(EventHistory, Index0) ->
                lager:info("EventHistory ~p: ~p", [Index0, EventHistory]),
                Index0 + 1
            end,
            0,
            EventHistorySurvived),
    lager:info("Cover"),
    _ =
        ordsets:fold(
            fun(Subset, Index0) ->
                lager:info("Subset ~p:", [Index0]),
                _ =
                    ordsets:fold(
                        fun(EventHistory, Index1) ->
                            lager:info("EventHistory ~p: ~p", [Index1, EventHistory]),
                            Index1 + 1
                        end,
                        0,
                        Subset),
                Index0 + 1
            end,
            0,
            Cover),
    lager:info("DataStore"),
    orddict:fold(
        fun(_Subset, ProvenanceStore, Index0) ->
            lager:info("ProvenanceStore ~p:", [Index0]),
            _Ignore0 =
                orddict:fold(
                    fun(Elem, Provenance, Index1) ->
                        lager:info("Elem ~p: ~p", [Index1, Elem]),
                        lager:info("Provenance ~p:", [Index1]),
                        _Ignore1 =
                            ordsets:fold(
                                fun(Dot, Index2) ->
                                    lager:info("Dot ~p:", [Index2]),
                                    _Ignore2 =
                                        ordsets:fold(
                                            fun(EventHistory, Index3) ->
                                                lager:info("EventHistory ~p: ~p", [Index3, EventHistory]),
                                                Index3 + 1
                                            end,
                                            0,
                                            Dot),
                                    Index2 + 1
                                end,
                                0,
                                Provenance),
                        Index1 + 1
                    end,
                    0,
                    ProvenanceStore),
            Index0 + 1
        end,
        0,
        DataStore).
