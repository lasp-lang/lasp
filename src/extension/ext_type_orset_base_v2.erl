-module(ext_type_orset_base_v2).

-author("Junghun Yoo <junghun.yoo@cs.ox.ac.uk>").

-behaviour(ext_type_orset_base).

%% API
-export([
    new/1,
    equal/2,
    insert/3,
    read/2,
    join/3,
    is_inflation/2,
    is_strict_inflation/2,
    map/5,
    filter/5,
    product/5,
    consistent_read/4,
    set_count/4,
    group_by_sum/5,
    order_by/5,
    next_event_history/4]).

-include("lasp_ext.hrl").

-export_type([
    ext_type_orset_base_v2/0]).

-type element() :: term().
-type ext_node_id() :: term().
-type ext_replica_id() :: term().
-type ext_event_history_all() :: ext_type_event_history_set:ext_event_history_set().
-type ext_event_history_survived() :: ext_type_event_history_set:ext_event_history_set().
-type ext_provenance_store() :: orddict:orddict(element(), ext_type_provenance:ext_provenance()).
-type ext_data_store() :: orddict:orddict(ext_type_cover:ext_subset_in_cover(), ext_provenance_store()).
-opaque ext_type_orset_base_v2() ::
    {
        ext_event_history_all(),
        ext_event_history_survived(),
        ext_type_cover:ext_cover(),
        ext_data_store()}.

-spec new(ext_type_path:ext_path_info_list()) -> ext_type_orset_base_v2().
new(_AllPathInfoList) ->
    {
        ext_type_event_history_set:new_event_history_set(),
        ext_type_event_history_set:new_event_history_set(),
        ext_type_cover:new_cover(),
        orddict:new()}.

-spec equal(ext_type_orset_base_v2(), ext_type_orset_base_v2()) -> boolean().
equal(
    {EventHistoryAllL, EventHistorySurvivedL, CoverL, DataStoreL}=_ORSetBaseV2L,
    {EventHistoryAllR, EventHistorySurvivedR, CoverR, DataStoreR}=_ORSetBaseV2R) ->
    EventHistoryAllL == EventHistoryAllR andalso
        EventHistorySurvivedL == EventHistorySurvivedR andalso
        CoverL == CoverR andalso
        DataStoreL == DataStoreR.

-spec insert(ext_type_event_history:ext_event_history(), element(), ext_type_orset_base_v2()) ->
    ext_type_orset_base_v2().
insert(EventHistory, Elem, {[], [], [], []}=_ORSetBaseV2) ->
    AtomEventHistory = ext_type_event_history:event_history_to_atom(EventHistory),
    NewProvenance =
        ext_type_provenance:new_provenance(ext_type_provenance:new_dot(AtomEventHistory)),
    NewSubsetInCover = ext_type_cover:new_subset_in_cover(AtomEventHistory),
    {
        ext_type_event_history_set:add_event_history(
            AtomEventHistory, ext_type_event_history_set:new_event_history_set()),
        ext_type_event_history_set:add_event_history(
            AtomEventHistory, ext_type_event_history_set:new_event_history_set()),
        ext_type_cover:new_cover(NewSubsetInCover),
        orddict:store(
            NewSubsetInCover,
            orddict:store(Elem, NewProvenance, orddict:new()),
            orddict:new())};
insert(
    EventHistory,
    Elem,
    {
        EventHistoryAll,
        EventHistorySurvived,
        [SubsetInCover]=_Cover,
        DataStore}=ORSetBaseV2) ->
    AtomEventHistory = ext_type_event_history:event_history_to_atom(EventHistory),
    CanBeSkipped =
        ext_type_event_history_set:is_found_dominant_event_history(AtomEventHistory, EventHistoryAll),
    case CanBeSkipped of
        true ->
            ORSetBaseV2;
        false ->
            NewDot = ext_type_provenance:new_dot(AtomEventHistory),
            NewProvenance = ext_type_provenance:new_provenance(NewDot),
            NewEventHistoryAll =
                ext_type_event_history_set:union_event_history_set(NewDot, EventHistoryAll),
            NewEventHistorySurvived =
                ext_type_event_history_set:union_event_history_set(NewDot, EventHistorySurvived),
            NewSubsetInCover =
                ext_type_event_history_set:union_event_history_set(NewDot, SubsetInCover),
            OldProvenanceStore = orddict:fetch(SubsetInCover, DataStore),
            NewProvenanceStore =
                orddict:update(
                    Elem,
                    fun(OldProvenance) ->
                        OldProvenancePruned =
                            ext_type_provenance:prune_provenance(
                                OldProvenance, NewEventHistorySurvived),
                        ext_type_provenance:plus_provenance(OldProvenancePruned, NewProvenance)
                    end,
                    NewProvenance,
                    OldProvenanceStore),
            {
                NewEventHistoryAll,
                NewEventHistorySurvived,
                ext_type_cover:new_cover(NewSubsetInCover),
                orddict:store(
                    NewSubsetInCover, NewProvenanceStore, orddict:new())}
    end.

-spec read(ext_type_cover:ext_subset_in_cover(), ext_type_orset_base_v2()) ->
    {ext_type_cover:ext_subset_in_cover(), sets:set()}.
read(
    PrevSubset,
    {_EventHistoryAll, _EventHistorySurvived, Cover, DataStore}=_ORSetBaseV2) ->
    case DataStore of
        [] ->
            {ext_type_cover:new_subset_in_cover(), sets:new()};
        _ ->
            SuperSubsets = ext_type_cover:find_all_super_subsets(PrevSubset, Cover),
            SubsetInCover = ext_type_cover:select_subset(SuperSubsets),
            {
                SubsetInCover,
                sets:from_list(orddict:fetch_keys(orddict:fetch(SubsetInCover, DataStore)))}
    end.

-spec join(ext_type_orset_base:ext_node_type(), ext_type_orset_base_v2(), ext_type_orset_base_v2()) ->
    ext_type_orset_base_v2().
join(_, {[], [], [], []}, ORSetBaseV2) ->
    ORSetBaseV2;
join(_, ORSetBaseV2, {[], [], [], []}) ->
    ORSetBaseV2;
join(
    _NodeType, ORSetBaseV2, ORSetBaseV2) ->
    ORSetBaseV2;
join(
    input,
    {
        EventHistoryAllL,
        EventHistorySurvivedL,
        [SingleSubsetL]=_CoverL,
        DataStoreL}=_ORSetBaseV2L,
    {
        EventHistoryAllR,
        EventHistorySurvivedR,
        [SingleSubsetR]=_CoverR,
        DataStoreR}=_ORSetBaseV2R) ->
    JoinedEventHistoryAll =
        ext_type_event_history_set:union_event_history_set(EventHistoryAllL, EventHistoryAllR),

    JoinedEventHistorySurvived =
        ext_type_event_history_set:union_event_history_set(
            ext_type_event_history_set:intersection_event_history_set(
                EventHistorySurvivedL, EventHistorySurvivedR),
            ext_type_event_history_set:union_event_history_set(
                ext_type_event_history_set:minus_event_history_set(
                    EventHistorySurvivedL, EventHistoryAllR),
                ext_type_event_history_set:minus_event_history_set(
                    EventHistorySurvivedR, EventHistoryAllL))),

    JoinedSingleSubset0 =
        ext_type_event_history_set:union_event_history_set(SingleSubsetL, SingleSubsetR),
    JoinedSingleSubset = ext_type_cover:prune_subset(JoinedSingleSubset0, JoinedEventHistoryAll),
    JoinedProvenanceStore =
        join_provenance_store(
            orddict:fetch(SingleSubsetL, DataStoreL),
            orddict:fetch(SingleSubsetR, DataStoreR),
            JoinedEventHistorySurvived),
    {JoinedCover, JoinedDataStore} =
        add_subset_provenance_store(
            JoinedSingleSubset, JoinedProvenanceStore, ext_type_cover:new_cover(), orddict:new()),

    {
        JoinedEventHistoryAll,
        JoinedEventHistorySurvived,
        JoinedCover,
        JoinedDataStore};
join(
    intermediate,
    {EventHistoryAllL, EventHistorySurvivedL, CoverL, DataStoreL}=_ORSetBaseV2L,
    {EventHistoryAllR, EventHistorySurvivedR, CoverR, DataStoreR}=_ORSetBaseV2R) ->
    JoinedEventHistoryAll =
        ext_type_event_history_set:union_event_history_set(EventHistoryAllL, EventHistoryAllR),

    JoinedEventHistorySurvived =
        ext_type_event_history_set:union_event_history_set(
            ext_type_event_history_set:intersection_event_history_set(
                EventHistorySurvivedL, EventHistorySurvivedR),
            ext_type_event_history_set:union_event_history_set(
                ext_type_event_history_set:minus_event_history_set(
                    EventHistorySurvivedL, EventHistoryAllR),
                ext_type_event_history_set:minus_event_history_set(
                    EventHistorySurvivedR, EventHistoryAllL))),

    {JoinedCover0, JoinedDataStore0} =
        ordsets:fold(
            fun(SubsetL, {AccInJoinedCover0, AccInJoinedDataStore0}) ->
                PrunedSubsetL = ext_type_cover:prune_subset(SubsetL, JoinedEventHistoryAll),
                PrunedProvenanceStoreL =
                    orddict:fold(
                        fun(Elem, Provenance, AccInPrunedProvenanceStoreL) ->
                            NewProvenance =
                                ext_type_provenance:prune_provenance(
                                    Provenance, JoinedEventHistorySurvived),
                            case NewProvenance of
                                [] ->
                                    AccInPrunedProvenanceStoreL;
                                _ ->
                                    orddict:store(
                                        Elem,
                                        NewProvenance,
                                        AccInPrunedProvenanceStoreL)
                            end
                        end,
                        orddict:new(),
                        orddict:fetch(SubsetL, DataStoreL)),
                add_subset_provenance_store(
                    PrunedSubsetL, PrunedProvenanceStoreL, AccInJoinedCover0, AccInJoinedDataStore0)
            end,
            {ext_type_cover:new_cover(), orddict:new()},
            CoverL),
    {JoinedCover, JoinedDataStore} =
        ordsets:fold(
            fun(SubsetR, {AccInJoinedCover, AccInJoinedDataStore}) ->
                PrunedSubsetR = ext_type_cover:prune_subset(SubsetR, JoinedEventHistoryAll),
                PrunedProvenanceStoreR =
                    orddict:fold(
                        fun(Elem, Provenance, AccInPrunedProvenanceStoreR) ->
                            NewProvenance =
                                ext_type_provenance:prune_provenance(
                                    Provenance, JoinedEventHistorySurvived),
                            case NewProvenance of
                                [] ->
                                    AccInPrunedProvenanceStoreR;
                                _ ->
                                    orddict:store(
                                        Elem,
                                        NewProvenance,
                                        AccInPrunedProvenanceStoreR)
                            end
                        end,
                        orddict:new(),
                        orddict:fetch(SubsetR, DataStoreR)),
                add_subset_provenance_store(
                    PrunedSubsetR, PrunedProvenanceStoreR, AccInJoinedCover, AccInJoinedDataStore)
            end,
            {JoinedCover0, JoinedDataStore0},
            CoverR),

    {
        JoinedEventHistoryAll,
        JoinedEventHistorySurvived,
        JoinedCover,
        JoinedDataStore}.

-spec is_inflation(ext_type_orset_base_v2(), ext_type_orset_base_v2()) -> boolean().
is_inflation(
    {EventHistoryAllL, EventHistorySurvivedL, CoverL, DataStoreL}=_ORSetBaseV2L,
    {EventHistoryAllR, EventHistorySurvivedR, CoverR, DataStoreR}=_ORSetBaseV2R) ->
% (EventHistoryAllL \subseteq EventHistoryAllR)
% \land
% (EventHistorySurvivedR \cap EventHistoryAllL \subseteq EventHistorySurvivedL)
% \land
% ((CoverL, DataStoreL) \sqsubseteq (CoverR, DataStoreR))
    AllEventStatesOrder =
        ext_type_event_history_set:is_orderly_subset(EventHistoryAllL, EventHistoryAllR),
    SurvivedEventStatesOrder =
        ext_type_event_history_set:is_orderly_subset(
            ext_type_event_history_set:intersection_event_history_set(
                EventHistorySurvivedR, EventHistoryAllL),
            EventHistorySurvivedL),
    CoverDataStoreOrder =
        is_inflation_cover_data_store(
            CoverL, DataStoreL, CoverR, DataStoreR, EventHistorySurvivedR),
    AllEventStatesOrder andalso
        SurvivedEventStatesOrder andalso
        CoverDataStoreOrder.

-spec is_strict_inflation(ext_type_orset_base_v2(), ext_type_orset_base_v2()) -> boolean().
is_strict_inflation(ORSetBaseL, ORSetBaseR) ->
    ext_type_orset_base:is_strict_inflation(ORSetBaseL, ORSetBaseR).

-spec map(
    {ext_node_id(), ext_replica_id()},
    function(),
    ext_type_path:ext_path_info_list(),
    ext_type_path:ext_path_info(),
    ext_type_orset_base_v2()) -> ext_type_orset_base_v2().
map({NodeId, _ReplicaId}=_Actor,
    Function,
    _AllPathInfoList,
    PathInfo,
    {EventHistoryAll, EventHistorySurvived, Cover, DataStore}=_ORSetBaseV2) ->
    {PathDict, NewEventHistoryAll} =
        ext_type_event_history_set:build_path_append_dict_and_update(
            NodeId, PathInfo, EventHistoryAll, orddict:new()),

    NewEventHistorySurvived =
        ext_type_event_history_set:append_cur_node(EventHistorySurvived, PathDict),

    {NewCover, NewDataStore} =
        ordsets:fold(
            fun(SubsetInCover, {AccInNewCover, AccInNewDataStore}) ->
                NewProvenanceStore =
                    orddict:fold(
                        fun(Elem, Provenance, AccInNewProvenanceStore) ->
                            NewProvenance =
                                ext_type_provenance:append_cur_node(Provenance, PathDict),
                            orddict:update(
                                Function(Elem),
                                fun(OldProvenance) ->
                                    ext_type_provenance:plus_provenance(
                                        OldProvenance, NewProvenance)
                                end,
                                NewProvenance,
                                AccInNewProvenanceStore)
                        end,
                        orddict:new(),
                        orddict:fetch(SubsetInCover, DataStore)),
                NewSubsetInCover = ext_type_cover:append_cur_node(SubsetInCover, PathDict),
                {
                    ordsets:add_element(NewSubsetInCover, AccInNewCover),
                    orddict:store(
                        NewSubsetInCover,
                        NewProvenanceStore,
                        AccInNewDataStore)}
            end,
            {orddict:new(), orddict:new()},
            Cover),

    {NewEventHistoryAll, NewEventHistorySurvived, NewCover, NewDataStore}.

-spec filter(
    {ext_node_id(), ext_replica_id()},
    function(),
    ext_type_path:ext_path_info_list(),
    ext_type_path:ext_path_info(),
    ext_type_orset_base_v2()) -> ext_type_orset_base_v2().
filter(
    {NodeId, _ReplicaId}=_Actor,
    Function,
    _AllPathInfoList,
    PathInfo,
    {EventHistoryAll, EventHistorySurvived, Cover, DataStore}=_ORSetBaseV2) ->
    {PathDict, NewEventHistoryAll} =
        ext_type_event_history_set:build_path_append_dict_and_update(
            NodeId, PathInfo, EventHistoryAll, orddict:new()),

    NewEventHistorySurvived =
        ext_type_event_history_set:append_cur_node(EventHistorySurvived, PathDict),

    {NewCover, NewDataStore} =
        ordsets:fold(
            fun(SubsetInCover, {AccInNewCover, AccInNewDataStore}) ->
                NewProvenanceStore =
                    orddict:fold(
                        fun(Elem, Provenance, AccInNewProvenanceStore) ->
                            case Function(Elem) of
                                false ->
                                    AccInNewProvenanceStore;
                                true ->
                                    NewProvenance =
                                        ext_type_provenance:append_cur_node(Provenance, PathDict),
                                    orddict:store(
                                        Elem,
                                        NewProvenance,
                                        AccInNewProvenanceStore)
                            end
                        end,
                        orddict:new(),
                        orddict:fetch(SubsetInCover, DataStore)),
                NewSubsetInCover = ext_type_cover:append_cur_node(SubsetInCover, PathDict),
                {
                    ordsets:add_element(NewSubsetInCover, AccInNewCover),
                    orddict:store(
                        NewSubsetInCover,
                        NewProvenanceStore,
                        AccInNewDataStore)}
            end,
            {orddict:new(), orddict:new()},
            Cover),

    {NewEventHistoryAll, NewEventHistorySurvived, NewCover, NewDataStore}.

-spec product(
    {ext_node_id(), ext_replica_id()},
    ext_type_path:ext_path_info_list(),
    ext_type_path:ext_path_info(),
    ext_type_orset_base_v2(),
    ext_type_orset_base_v2()) -> ext_type_orset_base_v2().
product(
    {NodeId, _ReplicaId}=_Actor,
    _AllPathInfoList,
    PathInfo,
    {EventHistoryAllL, EventHistorySurvivedL, CoverL, DataStoreL}=_ORSetBaseV2L,
    {EventHistoryAllR, EventHistorySurvivedR, CoverR, DataStoreR}=_ORSetBaseV2R) ->
    {PathDict, ProductEventHistoryAll} =
        ext_type_event_history_set:build_path_append_dict_and_update(
            NodeId,
            PathInfo,
            ext_type_event_history_set:union_event_history_set(EventHistoryAllL, EventHistoryAllR),
            orddict:new()),

    ProductEventHistorySurvived0 =
        ext_type_event_history_set:union_event_history_set(
            ext_type_event_history_set:intersection_event_history_set(
                EventHistorySurvivedL, EventHistorySurvivedR),
            ext_type_event_history_set:union_event_history_set(
                ext_type_event_history_set:minus_event_history_set(
                    EventHistorySurvivedL, EventHistoryAllR),
                ext_type_event_history_set:minus_event_history_set(
                    EventHistorySurvivedR, EventHistoryAllL))),
    ProductEventHistorySurvived =
        ext_type_event_history_set:append_cur_node(ProductEventHistorySurvived0, PathDict),

    {ProductCover, ProductDataStore} =
        ordsets:fold(
            fun(SubsetInCoverL, {AccInProductCoverL, AccInProductDataStoreL}) ->
                ordsets:fold(
                    fun(SubsetInCoverR, {AccInProductCoverR, AccInProductDataStoreR}) ->
                        ProductSubset0 =
                            ext_type_event_history_set:union_event_history_set(
                                SubsetInCoverL, SubsetInCoverR),
                        ProductSubset1 =
                            ext_type_cover:append_cur_node(ProductSubset0, PathDict),
                        ProductSubset =
                            ext_type_cover:prune_subset(ProductSubset1, ProductEventHistoryAll),

                        ProductProvenanceStore =
                            orddict:fold(
                                fun(ElemL,
                                    ProvenanceL,
                                    AccInProductProvenanceStoreL) ->
                                    orddict:fold(
                                        fun(ElemR,
                                            ProvenanceR,
                                            AccInProductProvenanceStoreR) ->
                                            NewProvenance0 =
                                                ext_type_provenance:cross_provenance(
                                                    ProvenanceL, ProvenanceR),
                                            NewProvenance1 =
                                                ext_type_provenance:append_cur_node(
                                                    NewProvenance0, PathDict),
                                            NewProvenance =
                                                ext_type_provenance:prune_provenance(
                                                    NewProvenance1, ProductEventHistorySurvived),
                                            case NewProvenance of
                                                [] ->
                                                    AccInProductProvenanceStoreR;
                                                _ ->
                                                    orddict:store(
                                                        {ElemL, ElemR},
                                                        NewProvenance,
                                                        AccInProductProvenanceStoreR)
                                            end
                                        end,
                                        AccInProductProvenanceStoreL,
                                        orddict:fetch(
                                            SubsetInCoverR, DataStoreR))
                                end,
                                orddict:new(),
                                orddict:fetch(SubsetInCoverL, DataStoreL)),

                        add_subset_provenance_store(
                            ProductSubset, ProductProvenanceStore,
                            AccInProductCoverR, AccInProductDataStoreR)
                    end,
                    {AccInProductCoverL, AccInProductDataStoreL},
                    CoverR)
            end,
            {ordsets:new(), orddict:new()},
            CoverL),

    {ProductEventHistoryAll, ProductEventHistorySurvived, ProductCover, ProductDataStore}.

-spec consistent_read(
    ext_type_path:ext_path_info_list(),
    ext_type_cover:ext_subset_in_cover(),
    ext_type_provenance:ext_dot_set(),
    ext_type_orset_base_v2()) ->
    {ext_type_cover:ext_subset_in_cover(), ext_type_provenance:ext_dot_set(), sets:set()}.
consistent_read(
    AllPathInfoList,
    PrevSubset,
    PrevCDS,
    {_EventHistoryAll, _EventHistorySurvived, Cover, DataStore}=_ORSetBaseV2) ->
    case DataStore of
        [] ->
            {ordsets:new(), ordsets:new(), sets:new()};
        _ ->
            SuperSubsets = ext_type_cover:find_all_super_subsets(PrevSubset, Cover),
            NewSubset = ext_type_cover:select_subset(SuperSubsets),
            NewCDSs = ext_type_provenance:get_CDSs(AllPathInfoList, NewSubset),
            SuperCDSs = ext_type_provenance:find_all_super_CDSs(PrevCDS, NewCDSs),
            NewCDS = ext_type_provenance:select_CDS(SuperCDSs),
            {
                NewSubset,
                NewCDS,
                orddict:fold(
                    fun(Elem, Provenance, AccInResultSet) ->
                        case ext_type_provenance:provenance_in_CDS(Provenance, NewCDS) of
                            [] ->
                                AccInResultSet;
                            _ ->
                                sets:add_element(Elem, AccInResultSet)
                        end
                    end,
                    sets:new(),
                    orddict:fetch(NewSubset, DataStore))}
    end.

-spec set_count(
    {ext_node_id(), ext_replica_id()},
    ext_type_path:ext_path_info_list(),
    ext_type_path:ext_path_info(),
    ext_type_orset_base_v2()) -> ext_type_orset_base_v2().
set_count(
    {NodeId, _ReplicaId}=_Actor,
    AllPathInfoList,
    PathInfo,
    {EventHistoryAll, EventHistorySurvived, Cover, DataStore}=_ORSetBaseV2) ->
    {PathDict, NewEventHistoryAll} =
        ext_type_event_history_set:build_path_append_dict_and_update(
            NodeId, PathInfo, EventHistoryAll, orddict:new()),

    NewEventHistorySurvived =
        ext_type_event_history_set:append_cur_node(EventHistorySurvived, PathDict),

    {NewCover, NewDataStore, NewGroupEventHistorySet} =
        ordsets:fold(
            fun(Subset,
                {AccInNewCover, AccInNewDataStore, AccInNewGroupEventHistorySet}) ->
                NewSubset = ext_type_event_history_set:append_cur_node(Subset, PathDict),
                CDS =
                    ext_type_provenance:select_CDS(
                        ext_type_provenance:get_CDSs(AllPathInfoList, NewSubset)),
                {NewElem, NewProvenance} =
                    orddict:fold(
                        fun(_Elem,
                            Provenance,
                            {AccInNewElem, AccInNewProvenance}) ->
                            ProvenanceInCDS =
                                ext_type_provenance:provenance_in_CDS(
                                    ext_type_provenance:append_cur_node(Provenance, PathDict),
                                    CDS),
                            case ProvenanceInCDS of
                                [] ->
                                    {AccInNewElem, AccInNewProvenance};
                                _ ->
                                    {
                                        AccInNewElem + 1,
                                        ext_type_provenance:cross_provenance(
                                            AccInNewProvenance,
                                            ProvenanceInCDS)}
                            end
                        end,
                        {0, ?PROVENANCE_ONE},
                        orddict:fetch(Subset, DataStore)),
                {NewProvenanceStore, GroupEventHistorySet} =
                    case NewElem of
                        0 ->
                            {orddict:new(), orddict:new()};
                        _ ->
                            {NewProvenanceWithGroup, GroupEvents0} =
                                ext_type_provenance:generate_group_event_history_for_provenance(
                                    NewProvenance, NodeId),
                            {
                                orddict:store(NewElem, NewProvenanceWithGroup, orddict:new()),
                                GroupEvents0}
                    end,
                NewSubsetWithGroup =
                    ext_type_event_history_set:union_event_history_set(NewSubset, GroupEventHistorySet),
                {
                    ext_type_cover:add_subset(NewSubsetWithGroup, AccInNewCover),
                    orddict:store(
                        NewSubsetWithGroup,
                        NewProvenanceStore,
                        AccInNewDataStore),
                    ext_type_event_history_set:union_event_history_set(
                        AccInNewGroupEventHistorySet, GroupEventHistorySet)}
            end,
            {ordsets:new(), orddict:new(), ordsets:new()},
            Cover),

    {
        ext_type_event_history_set:union_event_history_set(
            NewEventHistoryAll, NewGroupEventHistorySet),
        ext_type_event_history_set:union_event_history_set(
            NewEventHistorySurvived, NewGroupEventHistorySet),
        NewCover,
        NewDataStore}.

-spec group_by_sum(
    {ext_node_id(), ext_replica_id()},
    function(),
    ext_type_path:ext_path_info_list(),
    ext_type_path:ext_path_info(),
    ext_type_orset_base_v2()) -> ext_type_orset_base_v2().
group_by_sum(
    {NodeId, _ReplicaId}=_Actor,
    SumFunction,
    AllPathInfoList,
    PathInfo,
    {EventHistoryAll, EventHistorySurvived, Cover, DataStore}=_ORSetBaseV2) ->
    {PathDict, NewEventHistoryAll} =
        ext_type_event_history_set:build_path_append_dict_and_update(
            NodeId, PathInfo, EventHistoryAll, orddict:new()),

    NewEventHistorySurvived =
        ext_type_event_history_set:append_cur_node(EventHistorySurvived, PathDict),

    {NewCover, NewDataStore, NewGroupEvents} =
        ordsets:fold(
            fun(Subset,
                {AccInNewCover, AccInNewDataStore, AccInNewGroupEvents}) ->
                NewSubset = ext_type_event_history_set:append_cur_node(Subset, PathDict),
                CDS =
                    ext_type_provenance:select_CDS(
                        ext_type_provenance:get_CDSs(AllPathInfoList, NewSubset)),
                NewProvenanceStore0 =
                    orddict:fold(
                        fun({Fst, Snd}=_Elem,
                            Provenance,
                            AccInNewProvenanceStore0) ->
                            ProvenanceInCDS =
                                ext_type_provenance:provenance_in_CDS(
                                    ext_type_provenance:append_cur_node(Provenance, PathDict),
                                    CDS),
                            case ProvenanceInCDS of
                                [] ->
                                    AccInNewProvenanceStore0;
                                _ ->
                                    orddict:update(
                                        Fst,
                                        fun({OldSum, OldProvenance}) ->
                                            {
                                                SumFunction(OldSum, Snd),
                                                ext_type_provenance:cross_provenance(
                                                    OldProvenance,
                                                    ProvenanceInCDS)}
                                        end,
                                        {SumFunction(undefined, Snd), ProvenanceInCDS},
                                        AccInNewProvenanceStore0)
                            end
                        end,
                        orddict:new(),
                        orddict:fetch(Subset, DataStore)),
                {NewProvenanceStore, GroupEvents} =
                    orddict:fold(
                        fun(Fst,
                            {Snd, ProvenanceInPS0},
                            {AccInNewProvenanceStore, AccInGroupEvents}) ->
                            {NewProvenanceWithGroup, GroupEvents0} =
                                ext_type_provenance:generate_group_event_history_for_provenance(
                                    ProvenanceInPS0, NodeId),
                            {
                                orddict:store(
                                    {Fst, Snd},
                                    NewProvenanceWithGroup,
                                    AccInNewProvenanceStore),
                                ext_type_event_history_set:union_event_history_set(
                                    GroupEvents0, AccInGroupEvents)}
                        end,
                        {orddict:new(), ordsets:new()},
                        NewProvenanceStore0),
                NewSubsetWithGroup =
                    ext_type_event_history_set:union_event_history_set(NewSubset, GroupEvents),
                {
                    ext_type_cover:add_subset(NewSubsetWithGroup, AccInNewCover),
                    orddict:store(
                        NewSubsetWithGroup,
                        NewProvenanceStore,
                        AccInNewDataStore),
                    ext_type_event_history_set:union_event_history_set(
                        AccInNewGroupEvents, GroupEvents)}
            end,
            {ordsets:new(), orddict:new(), ordsets:new()},
            Cover),

    {
        ext_type_event_history_set:union_event_history_set(
            NewEventHistoryAll, NewGroupEvents),
        ext_type_event_history_set:union_event_history_set(
            NewEventHistorySurvived, NewGroupEvents),
        NewCover,
        NewDataStore}.

-spec order_by(
    {ext_node_id(), ext_replica_id()},
    function(),
    ext_type_path:ext_path_info_list(),
    ext_type_path:ext_path_info(),
    ext_type_orset_base_v2()) -> ext_type_orset_base_v2().
order_by(
    {NodeId, _ReplicaId}=_Actor,
    CompareFunction,
    AllPathInfoList,
    PathInfo,
    {EventHistoryAll, EventHistorySurvived, Cover, DataStore}=_ORSetBaseV2) ->
    {PathDict, NewEventHistoryAll} =
        ext_type_event_history_set:build_path_append_dict_and_update(
            NodeId, PathInfo, EventHistoryAll, orddict:new()),

    NewEventHistorySurvived =
        ext_type_event_history_set:append_cur_node(EventHistorySurvived, PathDict),

    {NewCover, NewDataStore, NewGroupEvents} =
        ordsets:fold(
            fun(Subset,
                {AccInNewCover, AccInNewDataStore, AccInNewGroupEvents}) ->
                NewSubset = ext_type_event_history_set:append_cur_node(Subset, PathDict),
                CDS =
                    ext_type_provenance:select_CDS(
                        ext_type_provenance:get_CDSs(AllPathInfoList, NewSubset)),
                {NewElem, NewProvenance} =
                    orddict:fold(
                        fun(Elem,
                            Provenance,
                            {AccInNewElem, AccInNewProvenance}) ->
                            ProvenanceInCDS =
                                ext_type_provenance:provenance_in_CDS(
                                    ext_type_provenance:append_cur_node(Provenance, PathDict),
                                    CDS),
                            case ProvenanceInCDS of
                                [] ->
                                    {AccInNewElem, AccInNewProvenance};
                                _ ->
                                    {
                                        AccInNewElem ++ [Elem],
                                        ext_type_provenance:cross_provenance(
                                            AccInNewProvenance,
                                            ProvenanceInCDS)}
                            end
                        end,
                        {[], ?PROVENANCE_ONE},
                        orddict:fetch(Subset, DataStore)),
                {NewProvenanceStore, GroupEvents} =
                    case NewElem of
                        [] ->
                            {orddict:new(), orddict:new()};
                        _ ->
                            {NewProvenanceWithGroup, GroupEvents0} =
                                ext_type_provenance:generate_group_event_history_for_provenance(
                                    NewProvenance, NodeId),
                            {
                                orddict:store(
                                    lists:sort(CompareFunction, NewElem),
                                    NewProvenanceWithGroup,
                                    orddict:new()),
                                GroupEvents0}
                    end,
                NewSubsetWithGroup =
                    ext_type_event_history_set:union_event_history_set(NewSubset, GroupEvents),
                {
                    ext_type_cover:add_subset(NewSubsetWithGroup, AccInNewCover),
                    orddict:store(
                        NewSubsetWithGroup,
                        NewProvenanceStore,
                        AccInNewDataStore),
                    ext_type_event_history_set:union_event_history_set(
                        AccInNewGroupEvents, GroupEvents)}
            end,
            {ordsets:new(), orddict:new(), ordsets:new()},
            Cover),

    {
        ext_type_event_history_set:union_event_history_set(
            NewEventHistoryAll, NewGroupEvents),
        ext_type_event_history_set:union_event_history_set(
            NewEventHistorySurvived, NewGroupEvents),
        NewCover,
        NewDataStore}.

-spec next_event_history(
    ext_type_event_history:ext_event_history_type(),
    ext_node_id(),
    ext_replica_id(),
    ext_type_orset_base_v2()) -> ext_type_event_history:ext_event_history().
next_event_history(
    EventType,
    NodeId,
    ReplicaId,
    {EventHistoryAll, _EventHistorySurvived, _Cover, _DataStore}=_ORSetBaseV2L) ->
    ext_type_event_history:next_event_history(EventType, NodeId, ReplicaId, EventHistoryAll).

%% @private
is_inflation_provenance_store(
    [], _ProvenanceStoreR, _EventHistorySurvivedR, _SubsetInCover) ->
    true;
is_inflation_provenance_store(
    [{Elem, ProvenanceL} | T]=_ProvenanceStoreL,
    ProvenanceStoreR,
    EventHistorySurvivedR,
    SubsetInCover) ->
    DotSetL = ext_type_provenance:prune_provenance(ProvenanceL, EventHistorySurvivedR),
    ProvenanceR =
        case orddict:find(Elem, ProvenanceStoreR) of
            {ok, Provenance} ->
                Provenance;
            error ->
                ordsets:new()
        end,
    DotSetR = ext_type_provenance:prune_provenance(ProvenanceR, EventHistorySurvivedR),
    case ordsets:is_subset(DotSetL, DotSetR) of
        false ->
            false;
        true ->
            is_inflation_provenance_store(
                T, ProvenanceStoreR, EventHistorySurvivedR, SubsetInCover)
    end.

%% @private
is_inflation_data_store(
    [], _DataStoreR, _EventHistorySurvivedR, _ProvenanceStoreL) ->
    true;
is_inflation_data_store(
    [H | T]=_RelatedSubsets, DataStoreR, EventHistorySurvivedR, ProvenanceStoreL) ->
    case is_inflation_provenance_store(
        ProvenanceStoreL,
        orddict:fetch(H, DataStoreR),
        EventHistorySurvivedR,
        H) of
        false ->
            false;
        true ->
            is_inflation_data_store(
                T, DataStoreR, EventHistorySurvivedR, ProvenanceStoreL)
    end.

%% @private
is_inflation_cover_data_store(
    [], _DataStoreL, _CoverR, _DataStoreR, _EventHistorySurvivedR) ->
    true;
is_inflation_cover_data_store(
    [H | T]=_CoverL, DataStoreL, CoverR, DataStoreR, EventHistorySurvivedR) ->
    RelatedSubsets =
        ordsets:fold(
            fun(SubsetInCoverR, AccInRelatedSubsets) ->
                case ext_type_event_history_set:is_orderly_subset(H, SubsetInCoverR) of
                    false ->
                        AccInRelatedSubsets;
                    true ->
                        ordsets:add_element(SubsetInCoverR, AccInRelatedSubsets)
                end
            end,
            ordsets:new(),
            CoverR),
    case RelatedSubsets of
        [] ->
            false;
        _ ->
            case is_inflation_data_store(
                RelatedSubsets,
                DataStoreR,
                EventHistorySurvivedR,
                orddict:fetch(H, DataStoreL)) of
                false ->
                    false;
                true ->
                    is_inflation_cover_data_store(
                        T, DataStoreL, CoverR, DataStoreR, EventHistorySurvivedR)
            end
    end.

%% @private
join_provenance_store(ProvenanceStoreL, ProvenanceStoreR, PruneEventHistorySet) ->
    Result0 =
        orddict:merge(
            fun(_Elem, ProvenanceL, ProvenanceR) ->
                ext_type_provenance:plus_provenance(ProvenanceL, ProvenanceR)
            end,
            ProvenanceStoreL,
            ProvenanceStoreR),
    orddict:fold(
        fun(Elem, Provenance, AccInProvenanceStore) ->
            NewProvenance =
                ext_type_provenance:prune_provenance(Provenance, PruneEventHistorySet),
            case NewProvenance of
                [] ->
                    AccInProvenanceStore;
                _ ->
                    orddict:store(Elem, NewProvenance, AccInProvenanceStore)
            end
        end,
        orddict:new(),
        Result0).

%% @private
combine_provenance_store(ProvenanceStoreL, ProvenanceStoreR) ->
    orddict:merge(
        fun(_Elem, ProvenanceL, ProvenanceR) ->
            ext_type_provenance:plus_provenance(ProvenanceL, ProvenanceR)
        end,
        ProvenanceStoreL,
        ProvenanceStoreR).

%% @private
add_subset_provenance_store(Subset, ProvenanceStore, [], []) ->
    {ext_type_cover:new_cover(Subset), orddict:store(Subset, ProvenanceStore, orddict:new())};
add_subset_provenance_store(Subset, ProvenanceStore, Cover, DataStore) ->
    {SuperSubsets, NewDataStore} =
        ordsets:fold(
            fun(SubsetInCover, {AccInSuperSubsets, AccInNewDataStore}) ->
                case ext_type_event_history_set:is_orderly_subset(Subset, SubsetInCover) of
                    false ->
                        {AccInSuperSubsets, AccInNewDataStore};
                    true ->
                        {
                            ordsets:add_element(SubsetInCover, AccInSuperSubsets),
                            orddict:update(
                                SubsetInCover,
                                fun(OldProvenanceStore) ->
                                    combine_provenance_store(ProvenanceStore, OldProvenanceStore)
                                end,
                                AccInNewDataStore)}
                end
            end,
            {ordsets:new(), DataStore},
            Cover),
    case SuperSubsets of
        [] ->
            {SubSubsets, NewNewDataStore} =
                ordsets:fold(
                    fun(SubsetInCover, {AccInSubSubsets, AccInNewNewDataStore}) ->
                        case ext_type_event_history_set:is_orderly_subset(SubsetInCover, Subset) of
                            false ->
                                {
                                    AccInSubSubsets,
                                    orddict:store(
                                        SubsetInCover,
                                        orddict:fetch(SubsetInCover, DataStore),
                                        AccInNewNewDataStore)};
                            true ->
                                NewProvenanceStore =
                                    combine_provenance_store(
                                        ProvenanceStore, orddict:fetch(SubsetInCover, DataStore)),
                                {
                                    ordsets:add_element(SubsetInCover, AccInSubSubsets),
                                    orddict:update(
                                        Subset,
                                        fun(OldProvenanceStore) ->
                                            combine_provenance_store(
                                                OldProvenanceStore, NewProvenanceStore)
                                        end,
                                        NewProvenanceStore,
                                        AccInNewNewDataStore)}
                        end
                    end,
                    {ordsets:new(), orddict:new()},
                    Cover),
            case SubSubsets of
                [] ->
                    {
                        ext_type_cover:add_subset(Subset, Cover),
                        orddict:store(Subset, ProvenanceStore, DataStore)};
                _ ->
                    {
                        ext_type_cover:add_subset(Subset, ordsets:subtract(Cover, SubSubsets)),
                        NewNewDataStore}
            end;
        _ ->
            {Cover, NewDataStore}
    end.
