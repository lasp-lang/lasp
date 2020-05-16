-module(ext_type_orset_base_v4).

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
    map/4,
    filter/4,
    product/4,
    consistent_read/4,
    set_count/4,
    group_by_sum/5,
    order_by/5,
    next_event_history/4]).

-include("lasp_ext.hrl").

-export_type([
    ext_type_orset_base_v4/0]).

-type element() :: term().
-type ext_node_id() :: term().
-type ext_replica_id() :: term().
-type ext_event_history_all() :: ext_type_event_history_set:ext_event_history_set().
-type ext_event_removed() :: ordsets:ordset(ext_type_event:ext_event()).
-type ext_provenance_store() :: orddict:orddict(element(), ext_type_provenance:ext_provenance()).
-type ext_data_store() ::
    orddict:orddict(ext_type_cover:ext_subset_in_cover(), ext_provenance_store()).
-opaque ext_type_orset_base_v4() ::
    {ext_event_history_all(), ext_event_removed(), ext_data_store()}.

-spec new(ext_type_path:ext_path_info_list()) -> ext_type_orset_base_v4().
new(_AllPathInfoList) ->
    {ext_type_event_history_set:new_event_history_set(), ordsets:new(), orddict:new()}.

-spec equal(ext_type_orset_base_v4(), ext_type_orset_base_v4()) -> boolean().
equal(
    {EventHistoryAllL, EventRemovedL, DataStoreL}=_ORSetBaseV4L,
    {EventHistoryAllR, EventRemovedR, DataStoreR}=_ORSetBaseV4R) ->
    EventHistoryAllL == EventHistoryAllR andalso
        EventRemovedL == EventRemovedR andalso
        DataStoreL == DataStoreR.

-spec insert(ext_type_event_history:ext_event_history(), element(), ext_type_orset_base_v4()) ->
    ext_type_orset_base_v4().
insert(EventHistory, Elem, {[], [], []}=_ORSetBaseV4) ->
    NewProvenance =
        ext_type_provenance:new_provenance(ext_type_provenance:new_dot(EventHistory)),
    NewSubsetUnknownInCover = ext_type_cover:new_subset_in_cover(),
    {
        ext_type_event_history_set:add_event_history(
            EventHistory, ext_type_event_history_set:new_event_history_set()),
        [],
        orddict:store(
            NewSubsetUnknownInCover,
            orddict:store(Elem, NewProvenance, orddict:new()),
            orddict:new())};
insert(EventHistory, Elem, {EventHistoryAll, EventRemoved, DataStore}=ORSetBaseV4) ->
    CanBeSkipped =
        ext_type_event_history_set:is_found_dominant_event_history(EventHistory, EventHistoryAll),
    case CanBeSkipped of
        true ->
            ORSetBaseV4;
        false ->
            NewDot = ext_type_provenance:new_dot(EventHistory),
            NewProvenance = ext_type_provenance:new_provenance(NewDot),
            NewEventHistoryAll =
                ext_type_event_history_set:union_event_history_set(NewDot, EventHistoryAll),
            NewEventHistorySurvived =
                ext_type_event_history_set:find_survived(EventHistoryAll, EventRemoved),
            [SingleSubsetUnknown] = orddict:fetch_keys(DataStore),
            SingleSubset = ext_type_event_history_set:subtract(EventHistoryAll, SingleSubsetUnknown),
            NewSingleSubset = ext_type_event_history_set:union_event_history_set(NewDot, SingleSubset),
            NewSingleSubsetUnknown =
                ext_type_event_history_set:subtract(NewEventHistoryAll, NewSingleSubset),
            OldProvenanceStore = orddict:fetch(SingleSubsetUnknown, DataStore),
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
                EventRemoved,
                orddict:store(NewSingleSubsetUnknown, NewProvenanceStore, orddict:new())}
    end.

-spec read(ext_type_cover:ext_subset_in_cover(), ext_type_orset_base_v4()) ->
    {ext_type_cover:ext_subset_in_cover(), sets:set()}.
read(
    PrevSubset,
    {EventHistoryAll, _EventRemoved, DataStore}=_ORSetBaseV4) ->
    case DataStore of
        [] ->
            {ext_type_cover:new_subset_in_cover(), sets:new()};
        _ ->
            Cover = ext_type_cover:generate_cover(orddict:fetch_keys(DataStore), EventHistoryAll),
            SuperSubsets = ext_type_cover:find_all_super_subsets(PrevSubset, Cover),
            SuperSubset = ext_type_cover:select_subset(SuperSubsets),
            SuperSubsetUnknown = ext_type_event_history_set:subtract(EventHistoryAll, SuperSubset),
            {
                SuperSubset,
                sets:from_list(orddict:fetch_keys(orddict:fetch(SuperSubsetUnknown, DataStore)))}
    end.

-spec join(
    ext_type_orset_base:ext_node_type(), ext_type_orset_base_v4(), ext_type_orset_base_v4()) ->
    ext_type_orset_base_v4().
join(_, {[], [], []}, ORSetBaseV4) ->
    ORSetBaseV4;
join(_, ORSetBaseV4, {[], [], []}) ->
    ORSetBaseV4;
join(
    _NodeType, ORSetBaseV4, ORSetBaseV4) ->
    ORSetBaseV4;
join(
    input,
    {
        EventHistoryAllL,
        EventRemovedL,
        [{SingleSubsetUnknownL, ProvenanceStoreL}]=_DataStoreL}=_ORSetBaseV4L,
    {
        EventHistoryAllR,
        EventRemovedR,
        [{SingleSubsetUnknownR, ProvenanceStoreR}]=_DataStoreR}=_ORSetBaseV4R) ->
    JoinedEventHistoryAll =
        ext_type_event_history_set:union_event_history_set(EventHistoryAllL, EventHistoryAllR),

    JoinedEventRemoved = ordsets:union(EventRemovedL, EventRemovedR),

    SingleSubsetL = ext_type_event_history_set:subtract(EventHistoryAllL, SingleSubsetUnknownL),
    SingleSubsetR = ext_type_event_history_set:subtract(EventHistoryAllR, SingleSubsetUnknownR),
    JoinedSingleSubset0 =
        ext_type_event_history_set:union_event_history_set(SingleSubsetL, SingleSubsetR),
    JoinedSingleSubset =
        ext_type_cover:prune_subset(JoinedSingleSubset0, JoinedEventHistoryAll),
    JoinedSingleSubsetUnknown =
        ext_type_event_history_set:subtract(JoinedEventHistoryAll, JoinedSingleSubset),
    JoinedEventHistorySurvived =
        ext_type_event_history_set:find_survived(JoinedEventHistoryAll, JoinedEventRemoved),
    JoinedProvenanceStore =
        join_provenance_store(ProvenanceStoreL, ProvenanceStoreR, JoinedEventHistorySurvived),
    JoinedDataStore =
        add_subset_provenance_store(
            JoinedSingleSubsetUnknown, JoinedProvenanceStore, orddict:new(), JoinedEventHistoryAll),

    {JoinedEventHistoryAll, JoinedEventRemoved, JoinedDataStore};
join(
    intermediate,
    {EventHistoryAllL, EventRemovedL, DataStoreL}=_ORSetBaseV4L,
    {EventHistoryAllR, EventRemovedR, DataStoreR}=_ORSetBaseV4R) ->
    JoinedEventHistoryAll =
        ext_type_event_history_set:union_event_history_set(EventHistoryAllL, EventHistoryAllR),

    JoinedEventRemoved = ordsets:union(EventRemovedL, EventRemovedR),

    JoinedEventHistorySurvived =
        ext_type_event_history_set:find_survived(JoinedEventHistoryAll, JoinedEventRemoved),

    JoinedDataStore0 =
        orddict:fold(
            fun(SubsetUnknownL, ProvenanceStoreL, AccInJoinedDataStore0) ->
                SubsetL = ext_type_event_history_set:subtract(EventHistoryAllL, SubsetUnknownL),
                PrunedSubsetL = ext_type_cover:prune_subset(SubsetL, JoinedEventHistoryAll),
                PrunedSubsetUnknownL =
                    ext_type_event_history_set:subtract(JoinedEventHistoryAll, PrunedSubsetL),
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
                        ProvenanceStoreL),
                add_subset_provenance_store(
                    PrunedSubsetUnknownL, PrunedProvenanceStoreL,
                    AccInJoinedDataStore0, JoinedEventHistoryAll)
            end,
            orddict:new(),
            DataStoreL),
    JoinedDataStore =
        orddict:fold(
            fun(SubsetUnknownR, ProvenanceStoreR, AccInJoinedDataStore) ->
                SubsetR = ext_type_event_history_set:subtract(EventHistoryAllR, SubsetUnknownR),
                PrunedSubsetR = ext_type_cover:prune_subset(SubsetR, JoinedEventHistoryAll),
                PrunedSubsetUnknownR =
                    ext_type_event_history_set:subtract(JoinedEventHistoryAll, PrunedSubsetR),
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
                        ProvenanceStoreR),
                add_subset_provenance_store(
                    PrunedSubsetUnknownR, PrunedProvenanceStoreR,
                    AccInJoinedDataStore, JoinedEventHistoryAll)
            end,
            JoinedDataStore0,
            DataStoreR),

    {JoinedEventHistoryAll, JoinedEventRemoved, JoinedDataStore}.

-spec is_inflation(ext_type_orset_base_v4(), ext_type_orset_base_v4()) -> boolean().
is_inflation(
    {EventHistoryAllL, EventRemovedL, DataStoreL}=_ORSetBaseV4L,
    {EventHistoryAllR, EventRemovedR, DataStoreR}=_ORSetBaseV4R) ->
% (EventHistoryAllL \subseteq EventHistoryAllR)
% \land
% (EventHistorySurvivedR \cap EventHistoryAllL \subseteq EventHistorySurvivedL)
% \land
% ((CoverL, DataStoreL) \sqsubseteq (CoverR, DataStoreR))
    ext_type_event_history_set:is_orderly_subset(EventHistoryAllL, EventHistoryAllR) andalso
        ordsets:is_subset(EventRemovedL, EventRemovedR) andalso
        is_inflation_cover_data_store(
            DataStoreL, EventHistoryAllL,
            DataStoreR, EventHistoryAllR,
            ext_type_event_history_set:find_survived(EventHistoryAllR, EventRemovedR)).

-spec is_strict_inflation(ext_type_orset_base_v4(), ext_type_orset_base_v4()) -> boolean().
is_strict_inflation(ORSetBaseL, ORSetBaseR) ->
    ext_type_orset_base:is_strict_inflation(ORSetBaseL, ORSetBaseR).

-spec map(
    {ext_node_id(), ext_replica_id()},
    function(),
    ext_type_path:ext_path_info(),
    ext_type_orset_base_v4()) -> ext_type_orset_base_v4().
map({NodeId, _ReplicaId}=_Actor,
    Function,
    PathInfo,
    {EventHistoryAll, EventRemoved, DataStore}=_ORSetBaseV4) ->
    NewEventHistoryAll =
        ext_type_event_history_set:append_cur_node(NodeId, PathInfo, EventHistoryAll),

    NewDataStore =
        orddict:fold(
            fun(SubsetUnknown, ProvenanceStore, AccInNewDataStore) ->
                NewProvenanceStore =
                    orddict:fold(
                        fun(Elem, Provenance, AccInNewProvenanceStore) ->
                            NewProvenance =
                                ext_type_provenance:append_cur_node(NodeId, PathInfo, Provenance),
                            orddict:update(
                                Function(Elem),
                                fun(OldProvenance) ->
                                    ext_type_provenance:plus_provenance(OldProvenance, NewProvenance)
                                end,
                                NewProvenance,
                                AccInNewProvenanceStore)
                        end,
                        orddict:new(),
                        ProvenanceStore),
                NewSubsetUnknown = ext_type_cover:append_cur_node(NodeId, PathInfo, SubsetUnknown),
                orddict:store(NewSubsetUnknown, NewProvenanceStore, AccInNewDataStore)
            end,
            orddict:new(),
            DataStore),

    {NewEventHistoryAll, EventRemoved, NewDataStore}.

-spec filter(
    {ext_node_id(), ext_replica_id()},
    function(),
    ext_type_path:ext_path_info(),
    ext_type_orset_base_v4()) -> ext_type_orset_base_v4().
filter(
    {NodeId, _ReplicaId}=_Actor,
    Function,
    PathInfo,
    {EventHistoryAll, EventRemoved, DataStore}=_ORSetBaseV4) ->
    NewEventHistoryAll =
        ext_type_event_history_set:append_cur_node(NodeId, PathInfo, EventHistoryAll),

    NewDataStore =
        orddict:fold(
            fun(SubsetUnknown, ProvenanceStore, AccInNewDataStore) ->
                NewProvenanceStore =
                    orddict:fold(
                        fun(Elem, Provenance, AccInNewProvenanceStore) ->
                            case Function(Elem) of
                                false ->
                                    AccInNewProvenanceStore;
                                true ->
                                    NewProvenance =
                                        ext_type_provenance:append_cur_node(
                                            NodeId, PathInfo, Provenance),
                                    orddict:store(Elem, NewProvenance, AccInNewProvenanceStore)
                            end
                        end,
                        orddict:new(),
                        ProvenanceStore),
                NewSubsetUnknown = ext_type_cover:append_cur_node(NodeId, PathInfo, SubsetUnknown),
                orddict:store(NewSubsetUnknown, NewProvenanceStore, AccInNewDataStore)
            end,
            orddict:new(),
            DataStore),

    {NewEventHistoryAll, EventRemoved, NewDataStore}.

-spec product(
    {ext_node_id(), ext_replica_id()},
    ext_type_path:ext_path_info(),
    ext_type_orset_base_v4(),
    ext_type_orset_base_v4()) -> ext_type_orset_base_v4().
product(
    {NodeId, _ReplicaId}=_Actor,
    PathInfo,
    {EventHistoryAllL, EventRemovedL, DataStoreL}=_ORSetBaseV4L,
    {EventHistoryAllR, EventRemovedR, DataStoreR}=_ORSetBaseV4R) ->
    ProductEventHistoryAll =
        ext_type_event_history_set:append_cur_node(
            NodeId,
            PathInfo,
            ext_type_event_history_set:union_event_history_set(EventHistoryAllL, EventHistoryAllR)),

    ProductEventRemoved = ordsets:union(EventRemovedL, EventRemovedR),

    ProductEventHistorySurvived =
        ext_type_event_history_set:find_survived(ProductEventHistoryAll, ProductEventRemoved),
    ProductDataStore =
        orddict:fold(
            fun(SubsetUnknownL, ProvenanceStoreL, AccInProductDataStoreL) ->
                orddict:fold(
                    fun(SubsetUnknownR, ProvenanceStoreR, AccInProductDataStoreR) ->
                        SubsetL = ext_type_event_history_set:subtract(EventHistoryAllL, SubsetUnknownL),
                        SubsetR = ext_type_event_history_set:subtract(EventHistoryAllR, SubsetUnknownR),
                        ProductSubset0 =
                            ext_type_event_history_set:union_event_history_set(SubsetL, SubsetR),
                        ProductSubset1 =
                            ext_type_cover:append_cur_node(NodeId, PathInfo, ProductSubset0),
                        ProductSubset =
                            ext_type_cover:prune_subset(ProductSubset1, ProductEventHistoryAll),
                        ProductSubsetUnknown =
                            ext_type_event_history_set:subtract(ProductEventHistoryAll, ProductSubset),

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
                                                    NodeId, PathInfo, NewProvenance0),
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
                                        ProvenanceStoreR)
                                end,
                                orddict:new(),
                                ProvenanceStoreL),

                        add_subset_provenance_store(
                            ProductSubsetUnknown, ProductProvenanceStore,
                            AccInProductDataStoreR, ProductEventHistoryAll)
                    end,
                    AccInProductDataStoreL,
                    DataStoreR)
            end,
            orddict:new(),
            DataStoreL),

    {ProductEventHistoryAll, ProductEventRemoved, ProductDataStore}.

-spec consistent_read(
    ext_type_path:ext_path_info_list(),
    ext_type_cover:ext_subset_in_cover(),
    ext_type_provenance:ext_dot_set(),
    ext_type_orset_base_v4()) ->
    {ext_type_cover:ext_subset_in_cover(), ext_type_provenance:ext_dot_set(), sets:set()}.
consistent_read(
    AllPathInfoList,
    PrevSubset,
    PrevCDS,
    {EventHistoryAll, _EventRemoved, DataStore}=_ORSetBaseV4) ->
    case DataStore of
        [] ->
            {ordsets:new(), ordsets:new(), sets:new()};
        _ ->
            Cover = ext_type_cover:generate_cover(orddict:fetch_keys(DataStore), EventHistoryAll),
            SuperSubsets = ext_type_cover:find_all_super_subsets(PrevSubset, Cover),
            SuperSubset = ext_type_cover:select_subset(SuperSubsets),
            NewSubsetUnknown = ext_type_event_history_set:subtract(EventHistoryAll, SuperSubset),
            NewCDSs = ext_type_provenance:get_CDSs(AllPathInfoList, SuperSubset),
            SuperCDSs = ext_type_provenance:find_all_super_CDSs(PrevCDS, NewCDSs),
            NewCDS = ext_type_provenance:select_CDS(SuperCDSs),
            {
                SuperSubset,
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
                    orddict:fetch(NewSubsetUnknown, DataStore))}
    end.

-spec set_count(
    {ext_node_id(), ext_replica_id()},
    ext_type_path:ext_path_info_list(),
    ext_type_path:ext_path_info(),
    ext_type_orset_base_v4()) -> ext_type_orset_base_v4().
set_count(
    {NodeId, _ReplicaId}=_Actor,
    AllPathInfoList,
    PathInfo,
    {EventHistoryAll, EventRemoved, DataStore}=_ORSetBaseV4) ->
    NewEventHistoryAll =
        ext_type_event_history_set:append_cur_node(NodeId, PathInfo, EventHistoryAll),

    {NewDataStore, NewGroupEventHistorySet} =
        orddict:fold(
            fun(SubsetUnknown, ProvenanceStore, {AccInNewDataStore, AccInNewGroupEventHistorySet}) ->
                NewSubsetUnknown =
                    ext_type_event_history_set:append_cur_node(NodeId, PathInfo, SubsetUnknown),
                CDS =
                    ext_type_provenance:select_CDS(
                        ext_type_provenance:get_CDSs(
                            AllPathInfoList,
                            ext_type_event_history_set:subtract(NewEventHistoryAll, NewSubsetUnknown))),
                {NewElem, NewProvenance} =
                    orddict:fold(
                        fun(_Elem, Provenance, {AccInNewElem, AccInNewProvenance}) ->
                            ProvenanceInCDS =
                                ext_type_provenance:provenance_in_CDS(
                                    ext_type_provenance:append_cur_node(NodeId, PathInfo, Provenance),
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
                        ProvenanceStore),
                {NewProvenanceStore, GroupEventHistorySet} =
                    case NewElem of
                        0 ->
                            {orddict:new(), orddict:new()};
                        _ ->
                            {NewProvenanceWithGroup, GroupEventHistorySet0} =
                                ext_type_provenance:generate_group_event_history_for_provenance_v2(
                                    NewProvenance, NodeId),
                            {
                                orddict:store(NewElem, NewProvenanceWithGroup, orddict:new()),
                                GroupEventHistorySet0}
                    end,
                {
                    orddict:store(NewSubsetUnknown, NewProvenanceStore, AccInNewDataStore),
                    ext_type_event_history_set:union_event_history_set(
                        AccInNewGroupEventHistorySet, GroupEventHistorySet)}
            end,
            {orddict:new(), ordsets:new()},
            DataStore),

    {
        ext_type_event_history_set:union_event_history_set(
            NewEventHistoryAll, NewGroupEventHistorySet),
        EventRemoved,
        NewDataStore}.

-spec group_by_sum(
    {ext_node_id(), ext_replica_id()},
    function(),
    ext_type_path:ext_path_info_list(),
    ext_type_path:ext_path_info(),
    ext_type_orset_base_v4()) -> ext_type_orset_base_v4().
group_by_sum(
    {NodeId, _ReplicaId}=_Actor,
    SumFunction,
    AllPathInfoList,
    PathInfo,
    {EventHistoryAll, EventRemoved, DataStore}=_ORSetBaseV4) ->
    NewEventHistoryAll =
        ext_type_event_history_set:append_cur_node(NodeId, PathInfo, EventHistoryAll),

    {NewDataStore, NewGroupEventHistorySet} =
        orddict:fold(
            fun(SubsetUnknown, ProvenanceStore, {AccInNewDataStore, AccInNewGroupEvents}) ->
                NewSubsetUnknown =
                    ext_type_event_history_set:append_cur_node(NodeId, PathInfo, SubsetUnknown),
                CDS =
                    ext_type_provenance:select_CDS(
                        ext_type_provenance:get_CDSs(
                            AllPathInfoList,
                            ext_type_event_history_set:subtract(NewEventHistoryAll, NewSubsetUnknown))),
                NewProvenanceStore0 =
                    orddict:fold(
                        fun({Fst, Snd}=_Elem, Provenance, AccInNewProvenanceStore0) ->
                            ProvenanceInCDS =
                                ext_type_provenance:provenance_in_CDS(
                                    ext_type_provenance:append_cur_node(NodeId, PathInfo, Provenance),
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
                        ProvenanceStore),
                {NewProvenanceStore, GroupEventHistorySet} =
                    orddict:fold(
                        fun(Fst,
                            {Snd, ProvenanceInPS0},
                            {AccInNewProvenanceStore, AccInGroupEvents}) ->
                            {NewProvenanceWithGroup, GroupEventHistorySet0} =
                                ext_type_provenance:generate_group_event_history_for_provenance_v2(
                                    ProvenanceInPS0, NodeId),
                            {
                                orddict:store(
                                    {Fst, Snd},
                                    NewProvenanceWithGroup,
                                    AccInNewProvenanceStore),
                                ext_type_event_history_set:union_event_history_set(
                                    GroupEventHistorySet0, AccInGroupEvents)}
                        end,
                        {orddict:new(), ordsets:new()},
                        NewProvenanceStore0),
                {
                    orddict:store(NewSubsetUnknown, NewProvenanceStore, AccInNewDataStore),
                    ext_type_event_history_set:union_event_history_set(
                        AccInNewGroupEvents, GroupEventHistorySet)}
            end,
            {orddict:new(), ordsets:new()},
            DataStore),

    {
        ext_type_event_history_set:union_event_history_set(
            NewEventHistoryAll, NewGroupEventHistorySet),
        EventRemoved,
        NewDataStore}.

-spec order_by(
    {ext_node_id(), ext_replica_id()},
    function(),
    ext_type_path:ext_path_info_list(),
    ext_type_path:ext_path_info(),
    ext_type_orset_base_v4()) -> ext_type_orset_base_v4().
order_by(
    {NodeId, _ReplicaId}=_Actor,
    CompareFunction,
    AllPathInfoList,
    PathInfo,
    {EventHistoryAll, EventRemoved, DataStore}=_ORSetBaseV4) ->
    NewEventHistoryAll =
        ext_type_event_history_set:append_cur_node(NodeId, PathInfo, EventHistoryAll),

    {NewDataStore, NewGroupEventHistorySet} =
        orddict:fold(
            fun(SubsetUnknown, ProvenanceStore, {AccInNewDataStore, AccInNewGroupEvents}) ->
                NewSubsetUnknown =
                    ext_type_event_history_set:append_cur_node(NodeId, PathInfo, SubsetUnknown),
                CDS =
                    ext_type_provenance:select_CDS(
                        ext_type_provenance:get_CDSs(
                            AllPathInfoList,
                            ext_type_event_history_set:subtract(NewEventHistoryAll, NewSubsetUnknown))),
                {NewElem, NewProvenance} =
                    orddict:fold(
                        fun(Elem, Provenance, {AccInNewElem, AccInNewProvenance}) ->
                            ProvenanceInCDS =
                                ext_type_provenance:provenance_in_CDS(
                                    ext_type_provenance:append_cur_node(
                                        NodeId, PathInfo, Provenance),
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
                        ProvenanceStore),
                {NewProvenanceStore, GroupEventHistorySet} =
                    case NewElem of
                        [] ->
                            {orddict:new(), orddict:new()};
                        _ ->
                            {NewProvenanceWithGroup, GroupEventHistorySet0} =
                                ext_type_provenance:generate_group_event_history_for_provenance_v2(
                                    NewProvenance, NodeId),
                            {
                                orddict:store(
                                    lists:sort(CompareFunction, NewElem),
                                    NewProvenanceWithGroup,
                                    orddict:new()),
                                GroupEventHistorySet0}
                    end,
                {
                    orddict:store(NewSubsetUnknown, NewProvenanceStore, AccInNewDataStore),
                    ext_type_event_history_set:union_event_history_set(
                        AccInNewGroupEvents, GroupEventHistorySet)}
            end,
            {orddict:new(), ordsets:new()},
            DataStore),

    {
        ext_type_event_history_set:union_event_history_set(
            NewEventHistoryAll, NewGroupEventHistorySet),
        EventRemoved,
        NewDataStore}.

-spec next_event_history(
    ext_type_event_history:ext_event_history_type(),
    ext_node_id(),
    ext_replica_id(),
    ext_type_orset_base_v4()) -> ext_type_event_history:ext_event_history().
next_event_history(
    EventType,
    NodeId,
    ReplicaId,
    {EventHistoryAll, _EventRemoved, _DataStore}=_ORSetBaseV4L) ->
    ext_type_event_history:next_event_history(EventType, NodeId, ReplicaId, EventHistoryAll).

%% @private
is_inflation_provenance_store(
    [], _ProvenanceStoreR, _EventHistorySurvivedR) ->
    true;
is_inflation_provenance_store(
    [{Elem, ProvenanceL} | T]=_ProvenanceStoreL, ProvenanceStoreR, EventHistorySurvivedR) ->
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
            is_inflation_provenance_store(T, ProvenanceStoreR, EventHistorySurvivedR)
    end.

%% @private
is_inflation_data_store(
    [], _DataStoreR, _EventHistorySurvivedR, _ProvenanceStoreL) ->
    true;
is_inflation_data_store(
    [H | T]=_RelatedSubsetUnknownsR, DataStoreR, EventHistorySurvivedR, ProvenanceStoreL) ->
    case is_inflation_provenance_store(
        ProvenanceStoreL, orddict:fetch(H, DataStoreR), EventHistorySurvivedR) of
        false ->
            false;
        true ->
            is_inflation_data_store(
                T, DataStoreR, EventHistorySurvivedR, ProvenanceStoreL)
    end.

%% @private
is_inflation_cover_data_store(
    [], _EventHistoryAllL, _DataStoreR, _EventHistoryAllR, _EventHistorySurvivedR) ->
    true;
is_inflation_cover_data_store(
    [{SubsetUnknownL, ProvenanceStoreL} | T]=_DataStoreL, EventHistoryAllL,
    DataStoreR, EventHistoryAllR, EventHistorySurvivedR) ->
    SubsetL = ext_type_event_history_set:subtract(EventHistoryAllL, SubsetUnknownL),
    RelatedSubsetUnknownsR =
        lists:foldl(
            fun(SubsetUnknownR, AccInRelatedSubsets) ->
                SubsetR = ext_type_event_history_set:subtract(EventHistoryAllR, SubsetUnknownR),
                case ext_type_event_history_set:is_orderly_subset(SubsetL, SubsetR) of
                    false ->
                        AccInRelatedSubsets;
                    true ->
                        ordsets:add_element(SubsetUnknownR, AccInRelatedSubsets)
                end
            end,
            ordsets:new(),
            orddict:fetch_keys(DataStoreR)),
    case RelatedSubsetUnknownsR of
        [] ->
            false;
        _ ->
            case is_inflation_data_store(
                RelatedSubsetUnknownsR, DataStoreR, EventHistorySurvivedR, ProvenanceStoreL) of
                false ->
                    false;
                true ->
                    is_inflation_cover_data_store(
                        T, EventHistoryAllL, DataStoreR, EventHistoryAllR, EventHistorySurvivedR)
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
add_subset_provenance_store(SubsetUnknown, ProvenanceStore, [], _EventHistoryAll) ->
    orddict:store(SubsetUnknown, ProvenanceStore, orddict:new());
add_subset_provenance_store(SubsetUnknown, ProvenanceStore, DataStore, EventHistoryAll) ->
    Subset = ext_type_event_history_set:subtract(EventHistoryAll, SubsetUnknown),
    {SuperSubsets, NewDataStore} =
        orddict:fold(
            fun(SubsetUnknownInCover, PSInDataStore, {AccInSuperSubsets, AccInNewDataStore}) ->
                SubsetInCover =
                    ext_type_event_history_set:subtract(EventHistoryAll, SubsetUnknownInCover),
                case ext_type_event_history_set:is_orderly_subset(Subset, SubsetInCover) of
                    false ->
                        {AccInSuperSubsets, AccInNewDataStore};
                    true ->
                        {
                            ordsets:add_element(SubsetUnknownInCover, AccInSuperSubsets),
                            orddict:store(
                                SubsetUnknownInCover,
                                combine_provenance_store(ProvenanceStore, PSInDataStore),
                                AccInNewDataStore)}
                end
            end,
            {ordsets:new(), DataStore},
            DataStore),
    case SuperSubsets of
        [] ->
            {SubSubsets, NewNewDataStore} =
                orddict:fold(
                    fun(SubsetUnknownInCover, PSInDataStore, {AccInSubSubsets, AccInNewNewDataStore}) ->
                        SubsetInCover =
                            ext_type_event_history_set:subtract(EventHistoryAll, SubsetUnknownInCover),
                        case ext_type_event_history_set:is_orderly_subset(SubsetInCover, Subset) of
                            false ->
                                {
                                    AccInSubSubsets,
                                    orddict:store(
                                        SubsetUnknownInCover, PSInDataStore, AccInNewNewDataStore)};
                            true ->
                                NewProvenanceStore =
                                    combine_provenance_store(ProvenanceStore, PSInDataStore),
                                {
                                    ordsets:add_element(SubsetUnknownInCover, AccInSubSubsets),
                                    orddict:update(
                                        SubsetUnknown,
                                        fun(OldProvenanceStore) ->
                                            combine_provenance_store(
                                                OldProvenanceStore, NewProvenanceStore)
                                        end,
                                        NewProvenanceStore,
                                        AccInNewNewDataStore)}
                        end
                    end,
                    {ordsets:new(), orddict:new()},
                    DataStore),
            case SubSubsets of
                [] ->
                    orddict:store(SubsetUnknown, ProvenanceStore, DataStore);
                _ ->
                    NewNewDataStore
            end;
        _ ->
            NewDataStore
    end.
