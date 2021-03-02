-module(ext_type_orset_base_v7).

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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([
    ext_type_orset_base_v7/0]).

-type ext_node_id() :: term().
-type ext_replica_id() :: term().
-type element() :: term().
-type ext_removed_event_tree_leaf_set() ::
    ordsets:ordset(ext_type_event_tree:ext_event_tree_leaf()).
-type ext_filtered_event_tree_set() :: ordsets:ordset(ext_type_event_tree:ext_event_tree()).
-type ext_provenance_store() :: orddict:orddict(element(), ext_type_provenance:ext_provenance()).
-type ext_data_store() :: {ext_filtered_event_tree_set(), ext_provenance_store()}.
-opaque ext_type_orset_base_v7() ::
    {ext_removed_event_tree_leaf_set(), ordsets:ordset(ext_data_store())}.

-spec new(ext_type_path:ext_path_info_list()) -> ext_type_orset_base_v7().
new(_AllPathInfoList) ->
    {ordsets:new(), ordsets:new()}.

-spec equal(ext_type_orset_base_v7(), ext_type_orset_base_v7()) -> boolean().
equal(
    {RemovedEventTreeLeafSetL, DataStoreSetL}=_ORSetBaseV7L,
    {RemovedEventTreeLeafSetR, DataStoreSetR}=_ORSetBaseV7R) ->
    RemovedEventTreeLeafSetL == RemovedEventTreeLeafSetR andalso DataStoreSetL == DataStoreSetR.

-spec insert(ext_type_event_history:ext_event_history(), element(), ext_type_orset_base_v7()) ->
    ext_type_orset_base_v7().
insert(EventTree, Elem, {[], []}=_ORSetBaseV7) ->
    NewProvenance =
        ext_type_provenance:new_provenance(ext_type_provenance:new_dot(EventTree)),
    ProvenanceStore = orddict:store(Elem, NewProvenance, orddict:new()),
    {[], [{[], ProvenanceStore}]};
insert(EventTree, Elem, {RemovedEventTreeLeafSet, [{[], ProvenanceStore}]}=_ORSetBaseV7) ->
    {NewProvenanceStore, IsPrunedPS} =
        orddict:fold(
            fun(ElemInPS, OldProvenance, {AccInProvenanceStore, AccInIsPrunedPS}) ->
                {NewProvenance, IsPrunedProvenance} =
                    ordsets:fold(
                        fun(OldDot, {AccInProvenance, AccInIsPrunedProvenance})->
                            {NewDot, IsPrunedDot} =
                                ordsets:fold(
                                    fun(OldEventTree, {AccInDot, AccInIsPrunedDot}) ->
                                        NewAccInDot =
                                            case ext_type_event_tree:is_related_to_tree_leaf(
                                                OldEventTree, EventTree) of
                                                true ->
                                                    AccInDot;
                                                false ->
                                                    ordsets:add_element(OldEventTree, AccInDot)
                                            end,
                                        NewAccInIsPrunedDot =
                                            ext_type_event_tree:is_related_to_tree_leaf(
                                                EventTree, OldEventTree),
                                        {NewAccInDot, AccInIsPrunedDot orelse NewAccInIsPrunedDot}
                                    end,
                                    {ordsets:new(), false},
                                    OldDot),
                            case NewDot of
                                [] ->
                                    {AccInProvenance, AccInIsPrunedProvenance orelse IsPrunedDot};
                                _ ->
                                    {
                                        ordsets:add_element(NewDot, AccInProvenance),
                                        AccInIsPrunedProvenance orelse IsPrunedDot}
                            end
                        end,
                        {ordsets:new(), false},
                        OldProvenance),
                case NewProvenance of
                    [] ->
                        {AccInProvenanceStore, AccInIsPrunedPS orelse IsPrunedProvenance};
                    _ ->
                        {
                            orddict:store(ElemInPS, NewProvenance, AccInProvenanceStore),
                            AccInIsPrunedPS orelse IsPrunedProvenance}
                end
            end,
            {orddict:new(), false},
            ProvenanceStore),
    case IsPrunedPS of
        true ->
            {RemovedEventTreeLeafSet, [{[], NewProvenanceStore}]};
        false ->
            NewProvenance =
                ext_type_provenance:new_provenance(ext_type_provenance:new_dot(EventTree)),
            FinalProvenanceStore =
                orddict:update(
                    Elem,
                    fun(OldProvenance) ->
                        ext_type_provenance:plus_provenance(OldProvenance, NewProvenance)
                    end,
                    NewProvenance,
                    NewProvenanceStore),
            {RemovedEventTreeLeafSet, [{[], FinalProvenanceStore}]}
    end.

-spec read(ext_type_cover:ext_subset_in_cover(), ext_type_orset_base_v7()) ->
    {ext_type_cover:ext_subset_in_cover(), sets:set()}.
read(PrevSubset, {RemovedEventTreeLeafSet, DataStoreSet}=_ORSetBaseV7) ->
    PrevSubsetRemoved =
        case RemovedEventTreeLeafSet of
            [] ->
                PrevSubset;
            _ ->
                ordsets:fold(
                    fun(EventTree, AccInPrevSubsetRemoved) ->
                        case is_event_tree_removed(EventTree, RemovedEventTreeLeafSet) of
                            true ->
                                AccInPrevSubsetRemoved;
                            false ->
                                ordsets:add_element(EventTree, AccInPrevSubsetRemoved)
                        end
                    end,
                    ordsets:new(),
                    PrevSubset)
        end,
    read_internal(PrevSubsetRemoved, DataStoreSet).

-spec join(
    ext_type_orset_base:ext_node_type(), ext_type_orset_base_v7(), ext_type_orset_base_v7()) ->
    ext_type_orset_base_v7().
join(_NodeType, ORSetBaseV7, ORSetBaseV7) ->
    ORSetBaseV7;
join(_NodeType, {[], []}=_ORSetBaseV7L, ORSetBaseV7R) ->
    ORSetBaseV7R;
join(_NodeType, ORSetBaseV7L, {[], []}=_ORSetBaseV7R) ->
    ORSetBaseV7L;
join(
    input,
    {RemovedEventTreeLeafSetL, [{[], ProvenanceStoreL}]}=ORSetBaseV7L,
    {RemovedEventTreeLeafSetR, [{[], ProvenanceStoreR}]}=ORSetBaseV7R) ->
    JoinRemovedEventTreeLeafSet = ordsets:union(RemovedEventTreeLeafSetL, RemovedEventTreeLeafSetR),
    EventTreeDictR = get_event_tree_dict_orset(ORSetBaseV7R, orddict:new()),
    JoinProvenanceStore0 =
        prune_provenance_store(
            ProvenanceStoreL, RemovedEventTreeLeafSetR, [], EventTreeDictR, orddict:new()),
    EventTreeDictL = get_event_tree_dict_orset(ORSetBaseV7L, orddict:new()),
    JoinProvenanceStore =
        prune_provenance_store(
            ProvenanceStoreR, RemovedEventTreeLeafSetL, [], EventTreeDictL, JoinProvenanceStore0),
    {JoinRemovedEventTreeLeafSet, [{[], JoinProvenanceStore}]};
join(
    intermediate,
    {RemovedEventTreeLeafSetL, DataStoreSetL}=ORSetBaseV7L,
    {RemovedEventTreeLeafSetR, DataStoreSetR}=ORSetBaseV7R) ->
    %% build a dict for the left
    EventTreeDictL = get_event_tree_dict_orset(ORSetBaseV7L, orddict:new()),
    %% prune the right and add to the result without checking
    JoinDataStoreSet0 =
        ordsets:fold(
            fun({FilteredEventTreeSetR, ProvenanceStoreR}, AccInJoinDataStoreSet0) ->
                NewFilteredEventTreeSetR =
                    prune_subset_in_cover_with_dict(FilteredEventTreeSetR, EventTreeDictL),
                NewProvenanceStoreR =
                    prune_provenance_store(
                        ProvenanceStoreR, RemovedEventTreeLeafSetL, [], EventTreeDictL,
                        orddict:new()),
                ordsets:add_element(
                    {NewFilteredEventTreeSetR, NewProvenanceStoreR}, AccInJoinDataStoreSet0)
            end,
            orddict:new(),
            DataStoreSetR),
    %% build a dict for the right
    EventTreeDictR = get_event_tree_dict_orset(ORSetBaseV7R, orddict:new()),
    %% prune the left and add to the result with checking
    JoinDataStoreSet =
        ordsets:fold(
            fun({FilteredEventTreeSetL, ProvenanceStoreL}, AccInJoinDataStoreSet) ->
                NewFilteredEventTreeSetL =
                    prune_subset_in_cover_with_dict(FilteredEventTreeSetL, EventTreeDictR),
                NewProvenanceStoreL =
                    prune_provenance_store(
                        ProvenanceStoreL, RemovedEventTreeLeafSetR, [], EventTreeDictR,
                        orddict:new()),
                add_data_store_to_data_store_set(
                    {NewFilteredEventTreeSetL, NewProvenanceStoreL}, AccInJoinDataStoreSet)
            end,
            JoinDataStoreSet0,
            DataStoreSetL),
    JoinRemovedEventTreeLeafSet =
        ordsets:union(
            prune_subset_in_cover_with_dict(RemovedEventTreeLeafSetL, EventTreeDictR),
            prune_subset_in_cover_with_dict(RemovedEventTreeLeafSetR, EventTreeDictL)),
    {JoinRemovedEventTreeLeafSet, JoinDataStoreSet}.

-spec is_inflation(ext_type_orset_base_v7(), ext_type_orset_base_v7()) -> boolean().
is_inflation(
    {RemovedEventTreeLeafSetL, DataStoreSetL}=_ORSetBaseV7L,
    {RemovedEventTreeLeafSetR, DataStoreSetR}=_ORSetBaseV7R) ->
    ordsets:is_subset(RemovedEventTreeLeafSetL, RemovedEventTreeLeafSetR) andalso
        is_inflation_data_store_set(
            DataStoreSetL, RemovedEventTreeLeafSetL, DataStoreSetR, RemovedEventTreeLeafSetR).

-spec is_strict_inflation(ext_type_orset_base_v7(), ext_type_orset_base_v7()) -> boolean().
is_strict_inflation(ORSetBaseL, ORSetBaseR) ->
    ext_type_orset_base:is_strict_inflation(ORSetBaseL, ORSetBaseR).

-spec map(
    {ext_node_id(), ext_replica_id()},
    function(),
    ext_type_path:ext_path_info_list(),
    ext_type_path:ext_path_info(),
    ext_type_orset_base_v7()) -> ext_type_orset_base_v7().
map({NodeId, _ReplicaId}=_Actor,
    Function,
    _AllPathInfoList,
    _PathInfo,
    {RemovedEventTreeLeafSet, DataStoreSet}=_ORSetBaseV7) ->
    MapDataStoreSet =
        ordsets:fold(
            fun({FilteredEventTreeSet, ProvenanceStore}, AccInMapDataStoreSet) ->
                NewProvenanceStore =
                    orddict:fold(
                        fun(Elem, Provenance, AccInProvenanceStore) ->
                            NewProvenance =
                                ordsets:fold(
                                    fun(Dot, AccInProvenance) ->
                                        NewDot =
                                            ordsets:fold(
                                                fun(EventTree, AccInDot) ->
                                                    NewEventTree = {NodeId, EventTree, undefined},
                                                    ordsets:add_element(NewEventTree, AccInDot)
                                                end,
                                                ordsets:new(),
                                                Dot),
                                        ordsets:add_element(NewDot, AccInProvenance)
                                    end,
                                    ordsets:new(),
                                    Provenance),
                            orddict:update(
                                Function(Elem),
                                fun(OldProvenance) ->
                                    ext_type_provenance:plus_provenance(
                                        OldProvenance, NewProvenance)
                                end,
                                NewProvenance,
                                AccInProvenanceStore)
                        end,
                        orddict:new(),
                        ProvenanceStore),
                ordsets:add_element(
                    {FilteredEventTreeSet, NewProvenanceStore}, AccInMapDataStoreSet)
            end,
            ordsets:new(),
            DataStoreSet),
    {RemovedEventTreeLeafSet, MapDataStoreSet}.

-spec filter(
    {ext_node_id(), ext_replica_id()},
    function(),
    ext_type_path:ext_path_info_list(),
    ext_type_path:ext_path_info(),
    ext_type_orset_base_v7()) -> ext_type_orset_base_v7().
filter(
    {NodeId, _ReplicaId}=_Actor,
    Function,
    _AllPathInfoList,
    _PathInfo,
    {RemovedEventTreeLeafSet, DataStoreSet}=_ORSetBaseV7) ->
    FilterDataStoreSet =
        ordsets:fold(
            fun({FilteredEventTreeSet, ProvenanceStore}, AccInFilterDataStoreSet) ->
                {NewProvenanceStore, NewFilteredEventTreeSet} =
                    orddict:fold(
                        fun(Elem, Provenance, {AccInProvenanceStore, AccInFilteredEventTreeSet}) ->
                            case Function(Elem) of
                                true ->
                                    NewProvenance =
                                        ordsets:fold(
                                            fun(Dot, AccInProvenance) ->
                                                NewDot =
                                                    ordsets:fold(
                                                        fun(EventTree, AccInDot) ->
                                                            NewEventTree =
                                                                {NodeId, EventTree, undefined},
                                                            ordsets:add_element(
                                                                NewEventTree, AccInDot)
                                                        end,
                                                        ordsets:new(),
                                                        Dot),
                                                ordsets:add_element(NewDot, AccInProvenance)
                                            end,
                                            ordsets:new(),
                                            Provenance),
                                    NewAccInProvenanceStore =
                                        orddict:update(
                                            Elem,
                                            fun(OldProvenance) ->
                                                ext_type_provenance:plus_provenance(
                                                    OldProvenance, NewProvenance)
                                            end,
                                            NewProvenance,
                                            AccInProvenanceStore),
                                    {NewAccInProvenanceStore, AccInFilteredEventTreeSet};
                                false ->
                                    NewAccInFilteredEventTreeSet =
                                        ordsets:fold(
                                            fun(Dot, AccInFilteredEventTreeSetDot) ->
                                                NewDot =
                                                    ordsets:fold(
                                                        fun(EventTree, AccInDot) ->
                                                            NewEventTree =
                                                                {NodeId, EventTree, undefined},
                                                            ordsets:add_element(
                                                                NewEventTree, AccInDot)
                                                        end,
                                                        ordsets:new(),
                                                        Dot),
                                                ordsets:union(AccInFilteredEventTreeSetDot, NewDot)
                                            end,
                                            AccInFilteredEventTreeSet,
                                            Provenance),
                                    {AccInProvenanceStore, NewAccInFilteredEventTreeSet}
                            end
                        end,
                        {orddict:new(), FilteredEventTreeSet},
                        ProvenanceStore),
                ordsets:add_element(
                    {NewFilteredEventTreeSet, NewProvenanceStore}, AccInFilterDataStoreSet)
            end,
            ordsets:new(),
            DataStoreSet),
    {RemovedEventTreeLeafSet, FilterDataStoreSet}.

-spec product(
    {ext_node_id(), ext_replica_id()},
    ext_type_path:ext_path_info_list(),
    ext_type_path:ext_path_info(),
    ext_type_orset_base_v7(),
    ext_type_orset_base_v7()) -> ext_type_orset_base_v7().
product(
    {NodeId, _ReplicaId}=_Actor,
    _AllPathInfoList,
    _PathInfo,
    {RemovedEventTreeLeafSetL, DataStoreSetL}=ORSetBaseV7L,
    {RemovedEventTreeLeafSetR, DataStoreSetR}=ORSetBaseV7R) ->
    %% build a dict for the left
    EventTreeDictL = get_event_tree_dict_orset(ORSetBaseV7L, orddict:new()),
    %% build a dict for the right
    EventTreeDictR = get_event_tree_dict_orset(ORSetBaseV7R, orddict:new()),
    ProductDataStoreSet =
        ordsets:fold(
            fun({FilteredEventTreeSetL, ProvenanceStoreL}, AccInProductDataStoreSetL) ->
                ordsets:fold(
                    fun({FilteredEventTreeSetR, ProvenanceStoreR}, AccInProductDataStoreSetR) ->
                        ProductFilteredEventTreeSet =
                            ordsets:union(FilteredEventTreeSetL, FilteredEventTreeSetR),
                        ProductProvenanceStore =
                            orddict:fold(
                                fun(ElemL, ProvenanceL, AccInProductProvenanceStoreL) ->
                                    orddict:fold(
                                        fun(ElemR, ProvenanceR, AccInProductProvenanceStoreR) ->
                                            ProductElem = {ElemL, ElemR},
                                            PrunedProvenanceL =
                                                ordsets:fold(
                                                    fun(DotL, AccInPrunedProvenanceL) ->
                                                        PrunedDotL =
                                                            prune_subset_in_cover_with_dict(
                                                                DotL, EventTreeDictR),
                                                        case PrunedDotL of
                                                            [] ->
                                                                AccInPrunedProvenanceL;
                                                            _ ->
                                                                ordsets:add_element(
                                                                    PrunedDotL,
                                                                    AccInPrunedProvenanceL)
                                                        end
                                                    end,
                                                    ordsets:new(),
                                                    ProvenanceL),
                                            PrunedProvenanceR =
                                                ordsets:fold(
                                                    fun(DotR, AccInPrunedProvenanceR) ->
                                                        PrunedDotR =
                                                            prune_subset_in_cover_with_dict(
                                                                DotR, EventTreeDictL),
                                                        case PrunedDotR of
                                                            [] ->
                                                                AccInPrunedProvenanceR;
                                                            _ ->
                                                                ordsets:add_element(
                                                                    PrunedDotR,
                                                                    AccInPrunedProvenanceR)
                                                        end
                                                    end,
                                                    ordsets:new(),
                                                    ProvenanceR),
                                            ProductProvenance =
                                                cross_provenance_event_tree(
                                                    PrunedProvenanceL, PrunedProvenanceR, NodeId),
                                            case ProductProvenance of
                                                [] ->
                                                    AccInProductProvenanceStoreR;
                                                _ ->
                                                    orddict:store(
                                                        ProductElem, ProductProvenance,
                                                        AccInProductProvenanceStoreR)
                                            end
                                        end,
                                        AccInProductProvenanceStoreL,
                                        ProvenanceStoreR)
                                end,
                                orddict:new(),
                                ProvenanceStoreL),
                        add_data_store_to_data_store_set(
                            {ProductFilteredEventTreeSet, ProductProvenanceStore},
                            AccInProductDataStoreSetR)
                    end,
                    AccInProductDataStoreSetL,
                    DataStoreSetR)
            end,
            ordsets:new(),
            DataStoreSetL),
    ProductRemovedEventTreeLeafSet =
        ordsets:union(
            prune_subset_in_cover_with_dict(RemovedEventTreeLeafSetL, EventTreeDictR),
            prune_subset_in_cover_with_dict(RemovedEventTreeLeafSetR, EventTreeDictL)),
    {ProductRemovedEventTreeLeafSet, ProductDataStoreSet}.

-spec consistent_read(
    ext_type_path:ext_path_info_list(),
    ext_type_cover:ext_subset_in_cover(),
    ext_type_provenance:ext_dot_set(),
    ext_type_orset_base_v7()) ->
    {ext_type_cover:ext_subset_in_cover(), ext_type_provenance:ext_dot_set(), sets:set()}.
consistent_read(
    _AllPathInfoList,
    PrevSubset,
    PrevCDS,
    {RemovedEventTreeLeafSet, DataStoreSet}=_ORSetBaseV7) ->
    PrevSubsetRemoved =
        case RemovedEventTreeLeafSet of
            [] ->
                PrevSubset;
            _ ->
                ordsets:fold(
                    fun(EventTree, AccInPrevSubsetRemoved) ->
                        case is_event_tree_removed(EventTree, RemovedEventTreeLeafSet) of
                            true ->
                                AccInPrevSubsetRemoved;
                            false ->
                                ordsets:add_element(EventTree, AccInPrevSubsetRemoved)
                        end
                    end,
                    ordsets:new(),
                    PrevSubset)
        end,
    consistent_read_internal(PrevSubsetRemoved, PrevCDS, DataStoreSet).

-spec set_count(
    {ext_node_id(), ext_replica_id()},
    ext_type_path:ext_path_info_list(),
    ext_type_path:ext_path_info(),
    ext_type_orset_base_v7()) -> ext_type_orset_base_v7().
set_count(
    {NodeId, _ReplicaId}=_Actor,
    _AllPathInfoList,
    _PathInfo,
    {RemovedEventTreeLeafSet, DataStoreSet}=_ORSetBaseV7) ->
    NewDataStoreSet =
        ordsets:fold(
            fun({FilteredEventTreeSet, ProvenanceStore}, AccInNewDataStoreSet) ->
                {CurSubset, EventTreeDict} =
                    get_event_tree_dict_with_subset_data_store(
                        FilteredEventTreeSet, ProvenanceStore, orddict:new()),
                CDS = get_cds(CurSubset, [], EventTreeDict),
                {NewFilteredEventTreeSet, NewElem, NewProvenance} =
                    orddict:fold(
                        fun(_Elem, Provenance,
                            {AccInFilteredEventTreeSet, AccInElem, AccInProvenance}) ->
                            {FilteredEventTreeSetProvenance, UpdateProvenance} =
                                ordsets:fold(
                                    fun(Dot,
                                        {
                                            AccInFilteredEventTreeSetProvenance,
                                            AccInUpdateProvenance}) ->
                                        NewDot =
                                            ordsets:fold(
                                                fun(EventTree, AccInNewDot) ->
                                                    NewEventTree = {NodeId, EventTree, undefined},
                                                    ordsets:add_element(NewEventTree, AccInNewDot)
                                                end,
                                                ordsets:new(),
                                                Dot),
                                        case is_a_dot_in_cds(Dot, CDS) of
                                            false ->
                                                {
                                                    ordsets:union(
                                                        AccInFilteredEventTreeSetProvenance,
                                                        NewDot),
                                                    AccInUpdateProvenance};
                                            true ->
                                                {
                                                    AccInFilteredEventTreeSetProvenance,
                                                    ordsets:add_element(
                                                        NewDot, AccInUpdateProvenance)}
                                        end
                                    end,
                                    {AccInFilteredEventTreeSet, ordsets:new()},
                                    Provenance),
                            case UpdateProvenance of
                                [] ->
                                    {FilteredEventTreeSetProvenance, AccInElem, AccInProvenance};
                                _ ->
                                    CombinedProvenance =
                                        cross_provenance_event_tree_agg(
                                            AccInProvenance, UpdateProvenance),
                                    {
                                        FilteredEventTreeSetProvenance,
                                        AccInElem + 1,
                                        CombinedProvenance}
                            end
                        end,
                        {FilteredEventTreeSet, 0, ?PROVENANCE_ONE},
                        ProvenanceStore),
                NewProvenanceStore = orddict:store(NewElem, NewProvenance, orddict:new()),
                ordsets:add_element(
                    {NewFilteredEventTreeSet, NewProvenanceStore}, AccInNewDataStoreSet)
            end,
            ordsets:new(),
            DataStoreSet),
    {RemovedEventTreeLeafSet, NewDataStoreSet}.

-spec group_by_sum(
    {ext_node_id(), ext_replica_id()},
    function(),
    ext_type_path:ext_path_info_list(),
    ext_type_path:ext_path_info(),
    ext_type_orset_base_v7()) -> ext_type_orset_base_v7().
group_by_sum(
    {NodeId, _ReplicaId}=_Actor,
    SumFunction,
    _AllPathInfoList,
    _PathInfo,
    {RemovedEventTreeLeafSet, DataStoreSet}=_ORSetBaseV7) ->
    NewDataStoreSet =
        ordsets:fold(
            fun({FilteredEventTreeSet, ProvenanceStore}, AccInNewDataStoreSet) ->
                {CurSubset, EventTreeDict} =
                    get_event_tree_dict_with_subset_data_store(
                        FilteredEventTreeSet, ProvenanceStore, orddict:new()),
                CDS = get_cds(CurSubset, [], EventTreeDict),
                {NewFilteredEventTreeSet, NewProvenanceStore0} =
                    orddict:fold(
                        fun({Fst, Snd}=_Elem, Provenance,
                            {AccInFilteredEventTreeSet, AccInNewProvenanceStore0}) ->
                            {FilteredEventTreeSetProvenance, UpdateProvenance} =
                                ordsets:fold(
                                    fun(Dot,
                                        {
                                            AccInFilteredEventTreeSetProvenance,
                                            AccInUpdateProvenance}) ->
                                        NewDot =
                                            ordsets:fold(
                                                fun(EventTree, AccInNewDot) ->
                                                    NewEventTree = {NodeId, EventTree, undefined},
                                                    ordsets:add_element(NewEventTree, AccInNewDot)
                                                end,
                                                ordsets:new(),
                                                Dot),
                                        case is_a_dot_in_cds(Dot, CDS) of
                                            false ->
                                                {
                                                    ordsets:union(
                                                        AccInFilteredEventTreeSetProvenance,
                                                        NewDot),
                                                    AccInUpdateProvenance};
                                            true ->
                                                {
                                                    AccInFilteredEventTreeSetProvenance,
                                                    ordsets:add_element(
                                                        NewDot, AccInUpdateProvenance)}
                                        end
                                    end,
                                    {AccInFilteredEventTreeSet, ordsets:new()},
                                    Provenance),
                            case UpdateProvenance of
                                [] ->
                                    {FilteredEventTreeSetProvenance, AccInNewProvenanceStore0};
                                _ ->
                                    UpdateProvenanceStore0 =
                                        orddict:update(
                                            Fst,
                                            fun({OldSum, OldProvenance}) ->
                                                {
                                                    SumFunction(OldSum, Snd),
                                                    cross_provenance_event_tree_agg(
                                                        OldProvenance, UpdateProvenance)}
                                            end,
                                            {
                                                SumFunction(undefined, Snd),
                                                cross_provenance_event_tree_agg(
                                                    UpdateProvenance, ?PROVENANCE_ONE)},
                                            AccInNewProvenanceStore0),
                                    {FilteredEventTreeSetProvenance, UpdateProvenanceStore0}
                            end
                        end,
                        {FilteredEventTreeSet, orddict:new()},
                        ProvenanceStore),
                NewProvenanceStore =
                    orddict:fold(
                        fun(Fst, {Snd, Provenance}, AccInNewProvenanceStore) ->
                            orddict:store({Fst, Snd}, Provenance, AccInNewProvenanceStore)
                        end,
                        orddict:new(),
                        NewProvenanceStore0),
                ordsets:add_element(
                    {NewFilteredEventTreeSet, NewProvenanceStore}, AccInNewDataStoreSet)
            end,
            ordsets:new(),
            DataStoreSet),
    {RemovedEventTreeLeafSet, NewDataStoreSet}.

-spec order_by(
    {ext_node_id(), ext_replica_id()},
    function(),
    ext_type_path:ext_path_info_list(),
    ext_type_path:ext_path_info(),
    ext_type_orset_base_v7()) -> ext_type_orset_base_v7().
order_by(
    {NodeId, _ReplicaId}=_Actor,
    CompareFunction,
    _AllPathInfoList,
    _PathInfo,
    {RemovedEventTreeLeafSet, DataStoreSet}=_ORSetBaseV7) ->
    NewDataStoreSet =
        ordsets:fold(
            fun({FilteredEventTreeSet, ProvenanceStore}, AccInNewDataStoreSet) ->
                {CurSubset, EventTreeDict} =
                    get_event_tree_dict_with_subset_data_store(
                        FilteredEventTreeSet, ProvenanceStore, orddict:new()),
                CDS = get_cds(CurSubset, [], EventTreeDict),
                {NewFilteredEventTreeSet, NewElem, NewProvenance} =
                    orddict:fold(
                        fun(Elem, Provenance,
                            {AccInFilteredEventTreeSet, AccInElem, AccInProvenance}) ->
                            {FilteredEventTreeSetProvenance, UpdateProvenance} =
                                ordsets:fold(
                                    fun(Dot,
                                        {
                                            AccInFilteredEventTreeSetProvenance,
                                            AccInUpdateProvenance}) ->
                                        NewDot =
                                            ordsets:fold(
                                                fun(EventTree, AccInNewDot) ->
                                                    NewEventTree = {NodeId, EventTree, undefined},
                                                    ordsets:add_element(NewEventTree, AccInNewDot)
                                                end,
                                                ordsets:new(),
                                                Dot),
                                        case is_a_dot_in_cds(Dot, CDS) of
                                            false ->
                                                {
                                                    ordsets:union(
                                                        AccInFilteredEventTreeSetProvenance,
                                                        NewDot),
                                                    AccInUpdateProvenance};
                                            true ->
                                                {
                                                    AccInFilteredEventTreeSetProvenance,
                                                    ordsets:add_element(
                                                        NewDot, AccInUpdateProvenance)}
                                        end
                                    end,
                                    {AccInFilteredEventTreeSet, ordsets:new()},
                                    Provenance),
                            case UpdateProvenance of
                                [] ->
                                    {FilteredEventTreeSetProvenance, AccInElem, AccInProvenance};
                                _ ->
                                    CombinedProvenance =
                                        cross_provenance_event_tree_agg(
                                            AccInProvenance, UpdateProvenance),
                                    {
                                        FilteredEventTreeSetProvenance,
                                        AccInElem ++ [Elem],
                                        CombinedProvenance}
                            end
                        end,
                        {FilteredEventTreeSet, [], ?PROVENANCE_ONE},
                        ProvenanceStore),
                NewProvenanceStore =
                    orddict:store(
                        lists:sort(CompareFunction, NewElem), NewProvenance, orddict:new()),
                ordsets:add_element(
                    {NewFilteredEventTreeSet, NewProvenanceStore}, AccInNewDataStoreSet)
            end,
            ordsets:new(),
            DataStoreSet),
    {RemovedEventTreeLeafSet, NewDataStoreSet}.

-spec next_event_history(
    ext_type_event_history:ext_event_history_type(),
    ext_node_id(),
    ext_replica_id(),
    ext_type_orset_base_v7()) -> ext_type_event_history:ext_event_history().
next_event_history(
    EventType,
    NodeId,
    ReplicaId,
    {[], []}=_ORSetBaseV7) ->
    ext_type_event_tree:get_next_event_history(EventType, NodeId, ReplicaId, ordsets:new());
next_event_history(
    EventType,
    NodeId,
    ReplicaId,
    {_RemovedEventTreeLeafSet, [{[], _ProvenanceStore}]}=ORSetBaseV7) ->
    EventTreeLeafDict = get_event_tree_dict_orset(ORSetBaseV7, orddict:new()),
    EventTreeLeafSet =
        case orddict:find(NodeId, EventTreeLeafDict) of
            {ok, EventTreeLeafSet0} ->
                EventTreeLeafSet0;
            _ ->
                ordsets:new()
        end,
    ext_type_event_tree:get_next_event_history(EventType, NodeId, ReplicaId, EventTreeLeafSet).

%% @private
read_internal(_PrevSubset, []) ->
    {ext_type_cover:new_subset_in_cover(), sets:new()};
read_internal(PrevSubset, [{FilteredEventTreeSet, ProvenanceStore} | T]=_DataStoreSet) ->
    {CurSubset, EventTreeDict} =
        get_event_tree_dict_with_subset_data_store(
            FilteredEventTreeSet, ProvenanceStore, orddict:new()),
    PrevSubsetPruned = prune_subset_in_cover_with_dict(PrevSubset, EventTreeDict),
    case ordsets:is_subset(PrevSubsetPruned, CurSubset) of
        true ->
            {CurSubset, sets:from_list(orddict:fetch_keys(ProvenanceStore))};
        false ->
            read_internal(PrevSubset, T)
    end.

%% @private
get_event_tree_dict_with_subset_data_store(FilteredEventTreeSet, ProvenanceStore, OutputDict) ->
    EventTreeDict0 =
        ordsets:fold(
            fun(FilteredEventTree, AccInEventTreeDict0) ->
                FilteredEventTreeNodeId =
                    ext_type_event_tree:get_node_id_from_event_tree(FilteredEventTree),
                orddict:update(
                    FilteredEventTreeNodeId,
                    fun(OldEventTreeSet) ->
                        ordsets:add_element(FilteredEventTree, OldEventTreeSet)
                    end,
                    ordsets:add_element(FilteredEventTree, ordsets:new()),
                    AccInEventTreeDict0)
            end,
            OutputDict,
            FilteredEventTreeSet),
    orddict:fold(
        fun(_Elem, Provenance, {AccInCurSubsetProvenance, AccInEventTreeDictProvenance}) ->
            ordsets:fold(
                fun(Dot, {AccInCurSubsetDot, AccInEventTreeDictDot}) ->
                    NewCurSubsetDot = ordsets:union(AccInCurSubsetDot, Dot),
                    NewEventTreeDictDot =
                        ordsets:fold(
                            fun(EventTree, AccInEventTreeDictEventTree) ->
                                EventTreeNodeId =
                                    ext_type_event_tree:get_node_id_from_event_tree(EventTree),
                                orddict:update(
                                    EventTreeNodeId,
                                    fun(OldEventTreeSet) ->
                                        ordsets:add_element(EventTree, OldEventTreeSet)
                                    end,
                                    ordsets:add_element(EventTree, ordsets:new()),
                                    AccInEventTreeDictEventTree)
                            end,
                            AccInEventTreeDictDot,
                            Dot),
                    {NewCurSubsetDot, NewEventTreeDictDot}
                end,
                {AccInCurSubsetProvenance, AccInEventTreeDictProvenance},
                Provenance)
        end,
        {FilteredEventTreeSet, EventTreeDict0},
        ProvenanceStore).

%% @private
prune_subset_in_cover_with_dict(Subset, EventTreeDict) ->
    ordsets:fold(
        fun(EventTree, AccInResult) ->
            case is_found_dominant_event_tree(EventTree, EventTreeDict) of
                true ->
                    AccInResult;
                false ->
                    ordsets:add_element(EventTree, AccInResult)
            end
        end,
        ordsets:new(),
        Subset).

%% @private
get_event_tree_dict_orset({RemovedEventTreeLeafSet, DataStoreSet}, OutputDict) ->
    {_CurSubset, ResultDict0} =
        ordsets:fold(
            fun({FilteredEventTreeSet, ProvenanceStore}, {AccInCurSubset, AccInResultDict0}) ->
                {NewCurSubset, NewResultDict0} =
                    get_event_tree_dict_with_subset_data_store(
                        FilteredEventTreeSet, ProvenanceStore, AccInResultDict0),
                {ordsets:union(AccInCurSubset, NewCurSubset), NewResultDict0}
            end,
            {ordsets:new(), OutputDict},
            DataStoreSet),
    ordsets:fold(
        fun(RemovedEventTreeLeaf, AccInResultDict0) ->
            EventTreeLeafNodeId =
                ext_type_event_tree:get_node_id_from_event_tree(RemovedEventTreeLeaf),
            orddict:update(
                EventTreeLeafNodeId,
                fun(OldEventTreeSet) ->
                    ordsets:add_element(RemovedEventTreeLeaf, OldEventTreeSet)
                end,
                ordsets:add_element(RemovedEventTreeLeaf, ordsets:new()),
                AccInResultDict0)
        end,
        ResultDict0,
        RemovedEventTreeLeafSet).

%% @private
prune_provenance_store(
    InputProvenanceStore,
    InputRemovedEventTreeLeafSet,
    InputFilteredEventTreeSet,
    InputEventTreeDict,
    OutputProvenanceStore) ->
    orddict:fold(
        fun(Elem, Provenance, AccInOutputProvenanceStore) ->
            NewProvenance =
                ordsets:fold(
                    fun(Dot, AccInProvenance) ->
                        NewDot =
                            ordsets:fold(
                                fun(EventTree, AccInNewDot)->
                                    case
                                        is_event_tree_removed(
                                            EventTree, InputRemovedEventTreeLeafSet) orelse
                                        is_event_tree_filtered(
                                            EventTree, InputFilteredEventTreeSet) orelse
                                        is_found_dominant_event_tree(
                                            EventTree, InputEventTreeDict) of
                                        true ->
                                            AccInNewDot;
                                        false ->
                                            ordsets:add_element(EventTree, AccInNewDot)
                                    end
                                end,
                                ordsets:new(),
                                Dot),
                        case NewDot of
                            [] ->
                                AccInProvenance;
                            _ ->
                                ordsets:add_element(NewDot, AccInProvenance)
                        end
                    end,
                    ordsets:new(),
                    Provenance),
            case NewProvenance of
                [] ->
                    AccInOutputProvenanceStore;
                _ ->
                    orddict:update(
                        Elem,
                        fun(OldProvenance) ->
                            ext_type_provenance:plus_provenance(OldProvenance, NewProvenance)
                        end,
                        NewProvenance,
                        AccInOutputProvenanceStore)
            end
        end,
        OutputProvenanceStore,
        InputProvenanceStore).

%% @private
is_event_tree_removed(EventTree, RemovedEventTreeLeafSet) ->
    EventTreeLeafSet = ext_type_event_tree:get_event_tree_leaf_set(EventTree),
    not ordsets:is_disjoint(EventTreeLeafSet, RemovedEventTreeLeafSet).

%% @private
is_event_tree_filtered(EventTree, FilteredEventTreeSet) ->
    ordsets:is_element(EventTree, FilteredEventTreeSet).

%% @private
is_found_dominant_event_tree(EventTree, EventTreeDict) ->
    EventTreeNodeId = ext_type_event_tree:get_node_id_from_event_tree(EventTree),
    case orddict:find(EventTreeNodeId, EventTreeDict) of
        {ok, EventTreeSet} ->
            case ext_type_event_tree:is_found_dominant_event_tree(
                EventTree, EventTreeSet) of
                true ->
                    true;
                false ->
                    false
            end;
        _ ->
            false
    end.

%% @private
is_inflation_data_store_set(
    [], _RemovedEventTreeLeafSetL, _DataStoreSetR, _RemovedEventTreeLeafSetR) ->
    true;
is_inflation_data_store_set(
    [H | T]=_DataStoreSetL, RemovedEventTreeLeafSetL, DataStoreSetR, RemovedEventTreeLeafSetR) ->
    case is_found_dominant_data_store(
        H, RemovedEventTreeLeafSetL, DataStoreSetR, RemovedEventTreeLeafSetR) of
        false ->
            false;
        true ->
            is_inflation_data_store_set(
                T, RemovedEventTreeLeafSetL, DataStoreSetR, RemovedEventTreeLeafSetR)
    end.

%% @private
is_found_dominant_data_store(
    _DataStoreL, _RemovedEventTreeLeafSetL, [], _RemovedEventTreeLeafSetR) ->
    false;
is_found_dominant_data_store(
    DataStoreL, RemovedEventTreeLeafSetL, [H | T]=_DataStoreSetR, RemovedEventTreeLeafSetR) ->
    case is_inflation_data_store(
        DataStoreL, RemovedEventTreeLeafSetL, H, RemovedEventTreeLeafSetR) of
        true ->
            true;
        false ->
            is_found_dominant_data_store(
                DataStoreL, RemovedEventTreeLeafSetL, T, RemovedEventTreeLeafSetR)
    end.

%% @private
is_inflation_data_store(
    {FilteredEventTreeSetL, ProvenanceStoreL}=_DataStoreL,
    _RemovedEventTreeLeafSetL,
    {FilteredEventTreeSetR, ProvenanceStoreR}=DataStoreR,
    RemovedEventTreeLeafSetR) ->
    EventTreeDictR =
        get_event_tree_dict_orset({RemovedEventTreeLeafSetR, [DataStoreR]}, orddict:new()),
    PrunedProvenanceStoreL =
        prune_provenance_store(
            ProvenanceStoreL,
            RemovedEventTreeLeafSetR, FilteredEventTreeSetR, EventTreeDictR,
            orddict:new()),
    ordsets:is_subset(FilteredEventTreeSetL, FilteredEventTreeSetR) andalso
        is_inflation_provenance_store(PrunedProvenanceStoreL, ProvenanceStoreR).

%% @private
is_inflation_provenance_store([], _ProvenanceStoreR) ->
    true;
is_inflation_provenance_store([{ElemL, ProvenanceL} | T]=_ProvenanceStoreL, ProvenanceStoreR) ->
    case orddict:find(ElemL, ProvenanceStoreR) of
        {ok, ProvenanceR} ->
            case ordsets:is_subset(ProvenanceL, ProvenanceR) of
                false ->
                    false;
                true ->
                    is_inflation_provenance_store(T, ProvenanceStoreR)
            end;
        _ ->
            false
    end.

%% @private
add_data_store_to_data_store_set({FilteredEventTreeSet, ProvenanceStore}, DataStoreSet) ->
    add_data_store_to_data_store_set_internal(
        {FilteredEventTreeSet, ProvenanceStore}, DataStoreSet, false, ordsets:new()).

%% @private
add_data_store_to_data_store_set_internal(
    {FilteredEventTreeSet, ProvenanceStore}, [], _IsFoundRelatedTo, OutputSet) ->
    ordsets:add_element({FilteredEventTreeSet, ProvenanceStore}, OutputSet);
add_data_store_to_data_store_set_internal(
    {FilteredEventTreeSetL, ProvenanceStoreL},
    [{FilteredEventTreeSetR, ProvenanceStoreR} | T]=DataStoreSet,
    IsFoundRelatedTo,
    OutputSet) ->
    case ordsets:is_subset(FilteredEventTreeSetR, FilteredEventTreeSetL) andalso
        is_inflation_provenance_store(ProvenanceStoreR, ProvenanceStoreL) of
        true ->
            add_data_store_to_data_store_set_internal(
                {FilteredEventTreeSetL, ProvenanceStoreL}, T, true, OutputSet);
        false ->
            case IsFoundRelatedTo of
                true ->
                    add_data_store_to_data_store_set_internal(
                        {FilteredEventTreeSetL, ProvenanceStoreL},
                        T,
                        true,
                        ordsets:add_element({FilteredEventTreeSetR, ProvenanceStoreR}, OutputSet));
                false ->
                    case ordsets:is_subset(FilteredEventTreeSetL, FilteredEventTreeSetR) andalso
                        is_inflation_provenance_store(ProvenanceStoreL, ProvenanceStoreR) of
                        true ->
                            ordsets:union(OutputSet, DataStoreSet);
                        false ->
                            add_data_store_to_data_store_set_internal(
                                {FilteredEventTreeSetL, ProvenanceStoreL},
                                T,
                                false,
                                ordsets:add_element(
                                    {FilteredEventTreeSetR, ProvenanceStoreR}, OutputSet))
                    end
            end
    end.

%% @private
cross_provenance_event_tree([], _PrunedProvenanceR, _NodeId) ->
    [];
cross_provenance_event_tree(_PrunedProvenanceL, [], _NodeId) ->
    [];
cross_provenance_event_tree(PrunedProvenanceL, PrunedProvenanceR, NodeId) ->
    ordsets:fold(
        fun(DotL, AccInResultL) ->
            ordsets:fold(
                fun(DotR, AccInResultR) ->
                    CrossedDot =
                        ordsets:fold(
                            fun(EventTreeL, AccInCrossedDotL) ->
                                ordsets:fold(
                                    fun(EventTreeR, AccInCrossedDotR) ->
                                        ordsets:add_element(
                                            {NodeId, EventTreeL, EventTreeR},
                                            AccInCrossedDotR)
                                    end,
                                    AccInCrossedDotL,
                                    DotR)
                            end,
                            ordsets:new(),
                            DotL),
                    ordsets:add_element(CrossedDot, AccInResultR)
                end,
                AccInResultL,
                PrunedProvenanceR)
        end,
        ordsets:new(),
        PrunedProvenanceL).

%% @private
consistent_read_internal(_PrevSubset, _PrevCDS, []) ->
    {ext_type_cover:new_subset_in_cover(), ext_type_provenance:new_dot_set(), sets:new()};
consistent_read_internal(
    PrevSubset, PrevCDS, [{FilteredEventTreeSet, ProvenanceStore} | T]=_DataStoreSet) ->
    {CurSubset, EventTreeDict} =
        get_event_tree_dict_with_subset_data_store(
            FilteredEventTreeSet, ProvenanceStore, orddict:new()),
    PrevSubsetPruned = prune_subset_in_cover_with_dict(PrevSubset, EventTreeDict),
    case ordsets:is_subset(PrevSubsetPruned, CurSubset) of
        true ->
            NewCDS = get_cds(CurSubset, PrevCDS, EventTreeDict),
            {CurSubset, NewCDS, read_with_cds(ProvenanceStore, NewCDS)};
        false ->
            consistent_read_internal(PrevSubset, PrevCDS, T)
    end.

%% @private
get_cds(Subset, _PrevCDS, EventTreeDict) ->
    EventTreeLeafDict =
        orddict:fold(
            fun(_NodeId, EventTreeSet, AccInEventTreeLeafDict) ->
                ordsets:fold(
                    fun(EventTree, AccInEventTreeLeafDictEventTree) ->
                        EventTreeLeafSet = ext_type_event_tree:get_event_tree_leaf_set(EventTree),
                        ordsets:fold(
                            fun({NId, EventId}=_EventTreeLeaf,
                                AccInInEventTreeLeafDictEventTreeLeaf) ->
                                case EventId of
                                    {ext_event_history_partial_order_group, EventIdSet} ->
                                        orddict:update(
                                            NId,
                                            fun(OldEventIdSet) ->
                                                ordsets:union(OldEventIdSet, EventIdSet)
                                            end,
                                            EventIdSet,
                                            AccInInEventTreeLeafDictEventTreeLeaf);
                                    _ ->
                                        orddict:update(
                                            NId,
                                            fun(OldEventIdSet) ->
                                                ordsets:add_element(EventId, OldEventIdSet)
                                            end,
                                            ordsets:add_element(EventId, ordsets:new()),
                                            AccInInEventTreeLeafDictEventTreeLeaf)
                                end
                            end,
                            AccInEventTreeLeafDictEventTree,
                            EventTreeLeafSet)
                    end,
                    AccInEventTreeLeafDict,
                    EventTreeSet)
            end,
            orddict:new(),
            EventTreeDict),
    EventTreeLeafSetCollection =
        orddict:fold(
            fun(NodeId, EventIdSet, AccInCollectionDict) ->
                case AccInCollectionDict of
                    [] ->
                        ordsets:fold(
                            fun(EventId, AccInNewCollectionDict) ->
                                NewEventTreeLeaf = {NodeId, EventId},
                                NewEventTreeLeafSet =
                                    ordsets:add_element(NewEventTreeLeaf, ordsets:new()),
                                ordsets:add_element(NewEventTreeLeafSet, AccInNewCollectionDict)
                            end,
                            ordsets:new(),
                            EventIdSet);
                    _ ->
                        ordsets:fold(
                            fun(EventId, AccInNewCollectionDict) ->
                                NewEventTreeLeaf = {NodeId, EventId},
                                ordsets:fold(
                                    fun(OldEventTreeLeafSet, AccInNewCollectionEventId) ->
                                        NewEventTreeLeafSet =
                                            ordsets:add_element(
                                                NewEventTreeLeaf, OldEventTreeLeafSet),
                                        ordsets:add_element(
                                            NewEventTreeLeafSet, AccInNewCollectionEventId)
                                    end,
                                    AccInNewCollectionDict,
                                    AccInCollectionDict)
                            end,
                            ordsets:new(),
                            EventIdSet)
                end
            end,
            ordsets:new(),
            EventTreeLeafDict),
    ordsets:fold(
        fun(EventTree, AccInNewCDS) ->
            EventTreeLeafSet = ext_type_event_tree:get_event_tree_leaf_set(EventTree),
            case ordsets:is_element(EventTreeLeafSet, EventTreeLeafSetCollection) of
                false ->
                    AccInNewCDS;
                true ->
                    NewDot = ordsets:add_element(EventTree, ordsets:new()),
                    ordsets:add_element(NewDot, AccInNewCDS)
            end
        end,
        ordsets:new(),
        Subset).

%% @private
read_with_cds(ProvenanceStore, CDS) ->
    orddict:fold(
        fun(Elem, Provenance, AccInResult) ->
            case is_found_a_dot_in_cds(Provenance, CDS) of
                false ->
                    AccInResult;
                true ->
                    sets:add_element(Elem, AccInResult)
            end
        end,
        sets:new(),
        ProvenanceStore).

%% @private
is_found_a_dot_in_cds([], _CDS) ->
    false;
is_found_a_dot_in_cds([H | T]=_Provenance, CDS) ->
    case is_a_dot_in_cds(H, CDS) of
        true ->
            true;
        false ->
            is_found_a_dot_in_cds(T, CDS)
    end.

%% @private
is_a_dot_in_cds([], _CDS) ->
    true;
is_a_dot_in_cds([H | T]=_Dot, CDS) ->
    EventTreeSet = ordsets:add_element(H, ordsets:new()),
    case ordsets:is_element(EventTreeSet, CDS) orelse
        ext_type_event_tree:is_valid_event_tree_agg(H) of
        false ->
            false;
        true ->
            is_a_dot_in_cds(T, CDS)
    end.

%% @private
cross_provenance_event_tree_agg(ProvenanceL, ProvenanceR) ->
    ordsets:fold(
        fun(DotL, AccInResultL) ->
            ordsets:fold(
                fun(DotR, AccInResultR) ->
                    NewDot = cross_dot_event_tree_agg(DotL, DotR),
                    ordsets:add_element(NewDot, AccInResultR)
                end,
                AccInResultL,
                ProvenanceR)
        end,
        ordsets:new(),
        ProvenanceL).

%% @private
cross_dot_event_tree_agg([], [EventTreeR]) ->
    ordsets:add_element(make_event_tree_agg(EventTreeR), ordsets:new());
cross_dot_event_tree_agg([EventTreeL], []) ->
    ordsets:add_element(make_event_tree_agg(EventTreeL), ordsets:new());
cross_dot_event_tree_agg([EventTreeL], [EventTreeR]) ->
    ordsets:add_element(combine_event_tree_agg(EventTreeL, EventTreeR), ordsets:new()).

%% @private
make_event_tree_agg(unknown) ->
    unknown;
make_event_tree_agg(undefined) ->
    undefined;
make_event_tree_agg({_NodeId, {ext_event_history_partial_order_group, _EventIdSet}}=EventTreeLeaf) ->
    EventTreeLeaf;
make_event_tree_agg({NodeId, EventId}) ->
    {NodeId, {ext_event_history_partial_order_group, ordsets:add_element(EventId, ordsets:new())}};
make_event_tree_agg({NodeId, ChildLeft, ChildRight}) ->
    {NodeId, make_event_tree_agg(ChildLeft), make_event_tree_agg(ChildRight)}.

%% @private
combine_event_tree_agg(unknown, unknown) ->
    unknown;
combine_event_tree_agg(undefined, undefined) ->
    undefined;
combine_event_tree_agg(
    {NodeId, {ext_event_history_partial_order_group, EventIdSetL}}=_EventTreeLeafL,
    {NodeId, {ext_event_history_partial_order_group, EventIdSetR}}=_EventTreeLeafR) ->
    {NodeId, {ext_event_history_partial_order_group, ordsets:union(EventIdSetL, EventIdSetR)}};
combine_event_tree_agg(
    {NodeId, {ext_event_history_partial_order_group, EventIdSetL}}=_EventTreeLeafL,
    {NodeId, EventIdR}=_EventTreeLeafR) ->
    {NodeId, {ext_event_history_partial_order_group, ordsets:add_element(EventIdR, EventIdSetL)}};
combine_event_tree_agg(
    {NodeId, EventIdL}=_EventTreeLeafL,
    {NodeId, {ext_event_history_partial_order_group, EventIdSetR}}=_EventTreeLeafR) ->
    {NodeId, {ext_event_history_partial_order_group, ordsets:add_element(EventIdL, EventIdSetR)}};
combine_event_tree_agg({NodeId, EventIdL}, {NodeId, EventIdR}) ->
    {
        NodeId,
        {
            ext_event_history_partial_order_group,
            ordsets:add_element(EventIdL, ordsets:add_element(EventIdR, ordsets:new()))}};
combine_event_tree_agg({NodeId, ChildLeftL, ChildRightL}, {NodeId, ChildLeftR, ChildRightR}) ->
    {
        NodeId,
        combine_event_tree_agg(ChildLeftL, ChildLeftR),
        combine_event_tree_agg(ChildRightL, ChildRightR)}.
