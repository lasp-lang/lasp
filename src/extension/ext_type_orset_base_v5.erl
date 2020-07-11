-module(ext_type_orset_base_v5).

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
    ext_type_orset_base_v5/0]).

-type element() :: term().
-type ext_node_id() :: term().
-type ext_replica_id() :: term().
-type ext_event_history_all() ::
    orddict:orddict(
        ext_type_event_history:ext_event_history(),
        ordsets:ordset(ext_type_event_history:ext_event_history())).
-type ext_event_removed() :: ordsets:ordset(ext_type_event:ext_event()).
-type ext_provenance_store() :: orddict:orddict(element(), ext_type_provenance:ext_provenance()).
-type ext_data_store() ::
    orddict:orddict(ext_type_cover:ext_subset_in_cover(), ext_provenance_store()).
-opaque ext_type_orset_base_v5() ::
    {ext_event_history_all(), ext_event_removed(), ext_data_store()}.

-spec new(ext_type_path:ext_path_info_list()) -> ext_type_orset_base_v5().
new(_AllPathInfoList) ->
    {orddict:new(), ordsets:new(), orddict:new()}.

-spec equal(ext_type_orset_base_v5(), ext_type_orset_base_v5()) -> boolean().
equal(
    {EventHistoryAllDictL, EventRemovedL, DataStoreEncL}=_ORSetBaseV5L,
    {EventHistoryAllDictR, EventRemovedR, DataStoreEncR}=_ORSetBaseV5R) ->
    EventHistoryAllDictL == EventHistoryAllDictR andalso
        EventRemovedL == EventRemovedR andalso
        DataStoreEncL == DataStoreEncR.

-spec insert(ext_type_event_history:ext_event_history(), element(), ext_type_orset_base_v5()) ->
    ext_type_orset_base_v5().
insert(EventHistory, Elem, {[], [], []}=_ORSetBaseV5) ->
    NewProvenance =
        ext_type_provenance:new_provenance(ext_type_provenance:new_dot(EventHistory)),
    NewSubsetUnknownInCover = ext_type_cover:new_subset_in_cover(),
    {
        orddict:store(EventHistory, ordsets:new(), orddict:new()),
        [],
        orddict:store(
            NewSubsetUnknownInCover,
            orddict:store(Elem, NewProvenance, orddict:new()),
            orddict:new())};
insert(EventHistory, Elem, {EventHistoryAllDict, EventRemoved, DataStore}=ORSetBaseV5) ->
    {EventHistoryAll, []} = ext_type_event_history_set:dict_to_set(EventHistoryAllDict),
    CanBeSkipped =
        ext_type_event_history_set:is_found_dominant_event_history(EventHistory, EventHistoryAll),
    case CanBeSkipped of
        true ->
            ORSetBaseV5;
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
            {NewEventHistoryAllDict, []} = ext_type_event_history_set:set_to_dict(NewEventHistoryAll),
            {
                NewEventHistoryAllDict,
                EventRemoved,
                orddict:store(NewSingleSubsetUnknown, NewProvenanceStore, orddict:new())}
    end.

-spec read(ext_type_cover:ext_subset_in_cover(), ext_type_orset_base_v5()) ->
    {ext_type_cover:ext_subset_in_cover(), sets:set()}.
read(
    PrevSubsetDec,
    {EventHistoryAllDict, _EventRemoved, DataStoreEnc}=_ORSetBaseV5) ->
    case DataStoreEnc of
        [] ->
            {ext_type_cover:new_subset_in_cover(), sets:new()};
        _ ->
            {EventHistoryAllDec, GroupDecodeDict} =
                ext_type_event_history_set:dict_to_set(EventHistoryAllDict),
            SubsetDecList =
                lists:foldl(
                    fun(SubsetEnc, AccInSubsetDecList) ->
                        AccInSubsetDecList ++
                        [ext_type_cover:decode_subset(SubsetEnc, GroupDecodeDict)]
                    end,
                    [],
                    orddict:fetch_keys(DataStoreEnc)),
            CoverDec = ext_type_cover:generate_cover(SubsetDecList, EventHistoryAllDec),
            SuperSubsetsDec = ext_type_cover:find_all_super_subsets(PrevSubsetDec, CoverDec),
            SuperSubsetDec = ext_type_cover:select_subset(SuperSubsetsDec),
            NewSubsetUnknownDec =
                ext_type_event_history_set:subtract(EventHistoryAllDec, SuperSubsetDec),
            GroupEncodeDict = reverse_dict(GroupDecodeDict),
            NewSubsetUnknownEnc = ext_type_cover:encode_subset(NewSubsetUnknownDec, GroupEncodeDict),
            {
                SuperSubsetDec,
                sets:from_list(orddict:fetch_keys(orddict:fetch(NewSubsetUnknownEnc, DataStoreEnc)))}
    end.

-spec join(
    ext_type_orset_base:ext_node_type(), ext_type_orset_base_v5(), ext_type_orset_base_v5()) ->
    ext_type_orset_base_v5().
join(_, {[], [], []}, ORSetBaseV5) ->
    ORSetBaseV5;
join(_, ORSetBaseV5, {[], [], []}) ->
    ORSetBaseV5;
join(
    _NodeType, ORSetBaseV5, ORSetBaseV5) ->
    ORSetBaseV5;
join(
    input,
    {
        EventHistoryAllDictL,
        EventRemovedL,
        [{SingleSubsetUnknownL, ProvenanceStoreL}]=_DataStoreL}=_ORSetBaseV5L,
    {
        EventHistoryAllDictR,
        EventRemovedR,
        [{SingleSubsetUnknownR, ProvenanceStoreR}]=_DataStoreR}=_ORSetBaseV5R) ->
    {EventHistoryAllL, []} = ext_type_event_history_set:dict_to_set(EventHistoryAllDictL),
    {EventHistoryAllR, []} = ext_type_event_history_set:dict_to_set(EventHistoryAllDictR),
    JoinedEventHistoryAll =
        ext_type_event_history_set:union_event_history_set(EventHistoryAllL, EventHistoryAllR),
    {JoinedEventHistoryAllDict, []} = ext_type_event_history_set:set_to_dict(JoinedEventHistoryAll),

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

    {JoinedEventHistoryAllDict, JoinedEventRemoved, JoinedDataStore};
join(
    intermediate,
    {EventHistoryAllDictL, EventRemovedL, DataStoreEncL}=_ORSetBaseV5L,
    {EventHistoryAllDictR, EventRemovedR, DataStoreEncR}=_ORSetBaseV5R) ->
    {EventHistoryAllDecL, GroupDecodeDictL} =
        ext_type_event_history_set:dict_to_set(EventHistoryAllDictL),
    {EventHistoryAllDecR, GroupDecodeDictR} =
        ext_type_event_history_set:dict_to_set(EventHistoryAllDictR),
    JoinedEventHistoryAllDec =
        ext_type_event_history_set:union_event_history_set(EventHistoryAllDecL, EventHistoryAllDecR),
    {JoinedEventHistoryAllDict, GroupEncodeDict} =
        ext_type_event_history_set:set_to_dict(JoinedEventHistoryAllDec),

    JoinedEventRemoved = ordsets:union(EventRemovedL, EventRemovedR),

    JoinedEventHistorySurvivedDec =
        ext_type_event_history_set:find_survived(JoinedEventHistoryAllDec, JoinedEventRemoved),

    JoinedDataStoreDec0 =
        orddict:fold(
            fun(SubsetUnknownEncL, ProvenanceStoreEncL, AccInJoinedDataStoreDec0) ->
                SubsetUnknownDecL = ext_type_cover:decode_subset(SubsetUnknownEncL, GroupDecodeDictL),
                SubsetDecL =
                    ext_type_event_history_set:subtract(EventHistoryAllDecL, SubsetUnknownDecL),
                PrunedSubsetDecL = ext_type_cover:prune_subset(SubsetDecL, JoinedEventHistoryAllDec),
                PrunedSubsetUnknownDecL =
                    ext_type_event_history_set:subtract(JoinedEventHistoryAllDec, PrunedSubsetDecL),
                PrunedProvenanceStoreDecL =
                    orddict:fold(
                        fun(Elem, ProvenanceEnc, AccInPrunedProvenanceStoreDecL) ->
                            NewProvenanceDec =
                                ext_type_provenance:prune_provenance(
                                    ext_type_provenance:decode_provenance(
                                        ProvenanceEnc, GroupDecodeDictL),
                                    JoinedEventHistorySurvivedDec),
                            case NewProvenanceDec of
                                [] ->
                                    AccInPrunedProvenanceStoreDecL;
                                _ ->
                                    orddict:store(
                                        Elem,
                                        NewProvenanceDec,
                                        AccInPrunedProvenanceStoreDecL)
                            end
                        end,
                        orddict:new(),
                        ProvenanceStoreEncL),
                add_subset_provenance_store(
                    PrunedSubsetUnknownDecL, PrunedProvenanceStoreDecL,
                    AccInJoinedDataStoreDec0, JoinedEventHistoryAllDec)
            end,
            orddict:new(),
            DataStoreEncL),
    JoinedDataStoreDec =
        orddict:fold(
            fun(SubsetUnknownEncR, ProvenanceStoreEncR, AccInJoinedDataStoreDec) ->
                SubsetUnknownDecR = ext_type_cover:decode_subset(SubsetUnknownEncR, GroupDecodeDictR),
                SubsetDecR =
                    ext_type_event_history_set:subtract(EventHistoryAllDecR, SubsetUnknownDecR),
                PrunedSubsetDecR = ext_type_cover:prune_subset(SubsetDecR, JoinedEventHistoryAllDec),
                PrunedSubsetUnknownDecR =
                    ext_type_event_history_set:subtract(JoinedEventHistoryAllDec, PrunedSubsetDecR),
                PrunedProvenanceStoreDecR =
                    orddict:fold(
                        fun(Elem, ProvenanceEnc, AccInPrunedProvenanceStoreDecR) ->
                            NewProvenanceDec =
                                ext_type_provenance:prune_provenance(
                                    ext_type_provenance:decode_provenance(
                                        ProvenanceEnc, GroupDecodeDictR),
                                    JoinedEventHistorySurvivedDec),
                            case NewProvenanceDec of
                                [] ->
                                    AccInPrunedProvenanceStoreDecR;
                                _ ->
                                    orddict:store(
                                        Elem,
                                        NewProvenanceDec,
                                        AccInPrunedProvenanceStoreDecR)
                            end
                        end,
                        orddict:new(),
                        ProvenanceStoreEncR),
                add_subset_provenance_store(
                    PrunedSubsetUnknownDecR, PrunedProvenanceStoreDecR,
                    AccInJoinedDataStoreDec, JoinedEventHistoryAllDec)
            end,
            JoinedDataStoreDec0,
            DataStoreEncR),
    JoinedDataStore = encode_data_store(JoinedDataStoreDec, GroupEncodeDict),

    {JoinedEventHistoryAllDict, JoinedEventRemoved, JoinedDataStore}.

-spec is_inflation(ext_type_orset_base_v5(), ext_type_orset_base_v5()) -> boolean().
is_inflation(
    {EventHistoryAllDictL, EventRemovedL, DataStoreEncL}=_ORSetBaseV5L,
    {EventHistoryAllDictR, EventRemovedR, DataStoreEncR}=_ORSetBaseV5R) ->
% (EventHistoryAllL \subseteq EventHistoryAllR)
% \land
% (EventHistorySurvivedR \cap EventHistoryAllL \subseteq EventHistorySurvivedL)
% \land
% ((CoverL, DataStoreL) \sqsubseteq (CoverR, DataStoreR))
    % only for input nodes !!!
    {EventHistoryAllDecL, GroupDecodeDictL} =
        ext_type_event_history_set:dict_to_set(EventHistoryAllDictL),
    {EventHistoryAllDecR, GroupDecodeDictR} =
        ext_type_event_history_set:dict_to_set(EventHistoryAllDictR),
    ext_type_event_history_set:is_orderly_subset(EventHistoryAllDecL, EventHistoryAllDecR) andalso
        ordsets:is_subset(EventRemovedL, EventRemovedR) andalso
        is_inflation_cover_data_store(
            DataStoreEncL, EventHistoryAllDecL, GroupDecodeDictL,
            DataStoreEncR, EventHistoryAllDecR, GroupDecodeDictR,
            ext_type_event_history_set:find_survived(EventHistoryAllDecR, EventRemovedR)).

-spec is_strict_inflation(ext_type_orset_base_v5(), ext_type_orset_base_v5()) -> boolean().
is_strict_inflation(ORSetBaseL, ORSetBaseR) ->
    ext_type_orset_base:is_strict_inflation(ORSetBaseL, ORSetBaseR).

-spec map(
    {ext_node_id(), ext_replica_id()},
    function(),
    ext_type_path:ext_path_info_list(),
    ext_type_path:ext_path_info(),
    ext_type_orset_base_v5()) -> ext_type_orset_base_v5().
map({NodeId, _ReplicaId}=_Actor,
    Function,
    _AllPathInfoList,
    PathInfo,
    {EventHistoryAllDict, EventRemoved, DataStoreEnc}=_ORSetBaseV5) ->
    NewEventHistoryAllDict = append_cur_node_dict(NodeId, PathInfo, EventHistoryAllDict),

    NewDataStoreEnc =
        orddict:fold(
            fun(SubsetUnknownEnc, ProvenanceStoreEnc, AccInNewDataStoreEnc) ->
                NewSubsetUnknownEnc =
                    ext_type_cover:append_cur_node_enc(NodeId, PathInfo, SubsetUnknownEnc),
                NewProvenanceStoreEnc =
                    orddict:fold(
                        fun(Elem, ProvenanceEnc, AccInNewProvenanceStoreEnc) ->
                            NewProvenanceEnc =
                                ext_type_provenance:append_cur_node_enc(
                                    NodeId, PathInfo, ProvenanceEnc),
                            orddict:update(
                                Function(Elem),
                                fun(OldProvenanceEnc) ->
                                    ext_type_provenance:plus_provenance(
                                        OldProvenanceEnc, NewProvenanceEnc)
                                end,
                                NewProvenanceEnc,
                                AccInNewProvenanceStoreEnc)
                        end,
                        orddict:new(),
                        ProvenanceStoreEnc),
                orddict:store(NewSubsetUnknownEnc, NewProvenanceStoreEnc, AccInNewDataStoreEnc)
            end,
            orddict:new(),
            DataStoreEnc),

    {NewEventHistoryAllDict, EventRemoved, NewDataStoreEnc}.

-spec filter(
    {ext_node_id(), ext_replica_id()},
    function(),
    ext_type_path:ext_path_info_list(),
    ext_type_path:ext_path_info(),
    ext_type_orset_base_v5()) -> ext_type_orset_base_v5().
filter(
    {NodeId, _ReplicaId}=_Actor,
    Function,
    _AllPathInfoList,
    PathInfo,
    {EventHistoryAllDict, EventRemoved, DataStoreEnc}=_ORSetBaseV5) ->
    NewEventHistoryAllDict = append_cur_node_dict(NodeId, PathInfo, EventHistoryAllDict),

    NewDataStore =
        orddict:fold(
            fun(SubsetUnknownEnc, ProvenanceStore, AccInNewDataStore) ->
                NewSubsetUnknownEnc =
                    ext_type_cover:append_cur_node_enc(NodeId, PathInfo, SubsetUnknownEnc),
                NewProvenanceStore =
                    orddict:fold(
                        fun(Elem, ProvenanceEnc, AccInNewProvenanceStore) ->
                            case Function(Elem) of
                                false ->
                                    AccInNewProvenanceStore;
                                true ->
                                    NewProvenanceEnc =
                                        ext_type_provenance:append_cur_node_enc(
                                            NodeId, PathInfo, ProvenanceEnc),
                                    orddict:store(Elem, NewProvenanceEnc, AccInNewProvenanceStore)
                            end
                        end,
                        orddict:new(),
                        ProvenanceStore),
                orddict:store(NewSubsetUnknownEnc, NewProvenanceStore, AccInNewDataStore)
            end,
            orddict:new(),
            DataStoreEnc),

    {NewEventHistoryAllDict, EventRemoved, NewDataStore}.

-spec product(
    {ext_node_id(), ext_replica_id()},
    ext_type_path:ext_path_info_list(),
    ext_type_path:ext_path_info(),
    ext_type_orset_base_v5(),
    ext_type_orset_base_v5()) -> ext_type_orset_base_v5().
product(
    {NodeId, _ReplicaId}=_Actor,
    _AllPathInfoList,
    PathInfo,
    {EventHistoryAllDictL, EventRemovedL, DataStoreEncL}=_ORSetBaseV5L,
    {EventHistoryAllDictR, EventRemovedR, DataStoreEncR}=_ORSetBaseV5R) ->
    {EventHistoryAllDecL, GroupDecodeDictL} =
        ext_type_event_history_set:dict_to_set(EventHistoryAllDictL),
    {EventHistoryAllDecR, GroupDecodeDictR} =
        ext_type_event_history_set:dict_to_set(EventHistoryAllDictR),
    ProductEventHistoryAllDec0 =
        ext_type_event_history_set:union_event_history_set(EventHistoryAllDecL, EventHistoryAllDecR),
    ProductEventHistoryAllDec =
        ext_type_event_history_set:append_cur_node(NodeId, PathInfo, ProductEventHistoryAllDec0),
    {ProductEventHistoryAllDict, GroupEncodeDict} =
        ext_type_event_history_set:set_to_dict(ProductEventHistoryAllDec),

    ProductEventRemoved = ordsets:union(EventRemovedL, EventRemovedR),

    ProductEventHistorySurvivedDec =
        ext_type_event_history_set:find_survived(ProductEventHistoryAllDec, ProductEventRemoved),

    ProductDataStoreDec =
        orddict:fold(
            fun(SubsetUnknownEncL, ProvenanceStoreEncL, AccInProductDataStoreDecL) ->
                orddict:fold(
                    fun(SubsetUnknownEncR, ProvenanceStoreEncR, AccInProductDataStoreDecR) ->
                        SubsetUnknownDecL =
                            ext_type_cover:decode_subset(SubsetUnknownEncL, GroupDecodeDictL),
                        SubsetDecL =
                            ext_type_event_history_set:subtract(EventHistoryAllDecL, SubsetUnknownDecL),
                        SubsetUnknownDecR =
                            ext_type_cover:decode_subset(SubsetUnknownEncR, GroupDecodeDictR),
                        SubsetDecR =
                            ext_type_event_history_set:subtract(EventHistoryAllDecR, SubsetUnknownDecR),
                        ProductSubsetDec0 =
                            ext_type_event_history_set:union_event_history_set(SubsetDecL, SubsetDecR),
                        ProductSubsetDec1 =
                            ext_type_cover:append_cur_node(NodeId, PathInfo, ProductSubsetDec0),
                        ProductSubsetDec =
                            ext_type_cover:prune_subset(ProductSubsetDec1, ProductEventHistoryAllDec),
                        ProductSubsetUnknownDec =
                            ext_type_event_history_set:subtract(ProductEventHistoryAllDec, ProductSubsetDec),

                        ProductProvenanceStoreDec =
                            orddict:fold(
                                fun(ElemL,
                                    ProvenanceEncL,
                                    AccInProductProvenanceStoreDecL) ->
                                    orddict:fold(
                                        fun(ElemR,
                                            ProvenanceEncR,
                                            AccInProductProvenanceStoreDecR) ->
                                            NewProvenanceDec0 =
                                                ext_type_provenance:cross_provenance(
                                                    ext_type_provenance:decode_provenance(
                                                        ProvenanceEncL, GroupDecodeDictL),
                                                    ext_type_provenance:decode_provenance(
                                                        ProvenanceEncR, GroupDecodeDictR)),
                                            NewProvenanceDec1 =
                                                ext_type_provenance:append_cur_node(
                                                    NodeId, PathInfo, NewProvenanceDec0),
                                            NewProvenanceDec =
                                                ext_type_provenance:prune_provenance(
                                                    NewProvenanceDec1, ProductEventHistorySurvivedDec),
                                            case NewProvenanceDec of
                                                [] ->
                                                    AccInProductProvenanceStoreDecR;
                                                _ ->
                                                    orddict:store(
                                                        {ElemL, ElemR},
                                                        NewProvenanceDec,
                                                        AccInProductProvenanceStoreDecR)
                                            end
                                        end,
                                        AccInProductProvenanceStoreDecL,
                                        ProvenanceStoreEncR)
                                end,
                                orddict:new(),
                                ProvenanceStoreEncL),

                        add_subset_provenance_store(
                            ProductSubsetUnknownDec, ProductProvenanceStoreDec,
                            AccInProductDataStoreDecR, ProductEventHistoryAllDec)
                    end,
                    AccInProductDataStoreDecL,
                    DataStoreEncR)
            end,
            orddict:new(),
            DataStoreEncL),
    ProductDataStore = encode_data_store(ProductDataStoreDec, GroupEncodeDict),

    {ProductEventHistoryAllDict, ProductEventRemoved, ProductDataStore}.

-spec consistent_read(
    ext_type_path:ext_path_info_list(),
    ext_type_cover:ext_subset_in_cover(),
    ext_type_provenance:ext_dot_set(),
    ext_type_orset_base_v5()) ->
    {ext_type_cover:ext_subset_in_cover(), ext_type_provenance:ext_dot_set(), sets:set()}.
consistent_read(
    AllPathInfoList,
    PrevSubsetDec,
    PrevCDS,
    {EventHistoryAllDict, _EventRemoved, DataStoreEnc}=_ORSetBaseV5) ->
    case DataStoreEnc of
        [] ->
            {ordsets:new(), ordsets:new(), sets:new()};
        _ ->
            {EventHistoryAllDec, GroupDecodeDict} =
                ext_type_event_history_set:dict_to_set(EventHistoryAllDict),
            SubsetDecList =
                lists:foldl(
                    fun(SubsetEnc, AccInSubsetDecList) ->
                        AccInSubsetDecList ++
                            [ext_type_cover:decode_subset(SubsetEnc, GroupDecodeDict)]
                    end,
                    [],
                    orddict:fetch_keys(DataStoreEnc)),
            CoverDec = ext_type_cover:generate_cover(SubsetDecList, EventHistoryAllDec),
            SuperSubsetsDec = ext_type_cover:find_all_super_subsets(PrevSubsetDec, CoverDec),
            SuperSubsetDec = ext_type_cover:select_subset(SuperSubsetsDec),
            NewSubsetUnknownDec =
                ext_type_event_history_set:subtract(EventHistoryAllDec, SuperSubsetDec),
            GroupEncodeDict = reverse_dict(GroupDecodeDict),
            NewSubsetUnknownEnc = ext_type_cover:encode_subset(NewSubsetUnknownDec, GroupEncodeDict),
            NewCDSs = ext_type_provenance:get_CDSs(AllPathInfoList, SuperSubsetDec),
            SuperCDSs = ext_type_provenance:find_all_super_CDSs(PrevCDS, NewCDSs),
            NewCDS = ext_type_provenance:select_CDS(SuperCDSs),
            {
                SuperSubsetDec,
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
                    orddict:fetch(NewSubsetUnknownEnc, DataStoreEnc))}
    end.

-spec set_count(
    {ext_node_id(), ext_replica_id()},
    ext_type_path:ext_path_info_list(),
    ext_type_path:ext_path_info(),
    ext_type_orset_base_v5()) -> ext_type_orset_base_v5().
set_count(
    {NodeId, _ReplicaId}=_Actor,
    AllPathInfoList,
    PathInfo,
    {EventHistoryAllDict, EventRemoved, DataStoreEnc}=_ORSetBaseV5) ->
    {_EventHistoryAllDec, GroupDecodeDict} =
        ext_type_event_history_set:dict_to_set(EventHistoryAllDict),
    NewEventHistoryAllDict = append_cur_node_dict(NodeId, PathInfo, EventHistoryAllDict),
    {NewEventHistoryAllDec, NewGroupDecodeDictBeforeGroup} =
        ext_type_event_history_set:dict_to_set(NewEventHistoryAllDict),

    {NewDataStoreEnc, _NewGroupDecodeDict, NewEventHistoryAllDictWithNewGroup} =
        orddict:fold(
            fun(SubsetUnknownEnc,
                ProvenanceStoreEnc,
                {
                    AccInNewDataStoreEnc,
                    AccInNewGroupDecodeDict,
                    AccInNewEventHistoryAllDictWithNewGroup}) ->
                SubsetUnknownDec = ext_type_cover:decode_subset(SubsetUnknownEnc, GroupDecodeDict),
                NewSubsetUnknownDec =
                    ext_type_event_history_set:append_cur_node(NodeId, PathInfo, SubsetUnknownDec),
                CDS =
                    ext_type_provenance:select_CDS(
                        ext_type_provenance:get_CDSs(
                            AllPathInfoList,
                            ext_type_event_history_set:subtract(
                                NewEventHistoryAllDec, NewSubsetUnknownDec))),
                {NewElem, NewProvenanceEnc} =
                    orddict:fold(
                        fun(_Elem, ProvenanceEnc, {AccInNewElem, AccInNewProvenanceEnc}) ->
                            ProvenanceEncInCDS =
                                ext_type_provenance:provenance_in_CDS(
                                    ext_type_provenance:append_cur_node_enc(
                                        NodeId, PathInfo, ProvenanceEnc),
                                    CDS),
                            case ProvenanceEncInCDS of
                                [] ->
                                    {AccInNewElem, AccInNewProvenanceEnc};
                                _ ->
                                    {
                                        AccInNewElem + 1,
                                        ext_type_provenance:cross_provenance_enc(
                                            AccInNewProvenanceEnc,
                                            ProvenanceEncInCDS)}
                            end
                        end,
                        {0, ?PROVENANCE_ONE},
                        ProvenanceStoreEnc),
                {NewProvenanceStoreEnc, GroupDecodeDictWithNewGroup, EventHistoryAllDictWithNewGroup} =
                    case NewElem of
                        0 ->
                            {
                                orddict:new(),
                                AccInNewGroupDecodeDict,
                                AccInNewEventHistoryAllDictWithNewGroup};
                        _ ->
                            {
                                NewProvenanceEncWithGroup,
                                NewGroupDecodeDict0,
                                NewEventHistoryAllDictWithNewGroup0} =
                                ext_type_provenance:generate_group_event_history_for_provenance_dict(
                                    NewProvenanceEnc, NodeId,
                                    AccInNewGroupDecodeDict, AccInNewEventHistoryAllDictWithNewGroup),
                            {
                                orddict:store(NewElem, NewProvenanceEncWithGroup, orddict:new()),
                                NewGroupDecodeDict0,
                                NewEventHistoryAllDictWithNewGroup0}
                    end,
                NewSubsetUnknownEnc =
                    ext_type_cover:encode_subset(
                        NewSubsetUnknownDec, reverse_dict(GroupDecodeDictWithNewGroup)),
                {
                    orddict:store(NewSubsetUnknownEnc, NewProvenanceStoreEnc, AccInNewDataStoreEnc),
                    GroupDecodeDictWithNewGroup,
                    EventHistoryAllDictWithNewGroup}
            end,
            {orddict:new(), NewGroupDecodeDictBeforeGroup, NewEventHistoryAllDict},
            DataStoreEnc),

    {
        NewEventHistoryAllDictWithNewGroup,
        EventRemoved,
        NewDataStoreEnc}.

-spec group_by_sum(
    {ext_node_id(), ext_replica_id()},
    function(),
    ext_type_path:ext_path_info_list(),
    ext_type_path:ext_path_info(),
    ext_type_orset_base_v5()) -> ext_type_orset_base_v5().
group_by_sum(
    {NodeId, _ReplicaId}=_Actor,
    SumFunction,
    AllPathInfoList,
    PathInfo,
    {EventHistoryAllDict, EventRemoved, DataStoreEnc}=_ORSetBaseV5) ->
    {_EventHistoryAllDec, GroupDecodeDict} =
        ext_type_event_history_set:dict_to_set(EventHistoryAllDict),
    NewEventHistoryAllDict = append_cur_node_dict(NodeId, PathInfo, EventHistoryAllDict),
    {NewEventHistoryAllDec, NewGroupDecodeDictBeforeGroup} =
        ext_type_event_history_set:dict_to_set(NewEventHistoryAllDict),

    {NewDataStoreEnc, _NewGroupDecodeDict, NewEventHistoryAllDictWithNewGroup} =
        orddict:fold(
            fun(SubsetUnknownEnc,
                ProvenanceStoreEnc,
                {
                    AccInNewDataStoreEnc,
                    AccInNewGroupDecodeDict,
                    AccInNewEventHistoryAllDictWithNewGroup}) ->
                SubsetUnknownDec = ext_type_cover:decode_subset(SubsetUnknownEnc, GroupDecodeDict),
                NewSubsetUnknownDec =
                    ext_type_event_history_set:append_cur_node(NodeId, PathInfo, SubsetUnknownDec),
                CDS =
                    ext_type_provenance:select_CDS(
                        ext_type_provenance:get_CDSs(
                            AllPathInfoList,
                            ext_type_event_history_set:subtract(
                                NewEventHistoryAllDec, NewSubsetUnknownDec))),
                NewProvenanceStoreEnc0 =
                    orddict:fold(
                        fun({Fst, Snd}=_Elem, ProvenanceEnc, AccInNewProvenanceStoreEnc0) ->
                            ProvenanceEncInCDS =
                                ext_type_provenance:provenance_in_CDS(
                                    ext_type_provenance:append_cur_node_enc(
                                        NodeId, PathInfo, ProvenanceEnc),
                                    CDS),
                            case ProvenanceEncInCDS of
                                [] ->
                                    AccInNewProvenanceStoreEnc0;
                                _ ->
                                    orddict:update(
                                        Fst,
                                        fun({OldSum, OldProvenance}) ->
                                            {
                                                SumFunction(OldSum, Snd),
                                                ext_type_provenance:cross_provenance_enc(
                                                    OldProvenance,
                                                    ProvenanceEncInCDS)}
                                        end,
                                        {SumFunction(undefined, Snd), ProvenanceEncInCDS},
                                        AccInNewProvenanceStoreEnc0)
                            end
                        end,
                        orddict:new(),
                        ProvenanceStoreEnc),
                {NewProvenanceStoreEnc, GroupDecodeDictWithNewGroup, EventHistoryAllDictWithNewGroup} =
                    orddict:fold(
                        fun(Fst,
                            {Snd, ProvenanceEncInPS0},
                            {
                                AccInNewProvenanceStoreEnc,
                                AccInGroupDecodeDictWithNewGroup,
                                AccInEventHistoryAllDictWithNewGroup}) ->
                            {
                                NewProvenanceEncWithGroup,
                                NewGroupDecodeDict0,
                                NewEventHistoryAllDictWithNewGroup0} =
                                ext_type_provenance:generate_group_event_history_for_provenance_dict(
                                    ProvenanceEncInPS0, NodeId,
                                    AccInGroupDecodeDictWithNewGroup,
                                    AccInEventHistoryAllDictWithNewGroup),
                            {
                                orddict:store(
                                    {Fst, Snd},
                                    NewProvenanceEncWithGroup,
                                    AccInNewProvenanceStoreEnc),
                                NewGroupDecodeDict0,
                                NewEventHistoryAllDictWithNewGroup0}
                        end,
                        {orddict:new(), AccInNewGroupDecodeDict, AccInNewEventHistoryAllDictWithNewGroup},
                        NewProvenanceStoreEnc0),
                NewSubsetUnknownEnc =
                    ext_type_cover:encode_subset(
                        NewSubsetUnknownDec, reverse_dict(GroupDecodeDictWithNewGroup)),
                {
                    orddict:store(NewSubsetUnknownEnc, NewProvenanceStoreEnc, AccInNewDataStoreEnc),
                    GroupDecodeDictWithNewGroup,
                    EventHistoryAllDictWithNewGroup}
            end,
            {orddict:new(), NewGroupDecodeDictBeforeGroup, NewEventHistoryAllDict},
            DataStoreEnc),

    {
        NewEventHistoryAllDictWithNewGroup,
        EventRemoved,
        NewDataStoreEnc}.

-spec order_by(
    {ext_node_id(), ext_replica_id()},
    function(),
    ext_type_path:ext_path_info_list(),
    ext_type_path:ext_path_info(),
    ext_type_orset_base_v5()) -> ext_type_orset_base_v5().
order_by(
    {NodeId, _ReplicaId}=_Actor,
    CompareFunction,
    AllPathInfoList,
    PathInfo,
    {EventHistoryAllDict, EventRemoved, DataStoreEnc}=_ORSetBaseV5) ->
    {_EventHistoryAllDec, GroupDecodeDict} =
        ext_type_event_history_set:dict_to_set(EventHistoryAllDict),
    NewEventHistoryAllDict = append_cur_node_dict(NodeId, PathInfo, EventHistoryAllDict),
    {NewEventHistoryAllDec, NewGroupDecodeDictBeforeGroup} =
        ext_type_event_history_set:dict_to_set(NewEventHistoryAllDict),

    {NewDataStoreEnc, _NewGroupDecodeDict, NewEventHistoryAllDictWithNewGroup} =
        orddict:fold(
            fun(SubsetUnknownEnc,
                ProvenanceStoreEnc,
                {
                    AccInNewDataStoreEnc,
                    AccInNewGroupDecodeDict,
                    AccInNewEventHistoryAllDictWithNewGroup}) ->
                SubsetUnknownDec = ext_type_cover:decode_subset(SubsetUnknownEnc, GroupDecodeDict),
                NewSubsetUnknownDec =
                    ext_type_event_history_set:append_cur_node(NodeId, PathInfo, SubsetUnknownDec),
                CDS =
                    ext_type_provenance:select_CDS(
                        ext_type_provenance:get_CDSs(
                            AllPathInfoList,
                            ext_type_event_history_set:subtract(
                                NewEventHistoryAllDec, NewSubsetUnknownDec))),
                {NewElem, NewProvenanceEnc} =
                    orddict:fold(
                        fun(Elem, ProvenanceEnc, {AccInNewElem, AccInNewProvenanceEnc}) ->
                            ProvenanceEncInCDS =
                                ext_type_provenance:provenance_in_CDS(
                                    ext_type_provenance:append_cur_node_enc(
                                        NodeId, PathInfo, ProvenanceEnc),
                                    CDS),
                            case ProvenanceEncInCDS of
                                [] ->
                                    {AccInNewElem, AccInNewProvenanceEnc};
                                _ ->
                                    {
                                        AccInNewElem ++ [Elem],
                                        ext_type_provenance:cross_provenance_enc(
                                            AccInNewProvenanceEnc,
                                            ProvenanceEncInCDS)}
                            end
                        end,
                        {[], ?PROVENANCE_ONE},
                        ProvenanceStoreEnc),
                {NewProvenanceStoreEnc, GroupDecodeDictWithNewGroup, EventHistoryAllDictWithNewGroup} =
                    case NewElem of
                        [] ->
                            {
                                orddict:new(),
                                AccInNewGroupDecodeDict,
                                AccInNewEventHistoryAllDictWithNewGroup};
                        _ ->
                            {
                                NewProvenanceEncWithGroup,
                                NewGroupDecodeDict0,
                                NewEventHistoryAllDictWithNewGroup0} =
                                ext_type_provenance:generate_group_event_history_for_provenance_dict(
                                    NewProvenanceEnc, NodeId,
                                    AccInNewGroupDecodeDict, AccInNewEventHistoryAllDictWithNewGroup),
                            {
                                orddict:store(
                                    lists:sort(CompareFunction, NewElem),
                                    NewProvenanceEncWithGroup,
                                    orddict:new()),
                                NewGroupDecodeDict0,
                                NewEventHistoryAllDictWithNewGroup0}
                    end,
                NewSubsetUnknownEnc =
                    ext_type_cover:encode_subset(
                        NewSubsetUnknownDec, reverse_dict(GroupDecodeDictWithNewGroup)),
                {
                    orddict:store(NewSubsetUnknownEnc, NewProvenanceStoreEnc, AccInNewDataStoreEnc),
                    GroupDecodeDictWithNewGroup,
                    EventHistoryAllDictWithNewGroup}
            end,
            {orddict:new(), NewGroupDecodeDictBeforeGroup, NewEventHistoryAllDict},
            DataStoreEnc),

    {
        NewEventHistoryAllDictWithNewGroup,
        EventRemoved,
        NewDataStoreEnc}.

-spec next_event_history(
    ext_type_event_history:ext_event_history_type(),
    ext_node_id(),
    ext_replica_id(),
    ext_type_orset_base_v5()) -> ext_type_event_history:ext_event_history().
next_event_history(
    EventType,
    NodeId,
    ReplicaId,
    {EventHistoryAllDict, _EventRemoved, _DataStore}=_ORSetBaseV5L) ->
    {EventHistoryAll, []} = ext_type_event_history_set:dict_to_set(EventHistoryAllDict),
    ext_type_event_history:next_event_history(EventType, NodeId, ReplicaId, EventHistoryAll).

%% @private
is_inflation_provenance_store(
    [], _GroupDecodeDictL, _ProvenanceStoreEncR, _GroupDecodeDictR, _EventHistorySurvivedDecR) ->
    true;
is_inflation_provenance_store(
    [{Elem, ProvenanceEncL} | T]=_ProvenanceStoreEncL, GroupDecodeDictL,
    ProvenanceStoreEncR, GroupDecodeDictR,
    EventHistorySurvivedDecR) ->
    ProvenanceDecL = ext_type_provenance:decode_provenance(ProvenanceEncL, GroupDecodeDictL),
    DotSetDecL = ext_type_provenance:prune_provenance(ProvenanceDecL, EventHistorySurvivedDecR),
    ProvenanceEncR =
        case orddict:find(Elem, ProvenanceStoreEncR) of
            {ok, Provenance} ->
                Provenance;
            error ->
                ordsets:new()
        end,
    ProvenanceDecR = ext_type_provenance:decode_provenance(ProvenanceEncR, GroupDecodeDictR),
    DotSetDecR = ext_type_provenance:prune_provenance(ProvenanceDecR, EventHistorySurvivedDecR),
    case ordsets:is_subset(DotSetDecL, DotSetDecR) of
        false ->
            false;
        true ->
            is_inflation_provenance_store(T, GroupDecodeDictL,
                ProvenanceStoreEncR, GroupDecodeDictR,
                EventHistorySurvivedDecR)
    end.

%% @private
is_inflation_data_store(
    [], _DataStoreEncR, _GroupDecodeDictR, _EventHistorySurvivedDecR,
    _ProvenanceStoreEncL, _GroupDecodeDictL) ->
    true;
is_inflation_data_store(
    [H | T]=_RelatedSubsetUnknownEncsR, DataStoreEncR, GroupDecodeDictR, EventHistorySurvivedDecR,
    ProvenanceStoreEncL, GroupDecodeDictL) ->
    case is_inflation_provenance_store(
        ProvenanceStoreEncL, GroupDecodeDictL,
        orddict:fetch(H, DataStoreEncR), GroupDecodeDictR,
        EventHistorySurvivedDecR) of
        false ->
            false;
        true ->
            is_inflation_data_store(
                T, DataStoreEncR, GroupDecodeDictR, EventHistorySurvivedDecR,
                ProvenanceStoreEncL, GroupDecodeDictL)
    end.

%% @private
is_inflation_cover_data_store(
    [], _EventHistoryAllL, _GroupDecodeDictL,
    _DataStoreEncR, _EventHistoryAllR, _GroupDecodeDictR,
    _EventHistorySurvivedR) ->
    true;
is_inflation_cover_data_store(
    [{SubsetUnknownEncL, ProvenanceStoreEncL} | T]=_DataStoreEncL, EventHistoryAllDecL, GroupDecodeDictL,
    DataStoreEncR, EventHistoryAllDecR, GroupDecodeDictR,
    EventHistorySurvivedDecR) ->
    SubsetUnknownDecL = ext_type_cover:decode_subset(SubsetUnknownEncL, GroupDecodeDictL),
    SubsetDecL = ext_type_event_history_set:subtract(EventHistoryAllDecL, SubsetUnknownDecL),
    RelatedSubsetUnknownEncsR =
        lists:foldl(
            fun(SubsetUnknownEncR, AccInRelatedSubsetUnknownEncs) ->
                SubsetUnknownDecR = ext_type_cover:decode_subset(SubsetUnknownEncR, GroupDecodeDictR),
                SubsetDecR =
                    ext_type_event_history_set:subtract(EventHistoryAllDecR, SubsetUnknownDecR),
                case ext_type_event_history_set:is_orderly_subset(SubsetDecL, SubsetDecR) of
                    false ->
                        AccInRelatedSubsetUnknownEncs;
                    true ->
                        ordsets:add_element(SubsetUnknownEncR, AccInRelatedSubsetUnknownEncs)
                end
            end,
            ordsets:new(),
            orddict:fetch_keys(DataStoreEncR)),
    case RelatedSubsetUnknownEncsR of
        [] ->
            false;
        _ ->
            case is_inflation_data_store(
                RelatedSubsetUnknownEncsR, DataStoreEncR, GroupDecodeDictR, EventHistorySurvivedDecR,
                ProvenanceStoreEncL, GroupDecodeDictL) of
                false ->
                    false;
                true ->
                    is_inflation_cover_data_store(
                        T, EventHistoryAllDecL, GroupDecodeDictL,
                        DataStoreEncR, EventHistoryAllDecR, GroupDecodeDictR,
                        EventHistorySurvivedDecR)
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

%% @private
encode_provenance_store(ProvenanceStoreDec, GroupEncodeDict) ->
    orddict:fold(
        fun(Elem, ProvenanceDec, AccInResult) ->
            orddict:store(
                Elem,
                ext_type_provenance:encode_provenance(ProvenanceDec, GroupEncodeDict),
                AccInResult)
        end,
        orddict:new(),
        ProvenanceStoreDec).

%% @private
encode_data_store(DataStoreDec, GroupEncodeDict) ->
    orddict:fold(
        fun(SubsetUnknownDec, ProvenanceStoreDec, AccInResult) ->
            orddict:store(
                ext_type_cover:encode_subset(SubsetUnknownDec, GroupEncodeDict),
                encode_provenance_store(ProvenanceStoreDec, GroupEncodeDict),
                AccInResult)
        end,
        orddict:new(),
        DataStoreDec).

%% @private
append_cur_node_dict(NodeId, PathInfo, EventHistoryAllDict) ->
    orddict:fold(
        fun(EventHistory, GroupSet, AccInResult) ->
            NewEventHistory =
                case ext_type_event_history:get_event(EventHistory) of
                    group_event ->
                        EventHistory;
                    _ ->
                        ext_type_event_history:append_cur_node(NodeId, PathInfo, EventHistory)
                end,
            orddict:store(NewEventHistory, GroupSet, AccInResult)
        end,
        orddict:new(),
        EventHistoryAllDict).

%% @private
reverse_dict(InputDict) ->
    orddict:fold(
        fun(Key, Value, AccInResult) ->
            orddict:store(Value, Key, AccInResult)
        end,
        orddict:new(),
        InputDict).
