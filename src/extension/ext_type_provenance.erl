-module(ext_type_provenance).

-author("Junghun Yoo <junghun.yoo@cs.ox.ac.uk>").

%% API
-export([
    new_dot/1,
    new_provenance/1,
    new_dot_set/0,
    plus_provenance/2,
    cross_provenance/2,
    prune_provenance/2,
    append_cur_node/3,
    get_CDSs/2,
    is_valid_CDS/2,
    is_subset_CDS/2,
    has_complete_dot/2,
    is_sth_removed_dot_set/2,
    provenance_in_CDS/2,
    generate_group_event_history_for_provenance/2,
    select_CDS/1,
    find_all_super_CDSs/2,
    size_provenance/1]).
-export([
    append_cur_node/2]).
-export([
    generate_group_event_history_for_provenance_v2/2]).
-export([
    encode_provenance/2,
    decode_provenance/2,
    append_cur_node_enc/3,
    generate_group_event_history_for_provenance_dict/4,
    cross_provenance_enc/2]).

-export_type([
    ext_dot/0,
    ext_dot_set/0,
    ext_provenance/0]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type ext_dot() :: ext_type_event_history_set:ext_event_history_set().
-type ext_dot_set() :: ordsets:ordset(ext_dot()).
-type ext_provenance() :: ordsets:ordset(ext_dot()).
-type ext_node_id() :: term().
-type ext_path_info() :: term().

-spec new_dot(ext_type_event_history:ext_event_history()) -> ext_dot().
new_dot(EventHistory) ->
    ext_type_event_history_set:add_event_history(
        EventHistory, ext_type_event_history_set:new_event_history_set()).

-spec new_provenance(ext_dot()) -> ext_provenance().
new_provenance(Dot) ->
    ordsets:add_element(Dot, ordsets:new()).

-spec new_dot_set() -> ext_dot_set().
new_dot_set() ->
    ordsets:new().

-spec plus_provenance(ext_provenance(), ext_provenance()) -> ext_provenance().
plus_provenance(ProvenanceL, ProvenanceR) ->
    ordsets:union(ProvenanceL, ProvenanceR).

-spec cross_provenance(ext_provenance(), ext_provenance()) -> ext_provenance().
cross_provenance(ProvenanceL, ProvenanceR) ->
    ordsets:fold(
        fun(DotL, AccInResultProvenance0) ->
            ordsets:fold(
                fun(DotR, AccInResultProvenance1) ->
                    ordsets:add_element(
                        ext_type_event_history_set:union_event_history_set(DotL, DotR),
                        AccInResultProvenance1)
                end,
                AccInResultProvenance0,
                ProvenanceR)
        end,
        ordsets:new(),
        ProvenanceL).

-spec prune_provenance(ext_provenance(), ext_type_event_history_set:ext_event_history_set()) ->
    ext_provenance().
prune_provenance(Provenance, EventHistorySet) ->
    ordsets:fold(
        fun(Dot, AccInProvenancePruned) ->
            case ordsets:intersection(Dot, EventHistorySet) == Dot of
                false ->
                    AccInProvenancePruned;
                true ->
                    ordsets:add_element(Dot, AccInProvenancePruned)
            end
        end,
        ordsets:new(),
        Provenance).

-spec append_cur_node(ext_node_id(), ext_path_info(), ext_provenance()) -> ext_provenance().
append_cur_node(CurNodeId, CurPathInfo, Provenance) ->
    ordsets:fold(
        fun(Dot, AccInResult) ->
            NewDot = ext_type_event_history_set:append_cur_node(CurNodeId, CurPathInfo, Dot),
            ordsets:add_element(NewDot, AccInResult)
        end,
        ordsets:new(),
        Provenance).

-spec is_valid_CDS(ext_dot_set(), ext_type_event_history_set:ext_event_history_set()) -> boolean().
is_valid_CDS([], _EventHistorySet) ->
    true;
is_valid_CDS([H | T]=_CDS, EventHistorySet) ->
    case ext_type_event_history_set:is_subset(H, EventHistorySet) of
        false ->
            false;
        true ->
            is_valid_CDS(T, EventHistorySet)
    end.

-spec is_subset_CDS(ext_dot_set(), ext_dot_set()) -> boolean().
is_subset_CDS(DotSetL, DotSetR) ->
    ordsets:is_subset(DotSetL, DotSetR).

-spec has_complete_dot(ext_provenance(), ext_dot_set()) -> boolean().
has_complete_dot([], _CDS) ->
    false;
has_complete_dot([H | T]=_Provenance, CDS) ->
    DotWithoutGroup = ext_type_event_history_set:remove_group_event_history(H),
    case ordsets:is_element(DotWithoutGroup, CDS) of
        true ->
            true;
        false ->
            has_complete_dot(T, CDS)
    end.

-spec is_sth_removed_dot_set(ext_type_event_history_set:ext_event_history_set(), ext_dot_set()) ->
    boolean().
is_sth_removed_dot_set(_EventHistorySetRemoved, []) ->
    false;
is_sth_removed_dot_set(EventHistorySetRemoved, [H | T]=_DotSet) ->
    case ext_type_event_history_set:intersection_event_history_set(EventHistorySetRemoved, H) of
        [] ->
            is_sth_removed_dot_set(EventHistorySetRemoved, T);
        _ ->
            true
    end.

-spec provenance_in_CDS(ext_provenance(), ext_dot_set()) -> ext_provenance().
provenance_in_CDS(Provenance, CDS) ->
    ordsets:fold(
        fun(Dot, AccInResultProvenance) ->
            case is_dot_in_CDS(Dot, CDS) of
                false ->
                    AccInResultProvenance;
                true ->
                    ordsets:add_element(Dot, AccInResultProvenance)
            end
        end,
        ordsets:new(),
        Provenance).

-spec generate_group_event_history_for_provenance(ext_provenance(), ext_node_id()) ->
    {ext_provenance(), ext_type_event_history_set:ext_event_history_set()}.
generate_group_event_history_for_provenance(Provenance, NodeId) ->
    ordsets:fold(
        fun(Dot, {AccInResultProvenance, AccInGroupEventHistorySet}) ->
            NewGroupEventHistory =
                ext_type_event_history:new_group_event_history(NodeId, Dot),
            NewDot =
                ext_type_event_history_set:add_event_history(NewGroupEventHistory, Dot),
            {
                ordsets:add_element(NewDot, AccInResultProvenance),
                ext_type_event_history_set:add_event_history(
                    NewGroupEventHistory, AccInGroupEventHistorySet)}
        end,
        {ordsets:new(), ext_type_event_history_set:new_event_history_set()},
        Provenance).

-spec select_CDS(ordsets:ordset(ext_dot_set())) -> ext_dot_set().
select_CDS(CDSs) ->
    ordsets:fold(
        fun(CDS, AccInResultCDS) ->
            case ordsets:size(CDS) >= ordsets:size(AccInResultCDS) of
                true ->
                    CDS;
                false ->
                    AccInResultCDS
            end
        end,
        ordsets:new(),
        CDSs).

-spec get_CDSs(ext_type_path:ext_path_info_list(), ext_type_cover:ext_subset_in_cover()) ->
    ordsets:ordset(ext_dot_set()).
get_CDSs(_AllPathInfoList, []) ->
    ordsets:new();
get_CDSs(AllPathInfoList, SubsetInCoverWithGroupEvents) ->
    DataflowPathDict =
        case lasp_config:get(ext_type_version, ext_type_orset_base_v1) of
            ext_type_orset_base_v2 ->
                ext_type_path:build_atom_dataflow_path_dict(AllPathInfoList);
            _ ->
                ext_type_path:build_dataflow_path_dict(AllPathInfoList)
        end,
    {SubsetInCoverWithNoGroup, InputEventHistoryDict} =
        ext_type_event_history_set:build_event_dict(SubsetInCoverWithGroupEvents),
    InputNodeIdFromPath = ordsets:from_list(orddict:fetch_keys(DataflowPathDict)),
    InputNodeIdFromEvent = ordsets:from_list(orddict:fetch_keys(InputEventHistoryDict)),
    case ordsets:size(InputNodeIdFromEvent) == 0 orelse InputNodeIdFromPath /= InputNodeIdFromEvent of
        true ->
            ordsets:new();
        false ->
            % a collection of dict(from `ENodeId' to its `InputEventHistorySet')s
            InputEventHistoryDictCollection =
                generate_event_history_set_dict_collection(InputEventHistoryDict),
            % Combine `DataflowPathDict' and `InputEventHistorySetCollection'
            ordsets:fold(
                fun(InputEHDict, AccInCDSCollection) ->
                    % `InputEHDict' X `DataflowPathDict'
                    %  where the content of both keys should be identical
                    NewCDS = generate_CDS(InputEHDict, DataflowPathDict),
                    case ext_type_provenance:is_valid_CDS(NewCDS, SubsetInCoverWithNoGroup) of
                        true ->
                            ordsets:add_element(NewCDS, AccInCDSCollection);
                        false ->
                            AccInCDSCollection
                    end
                end,
                ordsets:new(),
                InputEventHistoryDictCollection)
    end.

-spec find_all_super_CDSs(ext_dot_set(), ordsets:ordset(ext_dot_set())) -> ordsets:ordset(ext_dot_set()).
find_all_super_CDSs(PrevCDS, CDSs) ->
    find_all_super_CDSs_internal(ordsets:new(), PrevCDS, CDSs).

-spec size_provenance(ext_provenance()) -> non_neg_integer().
size_provenance(Provenance) ->
    ordsets:size(Provenance).

-spec append_cur_node(
    ext_provenance(),
    orddict:orddict(ext_type_path:ext_dataflow_path(), ext_type_path:ext_dataflow_path())) ->
    ext_provenance().
append_cur_node(Provenance, PathDict) ->
    ordsets:fold(
        fun(Dot, AccInResult) ->
            NewDot = ext_type_event_history_set:append_cur_node(Dot, PathDict),
            ordsets:add_element(NewDot, AccInResult)
        end,
        ordsets:new(),
        Provenance).

-spec generate_group_event_history_for_provenance_v2(ext_provenance(), ext_node_id()) ->
    {ext_provenance(), ext_type_event_history_set:ext_event_history_set()}.
generate_group_event_history_for_provenance_v2(Provenance, NodeId) ->
    ordsets:fold(
        fun(Dot, {AccInResultProvenance, AccInGroupEventHistorySet}) ->
            DotWithoutGroup = ext_type_event_history_set:remove_group_event_history(Dot),
            NewGroupEventHistory =
                ext_type_event_history:new_group_event_history(NodeId, DotWithoutGroup),
            NewDot =
                ext_type_event_history_set:add_event_history(NewGroupEventHistory, DotWithoutGroup),
            {
                ordsets:add_element(NewDot, AccInResultProvenance),
                ext_type_event_history_set:add_event_history(
                    NewGroupEventHistory, AccInGroupEventHistorySet)}
        end,
        {ordsets:new(), ext_type_event_history_set:new_event_history_set()},
        Provenance).

-spec encode_provenance(ext_provenance(), term()) -> ext_provenance().
encode_provenance(ProvenanceDec, GroupEncodeDict) ->
    ordsets:fold(
        fun(DotDec, AccInResult) ->
            DotEnc = ext_type_event_history_set:encode_set(DotDec, GroupEncodeDict),
            ordsets:add_element(DotEnc, AccInResult)
        end,
        ordsets:new(),
        ProvenanceDec).

-spec decode_provenance(ext_provenance(), term()) -> ext_provenance().
decode_provenance(ProvenanceEnc, GroupDecodeDict) ->
    ordsets:fold(
        fun(DotEnc, AccInResult) ->
            DotDec = ext_type_event_history_set:decode_set(DotEnc, GroupDecodeDict),
            ordsets:add_element(DotDec, AccInResult)
        end,
        ordsets:new(),
        ProvenanceEnc).

-spec append_cur_node_enc(ext_node_id(), ext_path_info(), ext_provenance()) -> ext_provenance().
append_cur_node_enc(CurNodeId, CurPathInfo, ProvenanceEnc) ->
    ordsets:fold(
        fun(DotEnc, AccInResult) ->
            NewDotEnc =
                ext_type_event_history_set:append_cur_node_enc(CurNodeId, CurPathInfo, DotEnc),
            ordsets:add_element(NewDotEnc, AccInResult)
        end,
        ordsets:new(),
        ProvenanceEnc).

-spec generate_group_event_history_for_provenance_dict(
    ext_provenance(), ext_node_id(), term(), term()) -> {ext_provenance(), term()}.
generate_group_event_history_for_provenance_dict(
    Provenance, NodeId, GroupDecodeDict, EventHistoryAllDict) ->
    ordsets:fold(
        fun(Dot, {AccInResultProvenance, AccInGroupDecodeDict, AccInEventHistoryAllDict}) ->
            DotWithoutGroup = ext_type_event_history_set:remove_group_event_history(Dot),
            {NewGroupEventHistoryEnc, NewGroupDecodeDict, NewEventHistoryAllDict} =
                add_group_event_history_to_dict(
                    NodeId, DotWithoutGroup, AccInGroupDecodeDict, AccInEventHistoryAllDict),
            NewDot =
                ext_type_event_history_set:add_event_history(
                    NewGroupEventHistoryEnc, DotWithoutGroup),
            {
                ordsets:add_element(NewDot, AccInResultProvenance),
                NewGroupDecodeDict,
                NewEventHistoryAllDict}
        end,
        {ordsets:new(), GroupDecodeDict, EventHistoryAllDict},
        Provenance).

-spec cross_provenance_enc(ext_provenance(), ext_provenance()) -> ext_provenance().
cross_provenance_enc(ProvenanceL, ProvenanceR) ->
    ordsets:fold(
        fun(DotL, AccInResultProvenance0) ->
            ordsets:fold(
                fun(DotR, AccInResultProvenance1) ->
                    ordsets:add_element(
                        ext_type_event_history_set:join_event_history_set(DotL, DotR),
                        AccInResultProvenance1)
                end,
                AccInResultProvenance0,
                ProvenanceR)
        end,
        ordsets:new(),
        ProvenanceL).

%% @private
generate_event_history_set_dict_collection(InputEventHistoryDict) ->
    orddict:fold(
        fun(ENodeId, EventHistorySet, AccInInputEventHistoryDictCollection) ->
            % Get the power set of the input event history set
            InputEventHistoryPowerSet =
                ext_type_event_history_set:generate_power_set_without_empty_set(EventHistorySet),
            % Cross-product those power sets
            case AccInInputEventHistoryDictCollection of
                [] ->
                    ordsets:fold(
                        fun(EHSet, AccInDictCollection) ->
                            ordsets:add_element(
                                orddict:store(ENodeId, EHSet, orddict:new()),
                                AccInDictCollection)
                        end,
                        ordsets:new(),
                        InputEventHistoryPowerSet);
                _ ->
                    ordsets:fold(
                        fun(OldDict, AccInCollection) ->
                            ordsets:fold(
                                fun(NewSet, AccInAccInCollection) ->
                                    ordsets:add_element(
                                        orddict:store(ENodeId, NewSet, OldDict),
                                        AccInAccInCollection)
                                end,
                                AccInCollection,
                                InputEventHistoryPowerSet)
                        end,
                        ordsets:new(),
                        AccInInputEventHistoryDictCollection)
            end
        end,
        ordsets:new(),
        InputEventHistoryDict).

%% @private
generate_CDS(InputEHDict, DataflowPathDict) ->
    InputEHDictCollectionForADot =
        orddict:fold(
            fun(ENodeId, InputEHSet, AccInInputEHDictCollectionForADot) ->
                case AccInInputEHDictCollectionForADot of
                    [] ->
                        ordsets:fold(
                            fun(InputEH, AccInInputEHDictCollection) ->
                                ordsets:add_element(
                                    orddict:store(ENodeId, InputEH, orddict:new()),
                                    AccInInputEHDictCollection)
                            end,
                            ordsets:new(),
                            InputEHSet);
                    _ ->
                        ordsets:fold(
                            fun(EHDict, AccInInputEHDictCollection) ->
                                ordsets:fold(
                                    fun(InputEH, AccInAccInInputEHDictCollection) ->
                                        ordsets:add_element(
                                            orddict:store(ENodeId, InputEH, EHDict),
                                            AccInAccInInputEHDictCollection)
                                    end,
                                    AccInInputEHDictCollection,
                                    InputEHSet)
                            end,
                            ordsets:new(),
                            AccInInputEHDictCollectionForADot)
                end
            end,
            ordsets:new(),
            InputEHDict),
    ordsets:fold(
        fun(InputEHDictForADot, AccInCDS) ->
            NewCompleteDot =
                orddict:fold(
                    fun(NodeId, DFPathSet, AccInNewCompleteDot) ->
                        {ok, OldEventHistory} = orddict:find(NodeId, InputEHDictForADot),
                        ordsets:fold(
                            fun(DFPath, AccInAccInNewCompleteDot) ->
                                NewEventHistory =
                                    ext_type_event_history:replace_dataflow_path(
                                        DFPath, OldEventHistory),
                                ext_type_event_history_set:add_event_history(
                                    NewEventHistory, AccInAccInNewCompleteDot)
                            end,
                            AccInNewCompleteDot,
                            DFPathSet)
                    end,
                    ext_type_event_history_set:new_event_history_set(),
                    DataflowPathDict),
            ordsets:add_element(NewCompleteDot, AccInCDS)
        end,
        ordsets:new(),
        InputEHDictCollectionForADot).

%% @private
is_event_history_set_with_CDS(DotExceptGroup, CDS) ->
    Remains =
        ordsets:fold(
            fun(DotInCDS, AccInRemains) ->
                ext_type_event_history_set:minus_event_history_set(AccInRemains, DotInCDS)
            end,
            DotExceptGroup,
            CDS),
    case Remains of
        [] ->
            true;
        _ ->
            false
    end.

%% @private
is_dot_in_CDS(Dot, CDS) ->
    DotExceptGroup = ext_type_event_history_set:remove_group_event_history(Dot),
    case DotExceptGroup == Dot of
        true ->
            ordsets:is_element(DotExceptGroup, CDS);
        false ->
            is_event_history_set_with_CDS(DotExceptGroup, CDS)
    end.

%% @private
find_all_super_CDSs_internal(Result, _PrevCDS, []) ->
    Result;
find_all_super_CDSs_internal(Result, PrevCDS, [H | T]=_CDSs) ->
    case is_subset_CDS(PrevCDS, H) of
        true ->
            find_all_super_CDSs_internal(ordsets:add_element(H, Result), PrevCDS, T);
        false ->
            find_all_super_CDSs_internal(Result, PrevCDS, T)
    end.

%% @private
add_group_event_history_to_dict(NodeId, EventHistorySet, GroupDecodeDict, EventHistoryAllDict) ->
    NewGroupEventHistoryDec =
        ext_type_event_history:new_group_event_history(NodeId, EventHistorySet),
    MaxCnt =
        orddict:fold(
            fun({_EHType, {EHNodeId, EHNodeCnt}}, _GroupEHDec, AccInMaxCnt) ->
                case EHNodeId == NodeId of
                    true ->
                        max(AccInMaxCnt, EHNodeCnt);
                    false ->
                        AccInMaxCnt
                end
            end,
            0,
            GroupDecodeDict),
    NewGroupEventHistoryEnc =
        ext_type_event_history:new_group_event_history(NodeId, MaxCnt + 1),
    NewGroupDecodeDict =
        orddict:store(NewGroupEventHistoryEnc, NewGroupEventHistoryDec, GroupDecodeDict),
    NewEventHistoryAllDict0 =
        ordsets:fold(
            fun(EventHistory, AccInNewEventHistoryAllDict0) ->
                orddict:update(
                    EventHistory,
                    fun(OldGroupSet) ->
                        ordsets:add_element(NewGroupEventHistoryEnc, OldGroupSet)
                    end,
                    ordsets:add_element(NewGroupEventHistoryEnc, ordsets:new()),
                    AccInNewEventHistoryAllDict0)
            end,
            EventHistoryAllDict,
            EventHistorySet),
    NewEventHistoryAllDict =
        orddict:store(NewGroupEventHistoryEnc, ordsets:new(), NewEventHistoryAllDict0),
    {NewGroupEventHistoryEnc, NewGroupDecodeDict, NewEventHistoryAllDict}.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

generate_event_history_set_dict_collection_test() ->
    InputDict0 =
        orddict:from_list(
            [{<<"node1">>, ordsets:from_list([<<"eventhistory1">>])}]),
    ExpectedOutput0 =
        ordsets:from_list(
            [
                orddict:from_list(
                    [{<<"node1">>, ordsets:from_list([<<"eventhistory1">>])}])]),
    ?assertEqual(
        ExpectedOutput0,
        generate_event_history_set_dict_collection(InputDict0)),

    InputDict1 =
        orddict:from_list(
            [
                {<<"node1">>, ordsets:from_list([<<"eventhistory1">>])},
                {<<"node2">>, ordsets:from_list([<<"eventhistory2">>])}]),
    ExpectedOutput1 =
        ordsets:from_list(
            [
                orddict:from_list(
                    [
                        {<<"node1">>, ordsets:from_list([<<"eventhistory1">>])},
                        {<<"node2">>, ordsets:from_list([<<"eventhistory2">>])}])]),
    ?assertEqual(
        ExpectedOutput1,
        generate_event_history_set_dict_collection(InputDict1)),

    InputDict2 =
        orddict:from_list(
            [
                {<<"node1">>, ordsets:from_list([<<"eventhistory1">>])},
                {<<"node2">>, ordsets:from_list([<<"eventhistory2">>, <<"eventhistory3">>])}]),
    ExpectedOutput2 =
        ordsets:from_list(
            [
                orddict:from_list(
                    [
                        {<<"node1">>, ordsets:from_list([<<"eventhistory1">>])},
                        {<<"node2">>, ordsets:from_list([<<"eventhistory2">>])}]),
                orddict:from_list(
                    [
                        {<<"node1">>, ordsets:from_list([<<"eventhistory1">>])},
                        {<<"node2">>, ordsets:from_list([<<"eventhistory3">>])}]),
                orddict:from_list(
                    [
                        {<<"node1">>, ordsets:from_list([<<"eventhistory1">>])},
                        {<<"node2">>, ordsets:from_list([<<"eventhistory2">>, <<"eventhistory3">>])}])]),
    ?assertEqual(
        ExpectedOutput2,
        generate_event_history_set_dict_collection(InputDict2)).

-endif.
