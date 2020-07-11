-module(ext_type_orset_base_v6).

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
    ext_type_orset_base_v6/0]).

-type element() :: term().
-type ext_node_id() :: term().
-type ext_replica_id() :: term().
-type ext_event_removed() :: ordsets:ordset(ext_type_event:ext_event()).
-type ext_provenance_store() :: orddict:orddict(element(), ext_type_provenance:ext_provenance()).
-type ext_data_store() ::
    orddict:orddict(ext_type_cover:ext_subset_in_cover(), ext_provenance_store()).
-opaque ext_type_orset_base_v6() ::
    {ext_path_enc_dict(), ext_event_history_all(), ext_event_removed(), ext_data_store()}.

-type ext_path_enc() :: {ext_node_id(), pos_integer()}.
-type ext_path_enc_dict() :: orddict:orddict(ext_path_enc(), ext_path_enc()).
-type ext_event_history_group_dict() ::
    orddict:orddict(
        ext_type_event_history:ext_event_history(),
        ordsets:ordset(ext_type_event_history:ext_event_history())).
-type ext_event_history_all() ::
    orddict:orddict(
        ext_type_event_history:ext_event_history_type(), ext_event_history_group_dict()).

%% ext_type_event_history.erl
event_history_types() ->
    [
        ext_event_history_partial_order_downward_closed,
        ext_event_history_partial_order_group,
        ext_event_history_partial_order_independent,
        ext_event_history_total_order
    ].

get_next_event_history_dict(
    ext_event_history_partial_order_independent, NodeId, ReplicaId, EventHistoryAllDict) ->
    MaxCnt =
        lists:foldl(
            fun(
                {
                    ext_event_history_partial_order_independent,
                    {{ENodeId, EReplicaId, ECount}=_Event, _DFPath}=_EventHistoryContents},
                AccInMaxCnt) ->
                case ENodeId == NodeId andalso EReplicaId == ReplicaId of
                    true ->
                        max(AccInMaxCnt, ECount);
                    false ->
                        AccInMaxCnt
                end;
            ({_EventHistoryTYpe, _EventHistoryContents}, AccInMaxCnt) ->
                    AccInMaxCnt
            end,
            0,
            orddict:fetch_keys(EventHistoryAllDict)),
    {
        ext_event_history_partial_order_independent,
        {
            ext_type_event:new_event(NodeId, ReplicaId, MaxCnt + 1),
            input_dataflow_path(NodeId)}};
get_next_event_history_dict(
    ext_event_history_total_order, NodeId, ReplicaId, EventHistoryAllDict) ->
    case orddict:find(NodeId, EventHistoryAllDict) of
        error ->
            {
                ext_event_history_total_order,
                {
                    ext_type_event:new_event(NodeId, ReplicaId, 1),
                    input_dataflow_path(NodeId)}};
        {ok, {EventHistory, _GroupSet}} ->
            {ext_event_history_total_order, {{_, _, Count}, _}} = EventHistory,
            {
                ext_event_history_total_order,
                {
                    ext_type_event:new_event(NodeId, ReplicaId, Count + 1),
                    input_dataflow_path(NodeId)}}
    end.

add_event_history_dict(
    {ext_event_history_partial_order_independent, _EventHistoryContents}=EventHistory,
    EventHistoryAllDict) ->
    {orddict:store(EventHistory, ordsets:new(), EventHistoryAllDict), ordsets:new()};
add_event_history_dict(
    {ext_event_history_total_order, {{NodeId, _, _}, _}}=EventHistory,
    EventHistoryAllDict) ->
    case orddict:find(NodeId, EventHistoryAllDict) of
        error ->
            {
                orddict:store(NodeId, {EventHistory, ordsets:new()}, EventHistoryAllDict),
                ordsets:new()};
        {ok, {EventHistoryInDict, _GroupSet}} ->
            case ext_type_event_history:is_related_to(EventHistoryInDict, EventHistory) of
                true ->
                    {
                        orddict:store(NodeId, {EventHistory, ordsets:new()}, EventHistoryAllDict),
                        ordsets:add_element(EventHistoryInDict, ordsets:new())};
                false ->
                    {EventHistoryAllDict, ordsets:new()}
            end
    end.

%% ext_type_path.erl
%% @private
get_index_internal(_Index, _DPath, []) ->
    0;
get_index_internal(Index, DPath, [H | T]=_DPathSet) ->
    case DPath == H of
        true ->
            Index;
        false ->
            get_index_internal(Index + 1, DPath, T)
    end.

%% @private
get_index(DPath, DPathSet) ->
    get_index_internal(1, DPath, DPathSet).

%% @private
add_path_enc_dict(
    ChildType, ChildNodeId, ChildPathDict, ParentNodeId, ParentPathDict, ResultDict) ->
    orddict:fold(
        fun(InputNodeId, DPathSet, AccInResultDict) ->
            {_Index, NewResultDict} =
                ordsets:fold(
                    fun(DPathChild, {AccInIndex, AccInNewResultDict}) ->
                        ParentDPathSet = orddict:fetch(InputNodeId, ParentPathDict),
                        ParentIndex =
                            case ChildType of
                                left_und ->
                                    get_index(
                                        {ParentNodeId, {DPathChild, undefined}}, ParentDPathSet);
                                right_und ->
                                    get_index(
                                        {ParentNodeId, {undefined, DPathChild}}, ParentDPathSet);
                                left_unk ->
                                    get_index(
                                        {ParentNodeId, {DPathChild, unknown}}, ParentDPathSet);
                                right_unk ->
                                    get_index(
                                        {ParentNodeId, {unknown, DPathChild}}, ParentDPathSet)
                            end,
                        NewResultDict0 =
                            orddict:store(
                                {ChildNodeId, {InputNodeId, AccInIndex}},
                                {ParentNodeId, {InputNodeId, ParentIndex}},
                                AccInNewResultDict),
                        {AccInIndex + 1, NewResultDict0}
                    end,
                    {1, AccInResultDict},
                    DPathSet),
            NewResultDict
        end,
        ResultDict,
        ChildPathDict).

%% @private
divide_path_info_list_internal(LeftList, _RightChild, []) ->
    {LeftList, []};
divide_path_info_list_internal(LeftList, RightChild, [H | T]=PathInfoList) ->
    case H of
        {RightChild, {_, _}} ->
            {LeftList, PathInfoList};
        _ ->
            divide_path_info_list_internal(LeftList ++ [H], RightChild, T)
    end.

%% @private
divide_path_info_list(RightChild, PathInfoList) ->
    divide_path_info_list_internal([], RightChild, PathInfoList).

generate_path_enc_dict([H | T]=AllPathInfoList) ->
    {CurNodeId, PrevNodeIdPair} = H,
    case PrevNodeIdPair of
        {undefined, undefined} ->
            orddict:store(undefined, {CurNodeId, {CurNodeId, 1}}, orddict:new());
        {undefined, RightChild} ->
            CurPathDict = ext_type_path:build_dataflow_path_dict(AllPathInfoList),
            RightPathDict = ext_type_path:build_dataflow_path_dict(T),
            add_path_enc_dict(
                right_und, RightChild, RightPathDict, CurNodeId, CurPathDict, orddict:new());
        {LeftChild, undefined} ->
            CurPathDict = ext_type_path:build_dataflow_path_dict(AllPathInfoList),
            LeftPathDict = ext_type_path:build_dataflow_path_dict(T),
            add_path_enc_dict(
                left_und, LeftChild, LeftPathDict, CurNodeId, CurPathDict, orddict:new());
        {LeftChild, RightChild} ->
            CurPathDict = ext_type_path:build_dataflow_path_dict(AllPathInfoList),
            {LeftAllPathInfoList, RightAllPathInfoList} = divide_path_info_list(RightChild, T),
            LeftPathDict = ext_type_path:build_dataflow_path_dict(LeftAllPathInfoList),
            RightPathDict = ext_type_path:build_dataflow_path_dict(RightAllPathInfoList),
            Result0 =
                add_path_enc_dict(
                    left_unk, LeftChild, LeftPathDict, CurNodeId, CurPathDict, orddict:new()),
            add_path_enc_dict(
                right_unk, RightChild, RightPathDict, CurNodeId, CurPathDict, Result0)
    end.

input_dataflow_path(InputNodeId) ->
    {InputNodeId, {InputNodeId, 1}}.

%% ext_type_orset_base_v6.erl
%% @private
all_dicts_to_set(EventHistoryAllDicts) ->
    {EventHistoryAllDec0, GroupDecodeDict} =
        ordsets:fold(
            fun(ext_event_history_partial_order_group,
                {AccInEventHistoryAllDec, AccInGroupDecodeDict}) ->
                {AccInEventHistoryAllDec, AccInGroupDecodeDict};
            (ext_event_history_total_order, {AccInEventHistoryAllDec, AccInGroupDecodeDict}) ->
                EventHistoryEncDict =
                    orddict:fetch(ext_event_history_total_order, EventHistoryAllDicts),
                orddict:fold(
                    fun(_,
                        {EventHistory, GroupEncSet},
                        {AccInNewEventHistoryAllDec, AccInNewGroupDecodeDict}) ->
                        NewEventHistoryAllDec =
                            ordsets:add_element(EventHistory, AccInNewEventHistoryAllDec),
                        NewGroupDecodeDict =
                            update_group_decode_dict(
                                EventHistory, GroupEncSet, AccInNewGroupDecodeDict),
                        {NewEventHistoryAllDec, NewGroupDecodeDict}
                    end,
                    {AccInEventHistoryAllDec, AccInGroupDecodeDict},
                    EventHistoryEncDict);
            (EventHistoryType, {AccInEventHistoryAllDec, AccInGroupDecodeDict}) ->
                EventHistoryEncDict = orddict:fetch(EventHistoryType, EventHistoryAllDicts),
                orddict:fold(
                    fun(EventHistory,
                        GroupEncSet,
                        {AccInNewEventHistoryAllDec, AccInNewGroupDecodeDict}) ->
                        NewEventHistoryAllDec =
                            ordsets:add_element(EventHistory, AccInNewEventHistoryAllDec),
                        NewGroupDecodeDict =
                            update_group_decode_dict(
                                EventHistory, GroupEncSet, AccInNewGroupDecodeDict),
                        {NewEventHistoryAllDec, NewGroupDecodeDict}
                    end,
                    {AccInEventHistoryAllDec, AccInGroupDecodeDict},
                    EventHistoryEncDict)
            end,
            {ordsets:new(), orddict:new()},
            event_history_types()),
    EventHistoryAllDec =
        orddict:fold(
            fun(_GroupEnc, GroupDec, AccInEventHistoryAllDec) ->
                ordsets:add_element(GroupDec, AccInEventHistoryAllDec)
            end,
            EventHistoryAllDec0,
            GroupDecodeDict),
    {EventHistoryAllDec, GroupDecodeDict}.

%% @private
update_group_decode_dict(EventHistory, GroupEncSet, GroupDecodeDict) ->
    ordsets:fold(
        fun({ext_event_history_partial_order_group, {NodeId, _}}=GroupEnc,
            AccInNewGroupDecodeDict) ->
            orddict:update(
                GroupEnc,
                fun({ext_event_history_partial_order_group, {OldNodeId, OldEHSet}}=_OldGroupDec) ->
                    {
                        ext_event_history_partial_order_group,
                        {OldNodeId, ordsets:add_element(EventHistory, OldEHSet)}}
                end,
                {
                    ext_event_history_partial_order_group,
                    {NodeId, ordsets:add_element(EventHistory, ordsets:new())}},
                AccInNewGroupDecodeDict)
        end,
        GroupDecodeDict,
        GroupEncSet).

%% @private
read_internal(_PrevSubsetDec, _EventHistoryAllDicts, _EventHistoryAllDec, _GroupDecodeDict, []) ->
    {ext_type_cover:new_subset_in_cover(), sets:new()};
read_internal(
    PrevSubsetDec,
    EventHistoryAllDicts,
    EventHistoryAllDec,
    GroupDecodeDict,
    [H | T]=_DataStoreEnc) ->
    {NewEventHistoryAllDec, NewGroupDecodeDict} =
        case {EventHistoryAllDec, GroupDecodeDict} of
            {[], []} ->
                all_dicts_to_set(EventHistoryAllDicts);
            _ ->
                {EventHistoryAllDec, GroupDecodeDict}
        end,
    {SubsetUnknownEnc, ProvenanceStoreEnc} = H,
    SubsetUnknownDec = ext_type_cover:decode_subset(SubsetUnknownEnc, NewGroupDecodeDict),
    SubsetDec = ordsets:subtract(NewEventHistoryAllDec, SubsetUnknownDec),
    case ext_type_event_history_set:is_orderly_subset(PrevSubsetDec, SubsetDec) of
        true ->
            {SubsetDec, sets:from_list(orddict:fetch_keys(ProvenanceStoreEnc))};
        false ->
            read_internal(
                PrevSubsetDec, EventHistoryAllDicts, NewEventHistoryAllDec, NewGroupDecodeDict, T)
    end.

%% @private
join_event_history_all_dict_input(
    ext_event_history_partial_order_independent, EventHistoryAllDictL, EventHistoryAllDictR) ->
    JoinedEventHistoryAllDict =
        orddict:merge(
            fun(_EventHistory, _GroupEncSetL, _GroupEncSetR) ->
                ordsets:new()
            end,
            EventHistoryAllDictL,
            EventHistoryAllDictR),
    {JoinedEventHistoryAllDict, ordsets:new(), ordsets:new()};
join_event_history_all_dict_input(
    ext_event_history_total_order, EventHistoryAllDictL, EventHistoryAllDictR) ->
    {JoinDict0, PrunedL, PrunedR, DupNodeIdSet} =
        orddict:fold(
            fun(NodeIdL,
                {EventHistoryL, GroupEncSetL},
                {AccInJoinDict0, AccInPrunedL, AccInPrunedR, AccInDupNodeIdSet}) ->
                case orddict:find(NodeIdL, EventHistoryAllDictR) of
                    error ->
                        {
                            orddict:store(NodeIdL, {EventHistoryL, GroupEncSetL}, AccInJoinDict0),
                            AccInPrunedL,
                            AccInPrunedR,
                            AccInDupNodeIdSet};
                    {ok, {EventHistoryR, GroupEncSetR}} ->
                        {NewAccInJoinDict0, NewAccInPrunedL, NewAccInPrunedR} =
                            case ext_type_event_history:is_related_to(
                                EventHistoryL, EventHistoryR) of
                                true ->
                                    {
                                        orddict:store(
                                            NodeIdL,
                                            {EventHistoryR, GroupEncSetR},
                                            AccInJoinDict0),
                                        ordsets:add_element(EventHistoryL, AccInPrunedL),
                                        AccInPrunedR};
                                false ->
                                    {
                                        orddict:store(
                                            NodeIdL,
                                            {EventHistoryL, GroupEncSetL},
                                            AccInJoinDict0),
                                        AccInPrunedL,
                                        ordsets:add_element(EventHistoryR, AccInPrunedR)}
                            end,
                        {
                            NewAccInJoinDict0,
                            NewAccInPrunedL,
                            NewAccInPrunedR,
                            ordsets:add_element(NodeIdL, AccInDupNodeIdSet)}
                end
            end,
            {orddict:new(), ordsets:new(), ordsets:new(), ordsets:new()},
            EventHistoryAllDictL),
    JoinDict1 =
        orddict:fold(
            fun(NodeIdR, {EventHistoryR, GroupEncSetR}, AccInJoinDict1) ->
                case ordsets:is_element(NodeIdR, DupNodeIdSet) of
                    true ->
                        AccInJoinDict1;
                    false ->
                        orddict:store(NodeIdR, {EventHistoryR, GroupEncSetR}, AccInJoinDict1)
                end
            end,
            JoinDict0,
            EventHistoryAllDictR),
    {JoinDict1, PrunedL, PrunedR};
join_event_history_all_dict_input(
    _EventHistoryType, _EventHistoryAllDictL, _EventHistoryAllDictR) ->
    {orddict:new(), ordsets:new(), ordsets:new()}.

%% @private
join_event_history_all_dicts_input(EventHistoryAllDictsL, EventHistoryAllDictsR) ->
    ordsets:fold(
        fun(ext_event_history_partial_order_group,
            {AccInJoinedEventHistoryAllDicts, AccInPrunedEncSetL, AccInPrunedEncSetR}) ->
            NewAccInJoinedEventHistoryAllDicts =
                orddict:store(
                    ext_event_history_partial_order_group,
                    orddict:new(),
                    AccInJoinedEventHistoryAllDicts),
            {NewAccInJoinedEventHistoryAllDicts, AccInPrunedEncSetL, AccInPrunedEncSetR};
        (
            EventHistoryType,
            {AccInJoinedEventHistoryAllDicts, AccInPrunedEncSetL, AccInPrunedEncSetR}) ->
            {NewJoinedEventHistoryAllDict, NewPrunedEncSetL, NewPrunedEncSetR} =
                join_event_history_all_dict_input(
                    EventHistoryType,
                    orddict:fetch(EventHistoryType, EventHistoryAllDictsL),
                    orddict:fetch(EventHistoryType, EventHistoryAllDictsR)),
            {
                orddict:store(
                    EventHistoryType,
                    NewJoinedEventHistoryAllDict,
                    AccInJoinedEventHistoryAllDicts),
                ordsets:union(AccInPrunedEncSetL, NewPrunedEncSetL),
                ordsets:union(AccInPrunedEncSetR, NewPrunedEncSetR)}
        end,
        {orddict:new(), ordsets:new(), ordsets:new()},
        event_history_types()).

%% @private
is_inflation_event_history_all_dicts(EventHistoryAllDictsL, EventHistoryAllDictsR) ->
    is_inflation_event_history_all_dicts_internal(
        true,
        EventHistoryAllDictsL,
        orddict:new(),
        EventHistoryAllDictsR,
        orddict:new(),
        [
            ext_event_history_total_order,
            ext_event_history_partial_order_downward_closed,
            ext_event_history_partial_order_independent,
            ext_event_history_partial_order_group],
        ordsets:new()).

%% @private
is_inflation_event_history_all_dicts_internal(
    false, _, _, _, _, _, _) ->
    {false, orddict:new(), orddict:new(), ordsets:new()};
is_inflation_event_history_all_dicts_internal(
    true, _, GroupDecodeDictL, _, GroupDecodeDictR, [], PrunedEncSetL) ->
    {true, GroupDecodeDictL, GroupDecodeDictR, PrunedEncSetL};
is_inflation_event_history_all_dicts_internal(
    true,
    EventHistoryAllDictsL, GroupDecodeDictL,
    EventHistoryAllDictsR, GroupDecodeDictR,
    [H | T]=_EventHistoryTypeList,
    PrunedEncSetL) ->
    {IsInflationEventHistoryDict, NewGroupDecodeDictL, NewGroupDecodeDictR, NewPrunedEncSetL} =
        is_inflation_event_history_dict(
            H,
            orddict:fetch(H, EventHistoryAllDictsL),
            GroupDecodeDictL,
            orddict:fetch(H, EventHistoryAllDictsR),
            GroupDecodeDictR,
            PrunedEncSetL),
    case IsInflationEventHistoryDict of
        false ->
            {false, orddict:new(), orddict:new(), ordsets:new()};
        true ->
            is_inflation_event_history_all_dicts_internal(
                true,
                EventHistoryAllDictsL, NewGroupDecodeDictL,
                EventHistoryAllDictsR, NewGroupDecodeDictR,
                T,
                NewPrunedEncSetL)
    end.

%% @private
is_inflation_event_history_dict(
    ext_event_history_partial_order_group,
    [], GroupDecodeDictL,
    _EventHistoryDictR, GroupDecodeDictR,
    PrunedEncSetL) ->
    {true, GroupDecodeDictL, GroupDecodeDictR, PrunedEncSetL};
is_inflation_event_history_dict(
    ext_event_history_partial_order_group,
    [H | T]=_EventHistoryDictL, GroupDecodeDictL,
    EventHistoryDictR, GroupDecodeDictR,
    PrunedEncSetL) ->
    {GroupEncL, []} = H,
    GroupDecL = orddict:fetch(GroupEncL, GroupDecodeDictL),
    {IsInflation, IsPruned} =
        orddict:fold(
            fun(GroupEncR, [], {AccInIsInflation, AccInIsPruned}) ->
                GroupDecR = orddict:fetch(GroupEncR, GroupDecodeDictR),
                NewAccInIsInflation =
                    AccInIsInflation orelse ordsets:is_subset(GroupDecL, GroupDecR),
                NewAccInIsPruned =
                    AccInIsPruned orelse
                        (ordsets:is_subset(GroupDecL, GroupDecR) andalso GroupDecL /= GroupDecR),
                {NewAccInIsInflation, NewAccInIsPruned}
            end,
            {false, false},
            EventHistoryDictR),
    NewPrunedEncSetL =
        case IsPruned of
            false ->
                PrunedEncSetL;
            true ->
                ordsets:add_element(GroupEncL, PrunedEncSetL)
        end,
    case IsInflation of
        false ->
            {false, orddict:new(), orddict:new(), ordsets:new()};
        true ->
            is_inflation_event_history_dict(
                ext_event_history_partial_order_group,
                T, GroupDecodeDictL,
                EventHistoryDictR, GroupDecodeDictR,
                NewPrunedEncSetL)
    end;
is_inflation_event_history_dict(
    ext_event_history_total_order,
    [], GroupDecodeDictL,
    EventHistoryDictR, GroupDecodeDictR,
    PrunedEncSetL) ->
    NewGroupDecodeDictR =
        orddict:fold(
            fun(_, {EventHistory, GroupEncSet}, AccInNewGroupDecodeDictR) ->
                update_group_decode_dict(EventHistory, GroupEncSet, AccInNewGroupDecodeDictR)
            end,
            GroupDecodeDictR,
            EventHistoryDictR),
    {true, GroupDecodeDictL, NewGroupDecodeDictR, PrunedEncSetL};
is_inflation_event_history_dict(
    ext_event_history_total_order,
    [H | T]=_EventHistoryDictL, GroupDecodeDictL,
    EventHistoryDictR, GroupDecodeDictR,
    PrunedEncSetL) ->
    {NodeIdL, {EventHistoryEncL, GroupEncSet}} = H,
    case orddict:find(NodeIdL, EventHistoryDictR) of
        error ->
            {false, orddict:new(), orddict:new(), ordsets:new()};
        {ok, {EventHistoryEncR, _GroupEncSetR}} ->
            case ext_type_event_history:is_related_to(EventHistoryEncL, EventHistoryEncR) of
                false ->
                    {false, orddict:new(), orddict:new(), ordsets:new()};
                true ->
                    NewGroupDecodeDictL =
                        update_group_decode_dict(EventHistoryEncL, GroupEncSet, GroupDecodeDictL),
                    NewPrunedEncSetL = ordsets:add_element(EventHistoryEncL, PrunedEncSetL),
                    is_inflation_event_history_dict(
                        ext_event_history_total_order,
                        T, NewGroupDecodeDictL,
                        EventHistoryDictR, GroupDecodeDictR,
                        NewPrunedEncSetL)
            end
    end;
is_inflation_event_history_dict(
    _EventHistoryType,
    [], GroupDecodeDictL,
    EventHistoryDictR, GroupDecodeDictR,
    PrunedEncSetL) ->
    NewGroupDecodeDictR =
        orddict:fold(
            fun(EventHistory, GroupEncSet, AccInNewGroupDecodeDictR) ->
                update_group_decode_dict(EventHistory, GroupEncSet, AccInNewGroupDecodeDictR)
            end,
            GroupDecodeDictR,
            EventHistoryDictR),
    {true, GroupDecodeDictL, NewGroupDecodeDictR, PrunedEncSetL};
is_inflation_event_history_dict(
    EventHistoryType,
    [H | T]=_EventHistoryDictL, GroupDecodeDictL,
    EventHistoryDictR, GroupDecodeDictR,
    PrunedEncSetL) ->
    {EventHistoryEncL, GroupEncSet} = H,
    {IsInflation, IsPruned} =
        is_inflation_event_history(
            EventHistoryType, EventHistoryEncL, orddict:fetch_keys(EventHistoryDictR)),
    NewPrunedEncSetL =
        case IsPruned of
            false ->
                PrunedEncSetL;
            true ->
                ordsets:add_element(EventHistoryEncL, PrunedEncSetL)
        end,
    case IsInflation of
        false ->
            {false, orddict:new(), orddict:new(), ordsets:new()};
        true ->
            NewGroupDecodeDictL =
                update_group_decode_dict(EventHistoryEncL, GroupEncSet, GroupDecodeDictL),
            is_inflation_event_history_dict(
                EventHistoryType,
                T, NewGroupDecodeDictL,
                EventHistoryDictR, GroupDecodeDictR,
                NewPrunedEncSetL)
    end.

%% @private
is_inflation_event_history(
    ext_event_history_partial_order_downward_closed, _EventHistoryEncL, []) ->
    {false, false};
is_inflation_event_history(
    ext_event_history_partial_order_downward_closed,
    EventHistoryEncL,
    [H | T]=_EventHistoryEncListR) ->
    case ext_type_event_history:is_related_to(EventHistoryEncL, H) of
        true ->
            {true, EventHistoryEncL /= H};
        false ->
            is_inflation_event_history(
                ext_event_history_partial_order_downward_closed, EventHistoryEncL, T)
    end;
is_inflation_event_history(
    ext_event_history_partial_order_independent, EventHistoryEncL, EventHistoryEncListR) ->
    case ordsets:is_element(EventHistoryEncL, ordsets:from_list(EventHistoryEncListR)) of
        true ->
            {true, false};
        false ->
            {false, false}
    end.

%% @private
is_inflation_data_store_enc(
    [], _GroupDecodeDictL, _EventHistoryAllDictsL,
    _DataStoreEncR, _GroupDecodeDictR, _EventRemovedR,
    _PrunedEncSetL) ->
    true;
is_inflation_data_store_enc(
    [H | T]=_DataStoreEncL, GroupDecodeDictL, EventHistoryAllDictsL,
    DataStoreEncR, GroupDecodeDictR, EventRemovedR,
    PrunedEncSetL) ->
    case is_inflation_cover_ps_store_enc(
        false,
        H, GroupDecodeDictL, EventHistoryAllDictsL,
        DataStoreEncR, GroupDecodeDictR, EventRemovedR,
        PrunedEncSetL) of
        false ->
            false;
        true ->
            is_inflation_data_store_enc(
                T, GroupDecodeDictL, EventHistoryAllDictsL,
                DataStoreEncR, GroupDecodeDictR, EventRemovedR,
                PrunedEncSetL)
    end.

%% @private
is_inflation_cover_ps_store_enc(
    Result,
    {_SubsetUnknownEncL, _PSEncL}, _GroupDecodeDictL, _EventHistoryAllDictsL,
    [], _GroupDecodeDictR, _EventRemovedR,
    _PrunedEncSetL) ->
    Result;
is_inflation_cover_ps_store_enc(
    Result,
    {SubsetUnknownEncL, PSEncL}, GroupDecodeDictL, EventHistoryAllDictsL,
    [H | T]=_DataStoreEncR, GroupDecodeDictR, EventRemovedR,
    PrunedEncSetL) ->
    {SubsetUnknownEncR, PSEncR} = H,
    case is_inflation_subset_unknown_enc(
        SubsetUnknownEncL, GroupDecodeDictL,
        SubsetUnknownEncR, GroupDecodeDictR,
        EventHistoryAllDictsL) of
        true ->
            case is_inflation_ps_store_enc(
                PSEncL, GroupDecodeDictL, PSEncR, GroupDecodeDictR,
                EventRemovedR, PrunedEncSetL) of
                false ->
                    false;
                true ->
                    is_inflation_cover_ps_store_enc(
                        true,
                        {SubsetUnknownEncL, PSEncL}, GroupDecodeDictL, EventHistoryAllDictsL,
                        T, GroupDecodeDictR, EventRemovedR,
                        PrunedEncSetL)
            end;
        false ->
            is_inflation_cover_ps_store_enc(
                Result,
                {SubsetUnknownEncL, PSEncL}, GroupDecodeDictL, EventHistoryAllDictsL,
                T, GroupDecodeDictR, EventRemovedR,
                PrunedEncSetL)
    end.

%% @private
is_inflation_subset_unknown_enc(
    SubsetUnknownEncL, GroupDecodeDictL,
    SubsetUnknownEncR, GroupDecodeDictR,
    EventHistoryAllDictsL) ->
    FilteredSubsetUnknownDecR =
        ordsets:fold(
            fun({ext_event_history_partial_order_group, _}=EHEncR,
                AccInFilteredSubsetUnknownDecR) ->
                EHDecR = orddict:fetch(EHEncR, GroupDecodeDictR),
                IsKnown =
                    orddict:fold(
                        fun(_EHEncL, EHDecL, AccInIsKnown) ->
                            AccInIsKnown orelse
                                ext_type_event_history:is_related_to(EHDecR, EHDecL)
                        end,
                        false,
                        GroupDecodeDictL),
                case IsKnown of
                    false ->
                        AccInFilteredSubsetUnknownDecR;
                    true ->
                        ordsets:add_element(EHDecR, AccInFilteredSubsetUnknownDecR)
                end;
            (
                {ext_event_history_total_order, {{NodeId, _, _}, _}}=EHEncR,
                AccInFilteredSubsetUnknownDecR) ->
                EventHistoryAllDict =
                    orddict:fetch(ext_event_history_total_order, EventHistoryAllDictsL),
                IsKnown =
                    case orddict:find(NodeId, EventHistoryAllDict) of
                        error ->
                            false;
                        {ok, {EventHistoryEncL, _GroupEncSetL}} ->
                            ext_type_event_history:is_related_to(EHEncR, EventHistoryEncL)
                    end,
                case IsKnown of
                    false ->
                        AccInFilteredSubsetUnknownDecR;
                    true ->
                        ordsets:add_element(EHEncR, AccInFilteredSubsetUnknownDecR)
                end;
            (
                {ext_event_history_partial_order_downward_closed, _}=EHEncR,
                AccInFilteredSubsetUnknownDecR) ->
                EventHistoryAllDict =
                    orddict:fetch(
                        ext_event_history_partial_order_downward_closed, EventHistoryAllDictsL),
                IsKnown =
                    lists:foldl(
                        fun(EHEncL, AccInIsKnown) ->
                            AccInIsKnown orelse
                                ext_type_event_history:is_related_to(EHEncR, EHEncL)
                        end,
                        false,
                        orddict:fetch_keys(EventHistoryAllDict)),
                case IsKnown of
                    false ->
                        AccInFilteredSubsetUnknownDecR;
                    true ->
                        ordsets:add_element(EHEncR, AccInFilteredSubsetUnknownDecR)
                end;
            (
                {ext_event_history_partial_order_independent, _}=EHEncR,
                AccInFilteredSubsetUnknownDecR) ->
                EventHistoryAllDict =
                    orddict:fetch(
                        ext_event_history_partial_order_independent, EventHistoryAllDictsL),
                IsKnown =
                    ordsets:is_element(
                        EHEncR, ordsets:from_list(orddict:fetch_keys(EventHistoryAllDict))),
                case IsKnown of
                    false ->
                        AccInFilteredSubsetUnknownDecR;
                    true ->
                        ordsets:add_element(EHEncR, AccInFilteredSubsetUnknownDecR)
                end
            end,
            ordsets:new(),
            SubsetUnknownEncR),
    SubsetUnknownDecL = ext_type_cover:decode_subset(SubsetUnknownEncL, GroupDecodeDictL),
    ordsets:is_subset(FilteredSubsetUnknownDecR, SubsetUnknownDecL).

%% @private
is_inflation_ps_store_enc(
    [], _GroupDecodeDictL, _PSEncR, _GroupDecodeDictR,
    _EventRemovedR, _PrunedEncSetL) ->
    true;
is_inflation_ps_store_enc(
    [H | T]=_PSEncL, GroupDecodeDictL, PSEncR, GroupDecodeDictR,
    EventRemovedR, PrunedEncSetL) ->
    {Elem, ProvenanceEncL} = H,
    ProvenanceEncR =
        case orddict:find(Elem, PSEncR) of
            error ->
                [];
            {ok, ProvenanceEncR0} ->
                ProvenanceEncR0
        end,
    PrunedProvenanceEncL =
        ordsets:fold(
            fun(DotEncL, AccInPrunedProvenanceEncL) ->
                case ordsets:intersection(DotEncL, PrunedEncSetL) of
                    [] ->
                        EventSetInDot = get_events_from_dot(DotEncL),
                        case ordsets:intersection(EventSetInDot, EventRemovedR) of
                            [] ->
                                ordsets:add_element(DotEncL, AccInPrunedProvenanceEncL);
                            _ ->
                                AccInPrunedProvenanceEncL
                        end;
                    _ ->
                        AccInPrunedProvenanceEncL
                end
            end,
            ordsets:new(),
            ProvenanceEncL),
    case ordsets:is_subset(PrunedProvenanceEncL, ProvenanceEncR) of
        false ->
            false;
        true ->
            is_inflation_ps_store_enc(
                T, GroupDecodeDictL, PSEncR, GroupDecodeDictR, EventRemovedR, PrunedEncSetL)
    end.

%% @private
get_events_from_dot(DotEnc) ->
    ordsets:fold(
        fun(EHEnc, AccInResult) ->
            case ext_type_event_history:get_event(EHEnc) of
                group_event ->
                    AccInResult;
                Event ->
                    ordsets:add_element(Event, AccInResult)
            end
        end,
        ordsets:new(),
        DotEnc).

%% @private
join_event_history_all_dicts_intermediate(EventHistoryAllDictsL, EventHistoryAllDictsR) ->
    {
        ResultEventHistoryAllDicts0,
        ResultPrunedEHSetL0, ResultPrunedEHSetR0,
        ResultGroupDecodeDictL0, ResultGroupDecodeDictR0,
        ResultNewEHSetForL0, ResultNewEHSetForR0} =
        lists:foldl(
            fun(EventHistoryType,
                {
                    AccInResultDicts,
                    AccInPrunedEHSetL, AccInPrunedEHSetR,
                    AccInGroupDecodeDictL, AccInGroupDecodeDictR,
                    AccInNewEHSetForL, AccInNewEHSetForR}) ->
                {
                    JoinedDict,
                    PrunedEHSetL, PrunedEHSetR,
                    GroupDecodeDictL, GroupDecodeDictR,
                    NewEHSetForL, NewEHSetForR} =
                    join_event_history_dict_intermediate(
                        EventHistoryType,
                        orddict:fetch(EventHistoryType, EventHistoryAllDictsL),
                        orddict:fetch(EventHistoryType, EventHistoryAllDictsR),
                        AccInPrunedEHSetL, AccInPrunedEHSetR,
                        AccInGroupDecodeDictL, AccInGroupDecodeDictR,
                        AccInNewEHSetForL, AccInNewEHSetForR),
                {
                    orddict:store(EventHistoryType, JoinedDict, AccInResultDicts),
                    PrunedEHSetL, PrunedEHSetR,
                    GroupDecodeDictL, GroupDecodeDictR,
                    NewEHSetForL, NewEHSetForR}
            end,
            {
                orddict:new(),
                ordsets:new(), ordsets:new(),
                orddict:new(), orddict:new(),
                ordsets:new(), ordsets:new()},
            [
                ext_event_history_total_order,
                ext_event_history_partial_order_downward_closed,
                ext_event_history_partial_order_independent]),
    {
        JoinedGroupDict,
        ResultPrunedEHSetL1, ResultPrunedEHSetR1,
        ResultGroupDecodeDictL1, ResultGroupDecodeDictR1,
        ResultGroupConvertDictL1, ResultGroupConvertDictR1,
        ResultNewEHSetForL1, ResultNewEHSetForR1} =
        join_event_history_group_dict_intermediate(
            ext_event_history_partial_order_group,
            orddict:fetch(ext_event_history_partial_order_group, EventHistoryAllDictsL),
            orddict:fetch(ext_event_history_partial_order_group, EventHistoryAllDictsR),
            ResultPrunedEHSetL0, ResultPrunedEHSetR0,
            ResultGroupDecodeDictL0, ResultGroupDecodeDictR0,
            ResultNewEHSetForL0, ResultNewEHSetForR0),
    ResultEventHistoryAllDicts1 =
        lists:foldl(
            fun(EventHistoryType, AccInResultEventHistoryAllDicts1) ->
                OldEventHistoryDict =
                    orddict:fetch(EventHistoryType, AccInResultEventHistoryAllDicts1),
                NewEventHistoryDict =
                    update_group_enc(
                        EventHistoryType, OldEventHistoryDict,
                        ResultGroupConvertDictL1, ResultGroupConvertDictR1),
                orddict:store(
                    EventHistoryType, NewEventHistoryDict, AccInResultEventHistoryAllDicts1)
            end,
            ResultEventHistoryAllDicts0,
            [
                ext_event_history_total_order,
                ext_event_history_partial_order_downward_closed,
                ext_event_history_partial_order_independent]),
    {
        orddict:store(
            ext_event_history_partial_order_group, JoinedGroupDict, ResultEventHistoryAllDicts1),
        ResultPrunedEHSetL1, ResultPrunedEHSetR1,
        ResultGroupDecodeDictL1, ResultGroupDecodeDictR1,
        ResultGroupConvertDictL1, ResultGroupConvertDictR1,
        ResultNewEHSetForL1, ResultNewEHSetForR1}.

%% @private
join_event_history_dict_intermediate(
    ext_event_history_total_order, EventHistoryDictL, EventHistoryDictR,
    PrunedEHSetL, PrunedEHSetR, GroupDecodeDictL, GroupDecodeDictR, NewEHSetForL, NewEHSetForR) ->
    {
        JoinDict0,
        NewPrunedEHSetL, NewPrunedEHSetR,
        NewGroupDecodeDictL, NewGroupDecodeDictR0,
        DupNodeIdSet,
        NewNewEHSetForR} =
        orddict:fold(
            fun(NodeIdL,
                {EventHistoryL, GroupEncSetL},
                {
                    AccInJoinDict0,
                    AccInNewPrunedEHSetL, AccInNewPrunedEHSetR,
                    AccInNewGroupDecodeDictL, AccInNewGroupDecodeDictR0,
                    AccInDupNodeIdSet,
                    AccInNewNewEHSetForR}) ->
                case orddict:find(NodeIdL, EventHistoryDictR) of
                    error ->
                        NewAccInNewGroupDecodeDictL =
                            update_group_decode_dict(
                                EventHistoryL, GroupEncSetL, AccInNewGroupDecodeDictL),
                        {
                            orddict:store(
                                NodeIdL, {EventHistoryL, {GroupEncSetL, []}}, AccInJoinDict0),
                            AccInNewPrunedEHSetL, AccInNewPrunedEHSetR,
                            NewAccInNewGroupDecodeDictL, AccInNewGroupDecodeDictR0,
                            AccInDupNodeIdSet,
                            ordsets:add_element(EventHistoryL, AccInNewNewEHSetForR)};
                    {ok, {EventHistoryR, GroupEncSetR}} ->
                        {
                            NewAccInJoinDict0,
                            NewAccInNewPrunedEHSetL, NewAccInNewPrunedEHSetR,
                            NewAccInNewNewEHSetForR} =
                            case ext_type_event_history:is_related_to(
                                EventHistoryL, EventHistoryR) of
                                true ->
                                    {
                                        orddict:store(
                                            NodeIdL,
                                            {EventHistoryR, {[], GroupEncSetR}},
                                            AccInJoinDict0),
                                        ordsets:add_element(EventHistoryL, AccInNewPrunedEHSetL),
                                        AccInNewPrunedEHSetR,
                                        AccInNewNewEHSetForR};
                                false ->
                                    {
                                        orddict:store(
                                            NodeIdL,
                                            {EventHistoryL, {GroupEncSetL, []}},
                                            AccInJoinDict0),
                                        AccInNewPrunedEHSetL,
                                        ordsets:add_element(EventHistoryR, AccInNewPrunedEHSetR),
                                        ordsets:add_element(EventHistoryL, AccInNewNewEHSetForR)}
                            end,
                        NewAccInNewGroupDecodeDictL =
                            update_group_decode_dict(
                                EventHistoryL, GroupEncSetL, AccInNewGroupDecodeDictL),
                        NewAccInNewGroupDecodeDictR0 =
                            update_group_decode_dict(
                                EventHistoryR, GroupEncSetR, AccInNewGroupDecodeDictR0),
                        {
                            NewAccInJoinDict0,
                            NewAccInNewPrunedEHSetL, NewAccInNewPrunedEHSetR,
                            NewAccInNewGroupDecodeDictL, NewAccInNewGroupDecodeDictR0,
                            ordsets:add_element(NodeIdL, AccInDupNodeIdSet),
                            NewAccInNewNewEHSetForR}
                end
            end,
            {
                orddict:new(),
                PrunedEHSetL, PrunedEHSetR,
                GroupDecodeDictL, GroupDecodeDictR,
                ordsets:new(),
                NewEHSetForR},
            EventHistoryDictL),
    {JoinDict1, NewGroupDecodeDictR1, NewNewEHSetForL} =
        orddict:fold(
            fun(NodeIdR,
                {EventHistoryR, GroupEncSetR},
                {AccInJoinDict1, AccInNewGroupDecodeDictR1, AccInNewNewEHSetForL}) ->
                case ordsets:is_element(NodeIdR, DupNodeIdSet) of
                    true ->
                        {AccInJoinDict1, AccInNewGroupDecodeDictR1, AccInNewNewEHSetForL};
                    false ->
                        NewAccInJoinDict1 =
                            orddict:store(
                                NodeIdR, {EventHistoryR, {[], GroupEncSetR}}, AccInJoinDict1),
                        NewAccInNewGroupDecodeDictR1 =
                            update_group_decode_dict(
                                EventHistoryR, GroupEncSetR, AccInNewGroupDecodeDictR1),
                        {
                            NewAccInJoinDict1,
                            NewAccInNewGroupDecodeDictR1,
                            ordsets:add_element(EventHistoryR, AccInNewNewEHSetForL)}
                end
            end,
            {JoinDict0, NewGroupDecodeDictR0, NewEHSetForL},
            EventHistoryDictR),
    {
        JoinDict1,
        NewPrunedEHSetL, NewPrunedEHSetR,
        NewGroupDecodeDictL, NewGroupDecodeDictR1,
        NewNewEHSetForL, NewNewEHSetForR};
join_event_history_dict_intermediate(
    ext_event_history_partial_order_downward_closed, _EventHistoryDictL, _EventHistoryDictR,
    PrunedEHSetL, PrunedEHSetR, GroupDecodeDictL, GroupDecodeDictR, NewEHSetForL, NewEHSetForR) ->
    {
        orddict:new(),
        PrunedEHSetL, PrunedEHSetR,
        GroupDecodeDictL, GroupDecodeDictR,
        NewEHSetForL, NewEHSetForR};
join_event_history_dict_intermediate(
    ext_event_history_partial_order_independent, EventHistoryDictL, EventHistoryDictR,
    PrunedEHSetL, PrunedEHSetR, GroupDecodeDictL, GroupDecodeDictR, NewEHSetForL, NewEHSetForR) ->
    {JoinDict0, NewGroupDecodeDictL, NewNewEHSetForR} =
        orddict:fold(
            fun(EventHistoryL,
                GroupEncSetL,
                {AccInJoinDict0, AccInNewGroupDecodeDictL, AccInNewNewEHSetForR}) ->
                NewAccInNewGroupDecodeDictL =
                    update_group_decode_dict(
                        EventHistoryL, GroupEncSetL, AccInNewGroupDecodeDictL),
                NewAccInNewNewEHSetForR =
                    case orddict:find(EventHistoryL, EventHistoryDictR) of
                        error ->
                            ordsets:add_element(EventHistoryL, AccInNewNewEHSetForR);
                        _ ->
                            AccInNewNewEHSetForR
                    end,
                {
                    orddict:store(EventHistoryL, {GroupEncSetL, []}, AccInJoinDict0),
                    NewAccInNewGroupDecodeDictL,
                    NewAccInNewNewEHSetForR}
            end,
            {orddict:new(), GroupDecodeDictL, NewEHSetForR},
            EventHistoryDictL),
    {JoinDict1, NewGroupDecodeDictR, NewNewEHSetForL} =
        orddict:fold(
            fun(EventHistoryR,
                GroupEncSetR,
                {AccInJoinDict1, AccInNewGroupDecodeDictR, AccInNewNewEHSetForL}) ->
                NewAccInNewGroupDecodeDictR =
                    update_group_decode_dict(
                        EventHistoryR, GroupEncSetR, AccInNewGroupDecodeDictR),
                {NewAccInJoinDict1, NewAccInNewNewEHSetForL} =
                    case orddict:find(EventHistoryR, EventHistoryDictL) of
                        error ->
                            {
                                orddict:store(EventHistoryR, {[], GroupEncSetR}, AccInJoinDict1),
                                ordsets:add_element(EventHistoryR, AccInNewNewEHSetForL)};
                        {ok, GroupEncSetL} ->
                            {
                                orddict:store(
                                    EventHistoryR, {GroupEncSetL, GroupEncSetR}, AccInJoinDict1),
                                AccInNewNewEHSetForL}
                    end,
                {NewAccInJoinDict1, NewAccInNewGroupDecodeDictR, NewAccInNewNewEHSetForL}
            end,
            {JoinDict0, GroupDecodeDictR, NewEHSetForL},
            EventHistoryDictR),
    {
        JoinDict1,
        PrunedEHSetL, PrunedEHSetR,
        NewGroupDecodeDictL, NewGroupDecodeDictR,
        NewNewEHSetForL, NewNewEHSetForR}.

%% @private
join_event_history_group_dict_intermediate(
    ext_event_history_partial_order_group, _EventHistoryDictL, _EventHistoryDictR,
    PrunedEHSetL, PrunedEHSetR, GroupDecodeDictL, GroupDecodeDictR, NewEHSetForL, NewEHSetForR) ->
    JoinDecDict0 =
        orddict:fold(
            fun(EHGroupEncL,
                {ext_event_history_partial_order_group, {NodeIdL, EHSetL}}=_EHGroupDecL,
                AccInJoinDecDict0) ->
                orddict:update(
                    NodeIdL,
                    fun(OldSet) ->
                        ordsets:add_element({EHSetL, {EHGroupEncL, undefined}}, OldSet)
                    end,
                    ordsets:add_element({EHSetL, {EHGroupEncL, undefined}}, ordsets:new()),
                    AccInJoinDecDict0)
            end,
            orddict:new(),
            GroupDecodeDictL),
    {JoinDecDict1, NewPrunedEHSetL, NewPrunedEHSetR} =
        orddict:fold(
            fun(EHGroupEncR,
                {ext_event_history_partial_order_group, {NodeIdR, EHSetR}}=_EHGroupDecR,
                {AccInJoinDecDict1, AccInNewPrunedEHSetL, AccInNewPrunedEHSetR}) ->
                case orddict:find(NodeIdR, AccInJoinDecDict1) of
                    error ->
                        {
                            orddict:store(
                                NodeIdR,
                                ordsets:add_element(
                                    {EHSetR, {undefined, EHGroupEncR}}, ordsets:new()),
                                AccInJoinDecDict1),
                            AccInNewPrunedEHSetL, AccInNewPrunedEHSetR};
                    {ok, DecInfoSet} ->
                        {NewDecInfoSet, NewAccInNewPrunedEHSetL, NewAccInNewPrunedEHSetR} =
                            update_dec_info_set(
                                DecInfoSet, AccInNewPrunedEHSetL, AccInNewPrunedEHSetR,
                                EHSetR, EHGroupEncR, ordsets:new(), false),
                        {
                            orddict:store(NodeIdR, NewDecInfoSet, AccInJoinDecDict1),
                            NewAccInNewPrunedEHSetL, NewAccInNewPrunedEHSetR}
                end
            end,
            {JoinDecDict0, PrunedEHSetL, PrunedEHSetR},
            GroupDecodeDictR),
    {JoinEncDict, GroupConvertDictL, GroupConvertDictR, NewNewEHSetForL, NewNewEHSetForR} =
        orddict:fold(
            fun(NodeId, DecInfoSet,
                {
                    AccInJoinEncDict,
                    AccInGroupConvertDictL, AccInGroupConvertDictR,
                    AccInNewNewEHSetForL, AccInNewNewEHSetForR}) ->
                {
                    _Index,
                    NewAccInJoinEncDict,
                    NewAccInGroupConvertDictL, NewAccInGroupConvertDictR,
                    NewAccInNewNewEHSetForL, NewAccInNewNewEHSetForR} =
                    ordsets:fold(
                        fun({_EHSet, {EHGroupEncL, EHGroupEncR}}=_DecInfo,
                            {
                                AccInIndex,
                                AccInNewAccInJoinEncDict,
                                AccInNewAccInGroupConvertDictL, AccInNewAccInGroupConvertDictR,
                                AccInNewAccInNewNewEHSetForL, AccInNewAccInNewNewEHSetForR}) ->
                            NewGroupEH =
                                {ext_event_history_partial_order_group, {NodeId, AccInIndex}},
                            NewAccInNewAccInJoinEncDict =
                                orddict:store(NewGroupEH, ordsets:new(), AccInNewAccInJoinEncDict),
                            {NewAccInNewAccInGroupConvertDictL, NewAccInNewAccInNewNewEHSetForL} =
                                case EHGroupEncL of
                                    undefined ->
                                        {
                                            AccInNewAccInGroupConvertDictL,
                                            ordsets:add_element(
                                                EHGroupEncR, AccInNewAccInNewNewEHSetForL)};
                                    _ ->
                                        {
                                            orddict:store(
                                                EHGroupEncL,
                                                NewGroupEH,
                                                AccInNewAccInGroupConvertDictL),
                                            AccInNewAccInNewNewEHSetForL}
                                end,
                            {NewAccInNewAccInGroupConvertDictR, NewAccInNewAccInNewNewEHSetForR} =
                                case EHGroupEncR of
                                    undefined ->
                                        {
                                            AccInNewAccInGroupConvertDictR,
                                            ordsets:add_element(
                                                EHGroupEncL,
                                                AccInNewAccInNewNewEHSetForR)};
                                    _ ->
                                        {
                                            orddict:store(
                                                EHGroupEncR,
                                                NewGroupEH,
                                                AccInNewAccInGroupConvertDictR),
                                            AccInNewAccInNewNewEHSetForR}
                                end,
                            {
                                AccInIndex + 1,
                                NewAccInNewAccInJoinEncDict,
                                NewAccInNewAccInGroupConvertDictL,
                                NewAccInNewAccInGroupConvertDictR,
                                NewAccInNewAccInNewNewEHSetForL, NewAccInNewAccInNewNewEHSetForR}
                        end,
                        {
                            1,
                            AccInJoinEncDict,
                            AccInGroupConvertDictL, AccInGroupConvertDictR,
                            AccInNewNewEHSetForL, AccInNewNewEHSetForR},
                        DecInfoSet),
                {
                    NewAccInJoinEncDict,
                    NewAccInGroupConvertDictL, NewAccInGroupConvertDictR,
                    NewAccInNewNewEHSetForL, NewAccInNewNewEHSetForR}
            end,
            {orddict:new(), orddict:new(), orddict:new(), NewEHSetForL, NewEHSetForR},
            JoinDecDict1),
    {
        JoinEncDict,
        NewPrunedEHSetL, NewPrunedEHSetR,
        GroupDecodeDictL, GroupDecodeDictR,
        GroupConvertDictL, GroupConvertDictR,
        NewNewEHSetForL, NewNewEHSetForR}.

%% @private
update_dec_info_set([], PrunedEHSetL, PrunedEHSetR, _EHSetR, _EHGroupEncR, NewDecInfoSet, true) ->
    {NewDecInfoSet, PrunedEHSetL, PrunedEHSetR};
update_dec_info_set([], PrunedEHSetL, PrunedEHSetR, EHSetR, EHGroupEncR, NewDecInfoSet, false) ->
    {
        ordsets:add_element({EHSetR, {undefined, EHGroupEncR}}, NewDecInfoSet),
        PrunedEHSetL, PrunedEHSetR};
update_dec_info_set(
    [H | T]=_DecInfoSet,
    PrunedEHSetL, PrunedEHSetR,
    EHSetR, EHGroupEncR,
    NewDecInfoSet, true) ->
    UpdatedNewDecInfoSet = ordsets:add_element(H, NewDecInfoSet),
    update_dec_info_set(
        T, PrunedEHSetL, PrunedEHSetR, EHSetR, EHGroupEncR, UpdatedNewDecInfoSet, true);
update_dec_info_set(
    [H | T]=_DecInfoSet,
    PrunedEHSetL, PrunedEHSetR,
    EHSetR, EHGroupEncR,
    NewDecInfoSet, false) ->
    {EHSetInfo, {EHGroupEncLInfo, _EHGroupEncRInfo}} = H,
    case EHSetInfo of
        EHSetR ->
            UpdatedNewDecInfoSet =
                ordsets:add_element({EHSetR, {EHGroupEncLInfo, EHGroupEncR}}, NewDecInfoSet),
            update_dec_info_set(
                T, PrunedEHSetL, PrunedEHSetR, EHSetR, EHGroupEncR, UpdatedNewDecInfoSet, true);
        _ ->
            case ordsets:is_subset(EHSetR, EHSetInfo) of
                true ->
                    UpdatedNewDecInfoSet = ordsets:add_element(H, NewDecInfoSet),
                    update_dec_info_set(
                        T,
                        PrunedEHSetL, ordsets:add_element(EHGroupEncR, PrunedEHSetR),
                        EHSetR, EHGroupEncR,
                        UpdatedNewDecInfoSet, true);
                false ->
                    case ordsets:is_subset(EHSetInfo, EHSetR) of
                        true ->
                            update_dec_info_set(
                                T,
                                ordsets:add_element(EHGroupEncLInfo, PrunedEHSetL), PrunedEHSetR,
                                EHSetR, EHGroupEncR,
                                NewDecInfoSet, false);
                        false ->
                            UpdatedNewDecInfoSet = ordsets:add_element(H, NewDecInfoSet),
                            update_dec_info_set(
                                T,
                                PrunedEHSetL, PrunedEHSetR,
                                EHSetR, EHGroupEncR,
                                UpdatedNewDecInfoSet, false)
                    end
            end
    end.

%% @private
join_data_store_enc(
    DataStoreEncL, DataStoreEncR,
    JoinedEventRemoved, PrunedEHSetL, PrunedEHSetR,
    GroupDecodeDictL, GroupDecodeDictR,
    GroupConvertDictL, GroupConvertDictR,
    NewEHSetForL, NewEHSetForR,
    EventHistoryAllDictsL, EventHistoryAllDictsR) ->
    UpdateNewEHSetForL =
        ordsets:fold(
            fun({ext_event_history_partial_order_group, _}=EHEncR, AccInUpdateNewEHSetForL) ->
                ordsets:add_element(
                    orddict:fetch(EHEncR, GroupConvertDictR), AccInUpdateNewEHSetForL);
            (EHEncR, AccInUpdateNewEHSetForL) ->
                ordsets:add_element(EHEncR, AccInUpdateNewEHSetForL)
            end,
            ordsets:new(),
            NewEHSetForL),
    UpdateNewEHSetForR =
        ordsets:fold(
            fun({ext_event_history_partial_order_group, _}=EHEncL, AccInUpdateNewEHSetForR) ->
                ordsets:add_element(
                    orddict:fetch(EHEncL, GroupConvertDictL), AccInUpdateNewEHSetForR);
            (EHEncL, AccInUpdateNewEHSetForR) ->
                ordsets:add_element(EHEncL, AccInUpdateNewEHSetForR)
            end,
            ordsets:new(),
            NewEHSetForR),
    join_data_store_enc_internal(
        DataStoreEncL, DataStoreEncR,
        orddict:new(), orddict:new(), orddict:new(),
        orddict:new(), orddict:new(), orddict:new(),
        JoinedEventRemoved, PrunedEHSetL, PrunedEHSetR,
        GroupDecodeDictL, GroupDecodeDictR,
        GroupConvertDictL, GroupConvertDictR,
        UpdateNewEHSetForL, UpdateNewEHSetForR,
        EventHistoryAllDictsL, EventHistoryAllDictsR).

%% @private
join_data_store_enc_internal(
    [], [],
    HighL, _LowL, NoneL,
    HighR, _LowR, NoneR,
    _JoinedEventRemoved, _PrunedEHSetL, _PrunedEHSetR,
    _GroupDecodeDictL, _GroupDecodeDictR,
    _GroupConvertDictL, _GroupConvertDictR,
    NewEHSetForL, NewEHSetForR,
    _EventHistoryAllDictsL, _EventHistoryAllDictsR) ->
    JoinedDataStoreEnc0 =
        lists:foldl(
            fun(DataStoreInListL, AccInJoinedDataStoreEnc0) ->
                orddict:fold(
                    fun(SubsetUnknownEncL, ProvenanceStoreEncL, AccInNewJoinedDataStoreEnc0) ->
                        orddict:store(
                            ordsets:union(SubsetUnknownEncL, NewEHSetForL),
                            ProvenanceStoreEncL,
                            AccInNewJoinedDataStoreEnc0)
                    end,
                    AccInJoinedDataStoreEnc0,
                    DataStoreInListL)
            end,
            orddict:new(),
            [HighL, NoneL]),
    lists:foldl(
        fun(DataStoreInListR, AccInResult) ->
            orddict:fold(
                fun(SubsetUnknownEncR, ProvenanceStoreEncR, AccInNewResult) ->
                    orddict:store(
                        ordsets:union(SubsetUnknownEncR, NewEHSetForR),
                        ProvenanceStoreEncR,
                        AccInNewResult)
                end,
                AccInResult,
                DataStoreInListR)
        end,
        JoinedDataStoreEnc0,
        [HighR, NoneR]);
join_data_store_enc_internal(
    [], DataStoreEncR,
    HighL, LowL, NoneL,
    HighR, LowR, NoneR,
    JoinedEventRemoved, PrunedEHSetL, PrunedEHSetR,
    GroupDecodeDictL, GroupDecodeDictR,
    GroupConvertDictL, GroupConvertDictR,
    NewEHSetForL, NewEHSetForR,
    EventHistoryAllDictsL, EventHistoryAllDictsR) ->
    NewNoneR =
        orddict:fold(
            fun(SubsetUnknownEncR, ProvenanceStoreEncR, AccInNewNoneR) ->
                orddict:store(SubsetUnknownEncR, ProvenanceStoreEncR, AccInNewNoneR)
            end,
            NoneR,
            DataStoreEncR),
    join_data_store_enc_internal(
        [], [],
        HighL, LowL, NoneL,
        HighR, LowR, NewNoneR,
        JoinedEventRemoved, PrunedEHSetL, PrunedEHSetR,
        GroupDecodeDictL, GroupDecodeDictR,
        GroupConvertDictL, GroupConvertDictR,
        NewEHSetForL, NewEHSetForR,
        EventHistoryAllDictsL, EventHistoryAllDictsR);
join_data_store_enc_internal(
    [HL | TL]=_DataStoreEncL, DataStoreEncR,
    HighL, LowL, NoneL,
    HighR, LowR, NoneR,
    JoinedEventRemoved, PrunedEHSetL, PrunedEHSetR,
    GroupDecodeDictL, GroupDecodeDictR,
    GroupConvertDictL, GroupConvertDictR,
    NewEHSetForL, NewEHSetForR,
    EventHistoryAllDictsL, EventHistoryAllDictsR) ->
    NewHighR0 =
        update_high(
            HL, HighR, GroupDecodeDictL, GroupDecodeDictR, EventHistoryAllDictsL,
            JoinedEventRemoved, PrunedEHSetL, PrunedEHSetR,
            GroupConvertDictL, GroupConvertDictR),
    case NewHighR0 of
        HighR ->
            NewHL0 =
                merge_low(
                    HL, LowR, GroupDecodeDictL, GroupDecodeDictR, EventHistoryAllDictsR,
                    JoinedEventRemoved, PrunedEHSetL, PrunedEHSetR,
                    GroupConvertDictL, GroupConvertDictR),
            case NewHL0 of
                HL ->
                    {
                        NewDataStoreEncR,
                        NewHighL, NewLowL, NewNoneL,
                        NewHighR, NewLowR} =
                        update_low_high(
                            HL,
                            DataStoreEncR,
                            HighL, LowL, NoneL,
                            HighR, LowR,
                            GroupDecodeDictL, GroupDecodeDictR,
                            EventHistoryAllDictsL, EventHistoryAllDictsR,
                            JoinedEventRemoved, PrunedEHSetL, PrunedEHSetR,
                            GroupConvertDictL, GroupConvertDictR),
                    join_data_store_enc_internal(
                        TL, NewDataStoreEncR,
                        NewHighL, NewLowL, NewNoneL,
                        NewHighR, NewLowR, NoneR,
                        JoinedEventRemoved, PrunedEHSetL, PrunedEHSetR,
                        GroupDecodeDictL, GroupDecodeDictR,
                        GroupConvertDictL, GroupConvertDictR,
                        NewEHSetForL, NewEHSetForR,
                        EventHistoryAllDictsL, EventHistoryAllDictsR);
                _ ->
                    {NewHL1, NewLowR, NewDataStoreEncR} =
                        merge_low_from_ds(
                            NewHL0, LowR, DataStoreEncR,
                            GroupDecodeDictL, GroupDecodeDictR, EventHistoryAllDictsR,
                            JoinedEventRemoved, PrunedEHSetL, PrunedEHSetR,
                            GroupConvertDictL, GroupConvertDictR),
                    {KeyNewHL1, ValueNewHL1} = NewHL1,
                    NewHighL = orddict:store(KeyNewHL1, ValueNewHL1, HighL),
                    join_data_store_enc_internal(
                        TL, NewDataStoreEncR,
                        NewHighL, LowL, NoneL,
                        HighR, NewLowR, NoneR,
                        JoinedEventRemoved, PrunedEHSetL, PrunedEHSetR,
                        GroupDecodeDictL, GroupDecodeDictR,
                        GroupConvertDictL, GroupConvertDictR,
                        NewEHSetForL, NewEHSetForR,
                        EventHistoryAllDictsL, EventHistoryAllDictsR)
            end;
        _ ->
            {KeyHL, ValueHL} = HL,
            NewLowL = orddict:store(KeyHL, ValueHL, LowL),
            {NewHighR1, NewDataStoreEncR} =
                update_high_from_ds(
                    HL, NewHighR0, DataStoreEncR,
                    GroupDecodeDictL, GroupDecodeDictR, EventHistoryAllDictsL,
                    JoinedEventRemoved, PrunedEHSetL, PrunedEHSetR,
                    GroupConvertDictL, GroupConvertDictR),
            join_data_store_enc_internal(
                TL, NewDataStoreEncR,
                HighL, NewLowL, NoneL,
                NewHighR1, LowR, NoneR,
                JoinedEventRemoved, PrunedEHSetL, PrunedEHSetR,
                GroupDecodeDictL, GroupDecodeDictR,
                GroupConvertDictL, GroupConvertDictR,
                NewEHSetForL, NewEHSetForR,
                EventHistoryAllDictsL, EventHistoryAllDictsR)
    end.

%% @private
update_high(
    {SubsetUnknownEncL, ProvenanceStoreEncL}=_CurrentL, HighDictR,
    GroupDecodeDictL, GroupDecodeDictR, EventHistoryAllDictsL,
    JoinedEventRemoved, PrunedEHSetL, PrunedEHSetR,
    GroupConvertDictL, GroupConvertDictR) ->
    orddict:fold(
        fun(SubsetUnknownEncHighR, ProvenanceStoreEncHighR, AccInResult) ->
            case is_inflation_subset_unknown_enc(
                SubsetUnknownEncL, GroupDecodeDictL,
                SubsetUnknownEncHighR, GroupDecodeDictR,
                EventHistoryAllDictsL) of
                false ->
                    orddict:store(SubsetUnknownEncHighR, ProvenanceStoreEncHighR, AccInResult);
                true ->
                    NewProvenanceStoreEncHighR =
                        join_provenance_store_enc(
                            ProvenanceStoreEncL, ProvenanceStoreEncHighR,
                            JoinedEventRemoved, PrunedEHSetL, PrunedEHSetR,
                            GroupConvertDictL, GroupConvertDictR),
                    orddict:store(SubsetUnknownEncHighR, NewProvenanceStoreEncHighR, AccInResult)
            end
        end,
        orddict:new(),
        HighDictR).

%% @private
update_high_from_ds(
    {SubsetUnknownEncL, ProvenanceStoreEncL}=_CurrentL, HighDictR, DataStoreEncR,
    GroupDecodeDictL, GroupDecodeDictR, EventHistoryAllDictsL,
    JoinedEventRemoved, PrunedEHSetL, PrunedEHSetR,
    GroupConvertDictL, GroupConvertDictR) ->
    orddict:fold(
        fun(SubsetUnknownEncR, ProvenanceStoreEncR,
            {AccInNewHighDictR, AccInNewDataStoreEncR}) ->
            case is_inflation_subset_unknown_enc(
                SubsetUnknownEncL, GroupDecodeDictL,
                SubsetUnknownEncR, GroupDecodeDictR,
                EventHistoryAllDictsL) of
                false ->
                    {
                        AccInNewHighDictR,
                        orddict:store(
                            SubsetUnknownEncR, ProvenanceStoreEncR, AccInNewDataStoreEncR)};
                true ->
                    NewProvenanceStoreEncR =
                        join_provenance_store_enc(
                            ProvenanceStoreEncL, ProvenanceStoreEncR,
                            JoinedEventRemoved, PrunedEHSetL, PrunedEHSetR,
                            GroupConvertDictL, GroupConvertDictR),
                    {
                        orddict:store(
                            SubsetUnknownEncR, NewProvenanceStoreEncR, AccInNewHighDictR),
                        AccInNewDataStoreEncR}
            end
        end,
        {HighDictR, orddict:new()},
        DataStoreEncR).

%% @private
merge_low(
    {SubsetUnknownEncL, ProvenanceStoreEncL}=_CurrentL, LowDictR,
    GroupDecodeDictL, GroupDecodeDictR, EventHistoryAllDictsR,
    JoinedEventRemoved, PrunedEHSetL, PrunedEHSetR,
    GroupConvertDictL, GroupConvertDictR) ->
    NewProvenanceStoreEncL =
        orddict:fold(
            fun(SubsetUnknownEncR, ProvenanceStoreEncR, AccInNewProvenanceStoreEncL) ->
                case is_inflation_subset_unknown_enc(
                    SubsetUnknownEncR, GroupDecodeDictR,
                    SubsetUnknownEncL, GroupDecodeDictL,
                    EventHistoryAllDictsR) of
                    false ->
                        AccInNewProvenanceStoreEncL;
                    true ->
                        join_provenance_store_enc(
                            AccInNewProvenanceStoreEncL, ProvenanceStoreEncR,
                            JoinedEventRemoved, PrunedEHSetL, PrunedEHSetR,
                            GroupConvertDictL, GroupConvertDictR)
                end
            end,
            ProvenanceStoreEncL,
            LowDictR),
    {SubsetUnknownEncL, NewProvenanceStoreEncL}.

%% @private
merge_low_from_ds(
    {SubsetUnknownEncL, ProvenanceStoreEncL}=_CurrentL, LowDictR, DataStoreEncR,
    GroupDecodeDictL, GroupDecodeDictR, EventHistoryAllDictsR,
    JoinedEventRemoved, PrunedEHSetL, PrunedEHSetR,
    GroupConvertDictL, GroupConvertDictR) ->
    {NewProvenanceStoreEncL, NewLowDictR, NewDataStoreEncR} =
        orddict:fold(
            fun(SubsetUnknownEncR, ProvenanceStoreEncR,
                {AccInNewProvenanceStoreEncL, AccInNewLowDictR, AccInNewDataStoreEncR}) ->
                case is_inflation_subset_unknown_enc(
                    SubsetUnknownEncR, GroupDecodeDictR,
                    SubsetUnknownEncL, GroupDecodeDictL,
                    EventHistoryAllDictsR) of
                    false ->
                        {
                            AccInNewProvenanceStoreEncL,
                            AccInNewLowDictR,
                            orddict:store(
                                SubsetUnknownEncR,
                                ProvenanceStoreEncR,
                                AccInNewDataStoreEncR)};
                    true ->
                        {
                            join_provenance_store_enc(
                                AccInNewProvenanceStoreEncL, ProvenanceStoreEncR,
                                JoinedEventRemoved, PrunedEHSetL, PrunedEHSetR,
                                GroupConvertDictL, GroupConvertDictR),
                            orddict:store(
                                SubsetUnknownEncR, ProvenanceStoreEncR, AccInNewLowDictR),
                            AccInNewDataStoreEncR}
                end
            end,
            {ProvenanceStoreEncL, LowDictR, orddict:new()},
            DataStoreEncR),
    {{SubsetUnknownEncL, NewProvenanceStoreEncL}, NewLowDictR, NewDataStoreEncR}.

%% @private
update_low_high(
    CurrentL, DataStoreEncR, HighL, LowL, NoneL, HighR, LowR,
    GroupDecodeDictL, GroupDecodeDictR, EventHistoryAllDictsL, EventHistoryAllDictsR,
    JoinedEventRemoved, PrunedEHSetL, PrunedEHSetR,
    GroupConvertDictL, GroupConvertDictR) ->
    update_low_high_internal(
        CurrentL, DataStoreEncR, HighL, LowL, NoneL, HighR, LowR, orddict:new(),
        GroupDecodeDictL, GroupDecodeDictR, EventHistoryAllDictsL, EventHistoryAllDictsR,
        JoinedEventRemoved, PrunedEHSetL, PrunedEHSetR,
        GroupConvertDictL, GroupConvertDictR).

%% @private
update_low_high_internal(
    {SubsetUnknownEncL, ProvenanceStoreEncL}=_CurrentL, [],
    HighL, LowL, NoneL,
    HighR, LowR,
    NewDataStoreEncR,
    _GroupDecodeDictL, _GroupDecodeDictR, _EventHistoryAllDictsL, _EventHistoryAllDictsR,
    _JoinedEventRemoved, _PrunedEHSetL, _PrunedEHSetR,
    _GroupConvertDictL, _GroupConvertDictR) ->
    {
        NewDataStoreEncR,
        HighL, LowL, orddict:store(SubsetUnknownEncL, ProvenanceStoreEncL, NoneL),
        HighR, LowR};
update_low_high_internal(
    {SubsetUnknownEncL, ProvenanceStoreEncL}=CurrentL, [H | T]=_DataStoreEncR,
    HighL, LowL, NoneL,
    HighR, LowR,
    NewDataStoreEncR,
    GroupDecodeDictL, GroupDecodeDictR, EventHistoryAllDictsL, EventHistoryAllDictsR,
    JoinedEventRemoved, PrunedEHSetL, PrunedEHSetR,
    GroupConvertDictL, GroupConvertDictR) ->
    {SubsetUnknownEncR, ProvenanceStoreEncR} = H,
    case is_inflation_subset_unknown_enc(
        SubsetUnknownEncL, GroupDecodeDictL,
        SubsetUnknownEncR, GroupDecodeDictR,
        EventHistoryAllDictsL) of
        false ->
            case is_inflation_subset_unknown_enc(
                SubsetUnknownEncR, GroupDecodeDictR,
                SubsetUnknownEncL, GroupDecodeDictL,
                EventHistoryAllDictsR) of
                false ->
                    NewDataStoreEncR1 =
                        orddict:store(SubsetUnknownEncR, ProvenanceStoreEncR, NewDataStoreEncR),
                    update_low_high_internal(
                        CurrentL, T,
                        HighL, LowL, NoneL,
                        HighR, LowR,
                        NewDataStoreEncR1,
                        GroupDecodeDictL, GroupDecodeDictR,
                        EventHistoryAllDictsL, EventHistoryAllDictsR,
                        JoinedEventRemoved, PrunedEHSetL, PrunedEHSetR,
                        GroupConvertDictL, GroupConvertDictR);
                true ->
                    NewProvenanceStoreEnc =
                        join_provenance_store_enc(
                            ProvenanceStoreEncL, ProvenanceStoreEncR,
                            JoinedEventRemoved, PrunedEHSetL, PrunedEHSetR,
                            GroupConvertDictL, GroupConvertDictR),
                    NewCurrentL = {SubsetUnknownEncL, NewProvenanceStoreEnc},
                    NewLowR = orddict:store(SubsetUnknownEncR, ProvenanceStoreEncR, LowR),
                    {NewCurrentL1, NewLowR1, NewDataStoreEncR1} =
                        merge_low_from_ds(
                            NewCurrentL, NewLowR, T,
                            GroupDecodeDictL, GroupDecodeDictR, EventHistoryAllDictsR,
                            JoinedEventRemoved, PrunedEHSetL, PrunedEHSetR,
                            GroupConvertDictL, GroupConvertDictR),
                    {KeyNewHL1, ValueNewHL1} = NewCurrentL1,
                    NewHighL = orddict:store(KeyNewHL1, ValueNewHL1, HighL),
                    {NewDataStoreEncR1, NewHighL, LowL, NoneL, HighR, NewLowR1}
            end;
        true ->
            NewProvenanceStoreEncR =
                join_provenance_store_enc(
                    ProvenanceStoreEncL, ProvenanceStoreEncR,
                    JoinedEventRemoved, PrunedEHSetL, PrunedEHSetR,
                    GroupConvertDictL, GroupConvertDictR),
            NewHighR = orddict:store(SubsetUnknownEncR, NewProvenanceStoreEncR, HighR),
            {NewHighR1, NewDataStoreEncR1} =
                update_high_from_ds(
                    CurrentL, NewHighR, T,
                    GroupDecodeDictL, GroupDecodeDictR, EventHistoryAllDictsL,
                    JoinedEventRemoved, PrunedEHSetL, PrunedEHSetR,
                    GroupConvertDictL, GroupConvertDictR),
            {NewDataStoreEncR1, HighL, LowL, NoneL, NewHighR1, LowR}
    end.

%% @private
join_provenance_store_enc(
    ProvenanceStoreEncL, ProvenanceStoreEncR,
    JoinedEventRemoved, PrunedEHSetL, PrunedEHSetR,
    GroupConvertDictL, GroupConvertDictR) ->
    Result0 =
        orddict:fold(
            fun(ElemL, ProvenanceEncL, AccInResult0) ->
                NewProvenanceEncL =
                    prune_provenance(
                        ProvenanceEncL, JoinedEventRemoved, PrunedEHSetL, GroupConvertDictL),
                case NewProvenanceEncL of
                    [] ->
                        AccInResult0;
                    _ ->
                        orddict:store(ElemL, NewProvenanceEncL, AccInResult0)
                end
            end,
            orddict:new(),
            ProvenanceStoreEncL),
    orddict:fold(
        fun(ElemR, ProvenanceEncR, AccInResult1) ->
            NewProvenanceEncR =
                prune_provenance(
                    ProvenanceEncR, JoinedEventRemoved, PrunedEHSetR, GroupConvertDictR),
            case NewProvenanceEncR of
                [] ->
                    AccInResult1;
                _ ->
                    orddict:update(
                        ElemR,
                        fun(OldProvenance) ->
                            ext_type_provenance:plus_provenance(OldProvenance, NewProvenanceEncR)
                        end,
                        NewProvenanceEncR,
                        AccInResult1)
            end
        end,
        Result0,
        ProvenanceStoreEncR).

%% @private
prune_provenance(ProvenanceEnc, JoinedEventRemoved, PrunedEHSet, GroupConvertDict) ->
    ordsets:fold(
        fun(Dot, AccInResult) ->
            {IsPruned, UpdatedDot} =
                prune_dot(Dot, JoinedEventRemoved, PrunedEHSet, GroupConvertDict, ordsets:new()),
            case IsPruned of
                true ->
                    AccInResult;
                false ->
                    ordsets:add_element(UpdatedDot, AccInResult)
            end
        end,
        ordsets:new(),
        ProvenanceEnc).

%% @private
prune_dot([], _JoinedEventRemoved, _PrunedEHSet, _GroupConvertDict, UpdatedDot) ->
    {false, UpdatedDot};
prune_dot([H | T]=_Dot, JoinedEventRemoved, PrunedEHSet, GroupConvertDict, UpdatedDot) ->
    case ordsets:is_element(H, PrunedEHSet) of
        true ->
            {true, ordsets:new()};
        false ->
            case ext_type_event_history:get_event(H) of
                group_event ->
                    ConvertedGroupEH = orddict:fetch(H, GroupConvertDict),
                    NewUpdatedDot = ordsets:add_element(ConvertedGroupEH, UpdatedDot),
                    prune_dot(T, JoinedEventRemoved, PrunedEHSet, GroupConvertDict, NewUpdatedDot);
                Event ->
                    case ordsets:is_element(Event, JoinedEventRemoved) of
                        true ->
                            {true, ordsets:new()};
                        false ->
                            NewUpdatedDot = ordsets:add_element(H, UpdatedDot),
                            prune_dot(
                                T, JoinedEventRemoved, PrunedEHSet, GroupConvertDict,
                                NewUpdatedDot)
                    end
            end
    end.

%% @private
update_event_history_all_dicts(PathEncDict, EventHistoryAllDicts) ->
    lists:foldl(
        fun(EventHistoryType, AccInResult) ->
            EventHistoryDict = orddict:fetch(EventHistoryType, EventHistoryAllDicts),
            NewEventHistoryDict =
                update_event_history_dict(EventHistoryType, PathEncDict, EventHistoryDict),
            orddict:store(EventHistoryType, NewEventHistoryDict, AccInResult)
        end,
        EventHistoryAllDicts,
        [
            ext_event_history_total_order,
            ext_event_history_partial_order_downward_closed,
            ext_event_history_partial_order_independent]).

%% @private
update_event_history_dict(ext_event_history_total_order, PathEncDict, EventHistoryDict) ->
    orddict:fold(
        fun(NodeId, {EventHistory, GroupEncSet}, AccInResult) ->
            NewEventHistory = update_event_history(PathEncDict, EventHistory),
            orddict:store(NodeId, {NewEventHistory, GroupEncSet}, AccInResult)
        end,
        orddict:new(),
        EventHistoryDict);
update_event_history_dict(
    ext_event_history_partial_order_downward_closed, _PathEncDict, EventHistoryDict) ->
    EventHistoryDict;
update_event_history_dict(
    ext_event_history_partial_order_independent, PathEncDict, EventHistoryDict) ->
    orddict:fold(
        fun(EventHistory, GroupEncSet, AccInResult) ->
            NewEventHistory = update_event_history(PathEncDict, EventHistory),
            orddict:store(NewEventHistory, GroupEncSet, AccInResult)
        end,
        orddict:new(),
        EventHistoryDict).

%% @private
update_subset_unknown_enc(PathEncDict, SubsetUnknownEnc) ->
    ordsets:fold(
        fun(EventHistory, AccInResult) ->
            NewEventHistory = update_event_history(PathEncDict, EventHistory),
            ordsets:add_element(NewEventHistory, AccInResult)
        end,
        ordsets:new(),
        SubsetUnknownEnc).

%% @private
update_provenance_enc(PathEncDict, ProvenanceEnc) ->
    ordsets:fold(
        fun(Dot, AccInResult) ->
            NewDot =
                ordsets:fold(
                    fun(EventHistory, AccInNewDot) ->
                        NewEventHistory = update_event_history(PathEncDict, EventHistory),
                        ordsets:add_element(NewEventHistory, AccInNewDot)
                    end,
                    ordsets:new(),
                    Dot),
            ordsets:add_element(NewDot, AccInResult)
        end,
        ordsets:new(),
        ProvenanceEnc).

%% @private
update_event_history(PathEncDict, {EventType, {Event, DFPathEnc}}=_EventHistory) ->
    NewDFPathEnc = orddict:fetch(DFPathEnc, PathEncDict),
    {EventType, {Event, NewDFPathEnc}}.

%% @private
update_group_enc(
    ext_event_history_total_order, EventHistoryDict, GroupConvertDictL, GroupConvertDictR) ->
    orddict:fold(
        fun(NodeId, {EventHistory, {GroupEncSetL, GroupEncSetR}}, AccInResult) ->
            NewGroupEncSet0 =
                ordsets:fold(
                    fun(GroupEncL, AccInNewGroupEncSet0) ->
                        NewGroupEncL = orddict:fetch(GroupEncL, GroupConvertDictL),
                        ordsets:add_element(NewGroupEncL, AccInNewGroupEncSet0)
                    end,
                    ordsets:new(),
                    GroupEncSetL),
            NewGroupEncSet1 =
                ordsets:fold(
                    fun(GroupEncR, AccInNewGroupEncSet1) ->
                        NewGroupEncR = orddict:fetch(GroupEncR, GroupConvertDictR),
                        ordsets:add_element(NewGroupEncR, AccInNewGroupEncSet1)
                    end,
                    NewGroupEncSet0,
                    GroupEncSetR),
            orddict:store(NodeId, {EventHistory, NewGroupEncSet1}, AccInResult)
        end,
        orddict:new(),
        EventHistoryDict);
update_group_enc(
    ext_event_history_partial_order_downward_closed, EventHistoryDict,
    _GroupConvertDictL, _GroupConvertDictR) ->
    EventHistoryDict;
update_group_enc(
    ext_event_history_partial_order_independent, EventHistoryDict,
    GroupConvertDictL, GroupConvertDictR) ->
    orddict:fold(
        fun(EventHistory, {GroupEncSetL, GroupEncSetR}, AccInResult) ->
            NewGroupEncSet0 =
                ordsets:fold(
                    fun(GroupEncL, AccInNewGroupEncSet0) ->
                        NewGroupEncL = orddict:fetch(GroupEncL, GroupConvertDictL),
                        ordsets:add_element(NewGroupEncL, AccInNewGroupEncSet0)
                    end,
                    ordsets:new(),
                    GroupEncSetL),
            NewGroupEncSet1 =
                ordsets:fold(
                    fun(GroupEncR, AccInNewGroupEncSet1) ->
                        NewGroupEncR = orddict:fetch(GroupEncR, GroupConvertDictR),
                        ordsets:add_element(NewGroupEncR, AccInNewGroupEncSet1)
                    end,
                    NewGroupEncSet0,
                    GroupEncSetR),
            orddict:store(EventHistory, NewGroupEncSet1, AccInResult)
        end,
        orddict:new(),
        EventHistoryDict).

%% @private
add_subset_unknown_provenance_store_enc(
    SubsetUnknownEnc, ProvenanceStoreEnc, DataStoreEnc,
    EventRemoved, PrunedEHSetL, PrunedEHSetR,
    GroupDecodeDictL, GroupDecodeDictR,
    GroupConvertDictL, GroupConvertDictR,
    NewEHSetForL, NewEHSetForR,
    EventHistoryAllDictsL, EventHistoryAllDictsR) ->
    {
        NewDataStoreEnc,
        NewHighL, NewLowL, NewNoneL,
        NewHighR, NewLowR} =
        update_low_high(
            {SubsetUnknownEnc, ProvenanceStoreEnc},
            DataStoreEnc,
            orddict:new(), orddict:new(), orddict:new(),
            orddict:new(), orddict:new(),
            GroupDecodeDictL, GroupDecodeDictR,
            EventHistoryAllDictsL, EventHistoryAllDictsR,
            EventRemoved, PrunedEHSetL, PrunedEHSetR,
            GroupConvertDictL, GroupConvertDictR),
    join_data_store_enc_internal(
        [], NewDataStoreEnc,
        NewHighL, NewLowL, NewNoneL,
        NewHighR, NewLowR, orddict:new(),
        EventRemoved, PrunedEHSetL, PrunedEHSetR,
        GroupDecodeDictL, GroupDecodeDictR,
        GroupConvertDictL, GroupConvertDictR,
        NewEHSetForL, NewEHSetForR,
        EventHistoryAllDictsL, EventHistoryAllDictsR).

%% @private
% consistent_read_internal(
%     _PrevSubsetDec, _EventHistoryAllDicts, _EventHistoryAllDec, _GroupDecodeDict, []) ->
%     {ext_type_cover:new_subset_in_cover(), sets:new()};
% consistent_read_internal(
%     PrevSubsetDec,
%     EventHistoryAllDicts,
%     EventHistoryAllDec,
%     GroupDecodeDict,
%     [H | T]=_DataStoreEnc) ->
%     {NewEventHistoryAllDec, NewGroupDecodeDict} =
%         case {EventHistoryAllDec, GroupDecodeDict} of
%             {[], []} ->
%                 all_dicts_to_set(EventHistoryAllDicts);
%             _ ->
%                 {EventHistoryAllDec, GroupDecodeDict}
%         end,
%     {SubsetUnknownEnc, ProvenanceStoreEnc} = H,
%     SubsetUnknownDec = ext_type_cover:decode_subset(SubsetUnknownEnc, NewGroupDecodeDict),
%     SubsetDec = ordsets:subtract(NewEventHistoryAllDec, SubsetUnknownDec),
%     case ext_type_event_history_set:is_orderly_subset(PrevSubsetDec, SubsetDec) of
%         true ->
%             {SubsetDec, sets:from_list(orddict:fetch_keys(ProvenanceStoreEnc))};
%         false ->
%             consistent_read_internal(
%                 PrevSubsetDec, EventHistoryAllDicts, NewEventHistoryAllDec, NewGroupDecodeDict, T)
%     end.

-spec new(ext_type_path:ext_path_info_list()) -> ext_type_orset_base_v6().
new(AllPathInfoList) ->
    PathEncDict = generate_path_enc_dict(AllPathInfoList),
    EventHistoryAllDicts =
        ordsets:fold(
            fun(EventHistoryType, AccInEventHistoryAllDicts) ->
                orddict:store(EventHistoryType, orddict:new(), AccInEventHistoryAllDicts)
            end,
            orddict:new(),
            event_history_types()),
    DataStoreEnc = orddict:store(ordsets:new(), orddict:new(), orddict:new()),
    {PathEncDict, EventHistoryAllDicts, ordsets:new(), DataStoreEnc}.

-spec equal(ext_type_orset_base_v6(), ext_type_orset_base_v6()) -> boolean().
equal(
    {PathEncDictL, EventHistoryAllDictsL, EventRemovedL, DataStoreEncL}=_ORSetBaseV6L,
    {PathEncDictR, EventHistoryAllDictsR, EventRemovedR, DataStoreEncR}=_ORSetBaseV6R) ->
    PathEncDictL == PathEncDictR andalso
        EventHistoryAllDictsL == EventHistoryAllDictsR andalso
        EventRemovedL == EventRemovedR andalso
        DataStoreEncL == DataStoreEncR.

-spec insert(ext_type_event_history:ext_event_history(), element(), ext_type_orset_base_v6()) ->
    ext_type_orset_base_v6().
insert(
    {EventHistoryType, _EventHistoryContents}=EventHistory,
    Elem,
    {PathEncDict, EventHistoryAllDicts, EventRemoved, DataStoreEnc}=ORSetBaseV6) ->
    EventHistoryAllDict = orddict:fetch(EventHistoryType, EventHistoryAllDicts),
    {NewEventHistoryAllDict, PrunedEventHistorySet} =
        add_event_history_dict(EventHistory, EventHistoryAllDict),
    case NewEventHistoryAllDict of
        EventHistoryAllDict ->
            ORSetBaseV6;
        _ ->
            NewEventHistoryAllDicts =
                orddict:store(EventHistoryType, NewEventHistoryAllDict, EventHistoryAllDicts),
            NewDot = ext_type_provenance:new_dot(EventHistory),
            NewProvenance = ext_type_provenance:new_provenance(NewDot),
            [{SingleSubsetUnknown, ProvenanceStoreEnc}] = DataStoreEnc,
            NewSingleSubsetUnknown =
                ordsets:subtract(SingleSubsetUnknown, PrunedEventHistorySet),
            NewProvenanceStoreEnc =
                case ProvenanceStoreEnc of
                    [] ->
                        orddict:store(Elem, NewProvenance, orddict:new());
                    _ ->
                        PrunedProvenanceStoreEnc =
                            orddict:fold(
                                fun(ElemInPS, ProvenanceEncInPS, AccInNewProvenanceStoreEnc0) ->
                                    PrunedProvenanceEncInPS =
                                        ordsets:fold(
                                            fun(DotEncInPS, AccInPrunedProvenanceEncInPS) ->
                                                PrunedDotEncInPS =
                                                    ordsets:subtract(
                                                        DotEncInPS, PrunedEventHistorySet),
                                                case PrunedDotEncInPS of
                                                    [] ->
                                                        AccInPrunedProvenanceEncInPS;
                                                    _ ->
                                                        ordsets:add_element(
                                                            PrunedDotEncInPS,
                                                            AccInPrunedProvenanceEncInPS)
                                                end
                                            end,
                                            ordsets:new(),
                                            ProvenanceEncInPS),
                                    case PrunedProvenanceEncInPS of
                                        [] ->
                                            AccInNewProvenanceStoreEnc0;
                                        _ ->
                                            orddict:store(
                                                ElemInPS,
                                                PrunedProvenanceEncInPS,
                                                AccInNewProvenanceStoreEnc0)
                                    end
                                end,
                                orddict:new(),
                                ProvenanceStoreEnc),
                        orddict:update(
                            Elem,
                            fun(OldProvenance) ->
                                ext_type_provenance:plus_provenance(OldProvenance, NewProvenance)
                            end,
                            NewProvenance,
                            PrunedProvenanceStoreEnc)
                end,
            NewDataStoreEnc =
                orddict:store(NewSingleSubsetUnknown, NewProvenanceStoreEnc, orddict:new()),
            {PathEncDict, NewEventHistoryAllDicts, EventRemoved, NewDataStoreEnc}
    end.

-spec read(ext_type_cover:ext_subset_in_cover(), ext_type_orset_base_v6()) ->
    {ext_type_cover:ext_subset_in_cover(), sets:set()}.
read(
    PrevSubsetDec,
    {_PathEncDict, EventHistoryAllDicts, _EventRemoved, DataStoreEnc}=_ORSetBaseV6) ->
    read_internal(PrevSubsetDec, EventHistoryAllDicts, ordsets:new(), orddict:new(), DataStoreEnc).

-spec join(
    ext_type_orset_base:ext_node_type(), ext_type_orset_base_v6(), ext_type_orset_base_v6()) ->
    ext_type_orset_base_v6().
join(_NodeType, ORSetBaseV6, ORSetBaseV6) ->
    ORSetBaseV6;
join(
    input,
    {
        PathEncDict,
        EventHistoryAllDictsL,
        EventRemovedL,
        [{[], ProvenanceStoreL}]=_DataStoreL}=_ORSetBaseV6L,
    {
        PathEncDict,
        EventHistoryAllDictsR,
        EventRemovedR,
        [{[], ProvenanceStoreR}]=_DataStoreR}=_ORSetBaseV6R) ->
    {JoinedEventHistoryAllDicts, PrunedEncSetL, PrunedEncSetR} =
        join_event_history_all_dicts_input(EventHistoryAllDictsL, EventHistoryAllDictsR),

    JoinedEventRemoved = ordsets:union(EventRemovedL, EventRemovedR),

    JoinedProvenanceStore0 =
        orddict:fold(
            fun(ElemL, ProvenanceEncL, AccInJoinedProvenanceStore0) ->
                NewProvenanceEncL =
                    ordsets:fold(
                        fun(DotEncL, AccInNewProvenanceEncL) ->
                            case ordsets:subtract(
                                ordsets:subtract(DotEncL, JoinedEventRemoved), PrunedEncSetL) of
                                DotEncL ->
                                    ordsets:add_element(DotEncL, AccInNewProvenanceEncL);
                                _ ->
                                    AccInNewProvenanceEncL
                            end
                        end,
                        ordsets:new(),
                        ProvenanceEncL),
                case NewProvenanceEncL of
                    [] ->
                        AccInJoinedProvenanceStore0;
                    _ ->
                        orddict:update(
                            ElemL,
                            fun(OldProvenanceEnc) ->
                                ext_type_provenance:plus_provenance(
                                    OldProvenanceEnc, NewProvenanceEncL)
                            end,
                            NewProvenanceEncL,
                            AccInJoinedProvenanceStore0)
                end
            end,
            orddict:new(),
            ProvenanceStoreL),
    JoinedProvenanceStore =
        orddict:fold(
            fun(ElemR, ProvenanceEncR, AccInJoinedProvenanceStore) ->
                NewProvenanceEncR =
                    ordsets:fold(
                        fun(DotEncR, AccInNewProvenanceEncR) ->
                            case ordsets:subtract(
                                ordsets:subtract(DotEncR, JoinedEventRemoved), PrunedEncSetR) of
                                DotEncR ->
                                    ordsets:add_element(DotEncR, AccInNewProvenanceEncR);
                                _ ->
                                    AccInNewProvenanceEncR
                            end
                        end,
                        ordsets:new(),
                        ProvenanceEncR),
                case NewProvenanceEncR of
                    [] ->
                        AccInJoinedProvenanceStore;
                    _ ->
                        orddict:update(
                            ElemR,
                            fun(OldProvenanceEnc) ->
                                ext_type_provenance:plus_provenance(
                                    OldProvenanceEnc, NewProvenanceEncR)
                            end,
                            NewProvenanceEncR,
                            AccInJoinedProvenanceStore)
                end
            end,
            JoinedProvenanceStore0,
            ProvenanceStoreR),
    JoinedDataStore = orddict:store([], JoinedProvenanceStore, orddict:new()),

    {PathEncDict, JoinedEventHistoryAllDicts, JoinedEventRemoved, JoinedDataStore};
join(
    intermediate,
    {PathEncDict, EventHistoryAllDictsL, EventRemovedL, DataStoreEncL}=_ORSetBaseV6L,
    {PathEncDict, EventHistoryAllDictsR, EventRemovedR, DataStoreEncR}=_ORSetBaseV6R) ->
    {
        JoinedEventHistoryAllDicts,
        PrunedEHSetL, PrunedEHSetR,
        GroupDecodeDictL, GroupDecodeDictR,
        GroupConvertDictL, GroupConvertDictR,
        NewEHSetForL, NewEHSetForR} =
        join_event_history_all_dicts_intermediate(EventHistoryAllDictsL, EventHistoryAllDictsR),

    JoinedEventRemoved = ordsets:union(EventRemovedL, EventRemovedR),

    JoinedDataStoreEnc =
        join_data_store_enc(
            DataStoreEncL, DataStoreEncR,
            JoinedEventRemoved, PrunedEHSetL, PrunedEHSetR,
            GroupDecodeDictL, GroupDecodeDictR,
            GroupConvertDictL, GroupConvertDictR,
            NewEHSetForL, NewEHSetForR,
            EventHistoryAllDictsL, EventHistoryAllDictsR),

    {PathEncDict, JoinedEventHistoryAllDicts, JoinedEventRemoved, JoinedDataStoreEnc}.

-spec is_inflation(ext_type_orset_base_v6(), ext_type_orset_base_v6()) -> boolean().
is_inflation(
    {PathEncDict, EventHistoryAllDictsL, EventRemovedL, DataStoreEncL}=_ORSetBaseV6L,
    {PathEncDict, EventHistoryAllDictsR, EventRemovedR, DataStoreEncR}=_ORSetBaseV6R) ->
% (EventHistoryAllL \subseteq EventHistoryAllR)
% \land
% (EventHistorySurvivedR \cap EventHistoryAllL \subseteq EventHistorySurvivedL)
% \land
% ((CoverL, DataStoreL) \sqsubseteq (CoverR, DataStoreR))
    {IsInflationEventHistoryAllDicts, GroupDecodeDictL, GroupDecodeDictR, PrunedEncSetL} =
        is_inflation_event_history_all_dicts(EventHistoryAllDictsL, EventHistoryAllDictsR),
    IsInflationEventHistoryAllDicts andalso
        ordsets:is_subset(EventRemovedL, EventRemovedR) andalso
        is_inflation_data_store_enc(
            DataStoreEncL, GroupDecodeDictL, EventHistoryAllDictsL,
            DataStoreEncR, GroupDecodeDictR, EventRemovedR,
            PrunedEncSetL).

-spec is_strict_inflation(ext_type_orset_base_v6(), ext_type_orset_base_v6()) -> boolean().
is_strict_inflation(ORSetBaseL, ORSetBaseR) ->
    ext_type_orset_base:is_strict_inflation(ORSetBaseL, ORSetBaseR).

-spec map(
    {ext_node_id(), ext_replica_id()},
    function(),
    ext_type_path:ext_path_info_list(),
    ext_type_path:ext_path_info(),
    ext_type_orset_base_v6()) -> ext_type_orset_base_v6().
map({_NodeId, _ReplicaId}=_Actor,
    Function,
    AllPathInfoList,
    _PathInfo,
    {_PathEncDict, EventHistoryAllDicts, EventRemoved, DataStoreEnc}=_ORSetBaseV6) ->
    NewPathEncDict = generate_path_enc_dict(AllPathInfoList),

    NewEventHistoryAllDicts = update_event_history_all_dicts(NewPathEncDict, EventHistoryAllDicts),

    NewDataStoreEnc =
        orddict:fold(
            fun(SubsetUnknownEnc, ProvenanceStoreEnc, AccInNewDataStoreEnc) ->
                NewSubsetUnknownEnc = update_subset_unknown_enc(NewPathEncDict, SubsetUnknownEnc),
                NewProvenanceStoreEnc =
                    orddict:fold(
                        fun(Elem, ProvenanceEnc, AccInNewProvenanceStoreEnc) ->
                            NewProvenanceEnc =
                                update_provenance_enc(NewPathEncDict, ProvenanceEnc),
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

    {NewPathEncDict, NewEventHistoryAllDicts, EventRemoved, NewDataStoreEnc}.

-spec filter(
    {ext_node_id(), ext_replica_id()},
    function(),
    ext_type_path:ext_path_info_list(),
    ext_type_path:ext_path_info(),
    ext_type_orset_base_v6()) -> ext_type_orset_base_v6().
filter(
    {_NodeId, _ReplicaId}=_Actor,
    Function,
    AllPathInfoList,
    _PathInfo,
    {_PathEncDict, EventHistoryAllDicts, EventRemoved, DataStoreEnc}=_ORSetBaseV6) ->
    NewPathEncDict = generate_path_enc_dict(AllPathInfoList),

    NewEventHistoryAllDicts = update_event_history_all_dicts(NewPathEncDict, EventHistoryAllDicts),

    NewDataStore =
        orddict:fold(
            fun(SubsetUnknownEnc, ProvenanceStore, AccInNewDataStore) ->
                NewSubsetUnknownEnc = update_subset_unknown_enc(NewPathEncDict, SubsetUnknownEnc),
                NewProvenanceStore =
                    orddict:fold(
                        fun(Elem, ProvenanceEnc, AccInNewProvenanceStore) ->
                            case Function(Elem) of
                                false ->
                                    AccInNewProvenanceStore;
                                true ->
                                    NewProvenanceEnc =
                                        update_provenance_enc(NewPathEncDict, ProvenanceEnc),
                                    orddict:store(Elem, NewProvenanceEnc, AccInNewProvenanceStore)
                            end
                        end,
                        orddict:new(),
                        ProvenanceStore),
                orddict:store(NewSubsetUnknownEnc, NewProvenanceStore, AccInNewDataStore)
            end,
            orddict:new(),
            DataStoreEnc),

    {NewPathEncDict, NewEventHistoryAllDicts, EventRemoved, NewDataStore}.

-spec product(
    {ext_node_id(), ext_replica_id()},
    ext_type_path:ext_path_info_list(),
    ext_type_path:ext_path_info(),
    ext_type_orset_base_v6(),
    ext_type_orset_base_v6()) -> ext_type_orset_base_v6().
product(
    {_NodeId, _ReplicaId}=_Actor,
    AllPathInfoList,
    _PathInfo,
    {_PathEncDictL, EventHistoryAllDictsL, EventRemovedL, DataStoreEncL}=_ORSetBaseV6L,
    {_PathEncDictR, EventHistoryAllDictsR, EventRemovedR, DataStoreEncR}=_ORSetBaseV6R) ->
    NewPathEncDict = generate_path_enc_dict(AllPathInfoList),

    NewEventHistoryAllDictsL =
        update_event_history_all_dicts(NewPathEncDict, EventHistoryAllDictsL),
    NewEventHistoryAllDictsR =
        update_event_history_all_dicts(NewPathEncDict, EventHistoryAllDictsR),

    {
        ProductEventHistoryAllDicts,
        PrunedEHSetL, PrunedEHSetR,
        GroupDecodeDictL, GroupDecodeDictR,
        GroupConvertDictL, GroupConvertDictR,
        NewEHSetForL, NewEHSetForR} =
        join_event_history_all_dicts_intermediate(
            NewEventHistoryAllDictsL, NewEventHistoryAllDictsR),

    ProductEventRemoved = ordsets:union(EventRemovedL, EventRemovedR),

    ProductDataStoreEnc =
        orddict:fold(
            fun(SubsetUnknownEncL, ProvenanceStoreEncL, AccInProductDataStoreEncL) ->
                orddict:fold(
                    fun(SubsetUnknownEncR, ProvenanceStoreEncR, AccInProductDataStoreEncR) ->
                        NewSubsetUnknownEncL0 =
                            update_subset_unknown_enc(NewPathEncDict, SubsetUnknownEncL),
                        NewSubsetUnknownEncL1 =
                            ordsets:fold(
                                fun(EventHistoryL, AccInNewSubsetUnknownEncL1) ->
                                    case ext_type_event_history:get_event(EventHistoryL) of
                                        group_event ->
                                            NewEventHistoryL =
                                                orddict:fetch(EventHistoryL, GroupConvertDictL),
                                            ordsets:add_element(
                                                NewEventHistoryL, AccInNewSubsetUnknownEncL1);
                                        _ ->
                                            ordsets:add_element(
                                                EventHistoryL, AccInNewSubsetUnknownEncL1)
                                    end
                                end,
                                ordsets:new(),
                                ordsets:union(
                                    ordsets:subtract(NewSubsetUnknownEncL0, PrunedEHSetL),
                                    NewEHSetForL)),

                        NewSubsetUnknownEncR0 =
                            update_subset_unknown_enc(NewPathEncDict, SubsetUnknownEncR),
                        NewSubsetUnknownEncR1 =
                            ordsets:fold(
                                fun(EventHistoryR, AccInNewSubsetUnknownEncR1) ->
                                    case ext_type_event_history:get_event(EventHistoryR) of
                                        group_event ->
                                            NewEventHistoryR =
                                                orddict:fetch(EventHistoryR, GroupConvertDictR),
                                            ordsets:add_element(
                                                NewEventHistoryR, AccInNewSubsetUnknownEncR1);
                                        _ ->
                                            ordsets:add_element(
                                                EventHistoryR, AccInNewSubsetUnknownEncR1)
                                    end
                                end,
                                ordsets:new(),
                                ordsets:union(
                                    ordsets:subtract(NewSubsetUnknownEncR0, PrunedEHSetR),
                                    NewEHSetForR)),

                        ProductSubsetUnknownEnc =
                            ordsets:intersection(NewSubsetUnknownEncL1, NewSubsetUnknownEncR1),

                        ProductProvenanceStoreEnc =
                            orddict:fold(
                                fun(ElemL, ProvenanceEncL,
                                    AccInProductProvenanceStoreEncL) ->
                                    orddict:fold(
                                        fun(ElemR, ProvenanceEncR,
                                            AccInProductProvenanceStoreEncR) ->
                                            NewProvenanceEncL =
                                                prune_provenance(
                                                    update_provenance_enc(
                                                        NewPathEncDict, ProvenanceEncL),
                                                    ProductEventRemoved,
                                                    PrunedEHSetL, GroupConvertDictL),
                                            NewProvenanceEncR =
                                                prune_provenance(
                                                    update_provenance_enc(
                                                        NewPathEncDict, ProvenanceEncR),
                                                    ProductEventRemoved,
                                                    PrunedEHSetR, GroupConvertDictR),
                                            NewProvenanceEnc =
                                                ext_type_provenance:cross_provenance(
                                                    NewProvenanceEncL, NewProvenanceEncR),
                                            case NewProvenanceEnc of
                                                [] ->
                                                    AccInProductProvenanceStoreEncR;
                                                _ ->
                                                    orddict:store(
                                                        {ElemL, ElemR},
                                                        NewProvenanceEnc,
                                                        AccInProductProvenanceStoreEncR)
                                            end
                                        end,
                                        AccInProductProvenanceStoreEncL,
                                        ProvenanceStoreEncR)
                                end,
                                orddict:new(),
                                ProvenanceStoreEncL),

                        add_subset_unknown_provenance_store_enc(
                            ProductSubsetUnknownEnc, ProductProvenanceStoreEnc,
                            AccInProductDataStoreEncR,
                            ProductEventRemoved, PrunedEHSetL, PrunedEHSetR,
                            GroupDecodeDictL, GroupDecodeDictR,
                            GroupConvertDictL, GroupConvertDictR,
                            NewEHSetForL, NewEHSetForR,
                            NewEventHistoryAllDictsL, NewEventHistoryAllDictsR)
                    end,
                    AccInProductDataStoreEncL,
                    DataStoreEncR)
            end,
            orddict:new(),
            DataStoreEncL),

    {NewPathEncDict, ProductEventHistoryAllDicts, ProductEventRemoved, ProductDataStoreEnc}.

-spec consistent_read(
    ext_type_path:ext_path_info_list(),
    ext_type_cover:ext_subset_in_cover(),
    ext_type_provenance:ext_dot_set(),
    ext_type_orset_base_v6()) ->
    {ext_type_cover:ext_subset_in_cover(), ext_type_provenance:ext_dot_set(), sets:set()}.
consistent_read(
    AllPathInfoList,
    PrevSubsetDec,
    PrevCDS,
    {EventHistoryAllDict, _EventRemoved, DataStoreEnc}=_ORSetBaseV6) ->
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
    ext_type_orset_base_v6()) -> ext_type_orset_base_v6().
set_count(
    {NodeId, _ReplicaId}=_Actor,
    AllPathInfoList,
    PathInfo,
    {EventHistoryAllDict, EventRemoved, DataStoreEnc}=_ORSetBaseV6) ->
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
    ext_type_orset_base_v6()) -> ext_type_orset_base_v6().
group_by_sum(
    {NodeId, _ReplicaId}=_Actor,
    SumFunction,
    AllPathInfoList,
    PathInfo,
    {EventHistoryAllDict, EventRemoved, DataStoreEnc}=_ORSetBaseV6) ->
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
    ext_type_orset_base_v6()) -> ext_type_orset_base_v6().
order_by(
    {NodeId, _ReplicaId}=_Actor,
    CompareFunction,
    AllPathInfoList,
    PathInfo,
    {EventHistoryAllDict, EventRemoved, DataStoreEnc}=_ORSetBaseV6) ->
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
    ext_type_orset_base_v6()) -> ext_type_event_history:ext_event_history().
next_event_history(
    EventType,
    NodeId,
    ReplicaId,
    {_PathEncDict, EventHistoryAllDicts, _EventRemoved, _DataStore}=_ORSetBaseV6L) ->
    EventHistoryAllDict = orddict:fetch(EventType, EventHistoryAllDicts),
    get_next_event_history_dict(EventType, NodeId, ReplicaId, EventHistoryAllDict).

%% @private
%%is_inflation_provenance_store(
%%    [], _GroupDecodeDictL, _ProvenanceStoreEncR, _GroupDecodeDictR, _EventHistorySurvivedDecR) ->
%%    true;
%%is_inflation_provenance_store(
%%    [{Elem, ProvenanceEncL} | T]=_ProvenanceStoreEncL, GroupDecodeDictL,
%%    ProvenanceStoreEncR, GroupDecodeDictR,
%%    EventHistorySurvivedDecR) ->
%%    ProvenanceDecL = ext_type_provenance:decode_provenance(ProvenanceEncL, GroupDecodeDictL),
%%    DotSetDecL = ext_type_provenance:prune_provenance(ProvenanceDecL, EventHistorySurvivedDecR),
%%    ProvenanceEncR =
%%        case orddict:find(Elem, ProvenanceStoreEncR) of
%%            {ok, Provenance} ->
%%                Provenance;
%%            error ->
%%                ordsets:new()
%%        end,
%%    ProvenanceDecR = ext_type_provenance:decode_provenance(ProvenanceEncR, GroupDecodeDictR),
%%    DotSetDecR = ext_type_provenance:prune_provenance(ProvenanceDecR, EventHistorySurvivedDecR),
%%    case ordsets:is_subset(DotSetDecL, DotSetDecR) of
%%        false ->
%%            false;
%%        true ->
%%            is_inflation_provenance_store(T, GroupDecodeDictL,
%%                ProvenanceStoreEncR, GroupDecodeDictR,
%%                EventHistorySurvivedDecR)
%%    end.

%% @private
%%is_inflation_data_store(
%%    [], _DataStoreEncR, _GroupDecodeDictR, _EventHistorySurvivedDecR,
%%    _ProvenanceStoreEncL, _GroupDecodeDictL) ->
%%    true;
%%is_inflation_data_store(
%%    [H | T]=_RelatedSubsetUnknownEncsR, DataStoreEncR, GroupDecodeDictR, EventHistorySurvivedDecR,
%%    ProvenanceStoreEncL, GroupDecodeDictL) ->
%%    case is_inflation_provenance_store(
%%        ProvenanceStoreEncL, GroupDecodeDictL,
%%        orddict:fetch(H, DataStoreEncR), GroupDecodeDictR,
%%        EventHistorySurvivedDecR) of
%%        false ->
%%            false;
%%        true ->
%%            is_inflation_data_store(
%%                T, DataStoreEncR, GroupDecodeDictR, EventHistorySurvivedDecR,
%%                ProvenanceStoreEncL, GroupDecodeDictL)
%%    end.

%% @private
%%is_inflation_cover_data_store(
%%    [], _EventHistoryAllL, _GroupDecodeDictL,
%%    _DataStoreEncR, _EventHistoryAllR, _GroupDecodeDictR,
%%    _EventHistorySurvivedR) ->
%%    true;
%%is_inflation_cover_data_store(
%%    [{SubsetUnknownEncL, ProvenanceStoreEncL} | T]=_DataStoreEncL, EventHistoryAllDecL, GroupDecodeDictL,
%%    DataStoreEncR, EventHistoryAllDecR, GroupDecodeDictR,
%%    EventHistorySurvivedDecR) ->
%%    SubsetUnknownDecL = ext_type_cover:decode_subset(SubsetUnknownEncL, GroupDecodeDictL),
%%    SubsetDecL = ext_type_event_history_set:subtract(EventHistoryAllDecL, SubsetUnknownDecL),
%%    RelatedSubsetUnknownEncsR =
%%        lists:foldl(
%%            fun(SubsetUnknownEncR, AccInRelatedSubsetUnknownEncs) ->
%%                SubsetUnknownDecR = ext_type_cover:decode_subset(SubsetUnknownEncR, GroupDecodeDictR),
%%                SubsetDecR =
%%                    ext_type_event_history_set:subtract(EventHistoryAllDecR, SubsetUnknownDecR),
%%                case ext_type_event_history_set:is_orderly_subset(SubsetDecL, SubsetDecR) of
%%                    false ->
%%                        AccInRelatedSubsetUnknownEncs;
%%                    true ->
%%                        ordsets:add_element(SubsetUnknownEncR, AccInRelatedSubsetUnknownEncs)
%%                end
%%            end,
%%            ordsets:new(),
%%            orddict:fetch_keys(DataStoreEncR)),
%%    case RelatedSubsetUnknownEncsR of
%%        [] ->
%%            false;
%%        _ ->
%%            case is_inflation_data_store(
%%                RelatedSubsetUnknownEncsR, DataStoreEncR, GroupDecodeDictR, EventHistorySurvivedDecR,
%%                ProvenanceStoreEncL, GroupDecodeDictL) of
%%                false ->
%%                    false;
%%                true ->
%%                    is_inflation_cover_data_store(
%%                        T, EventHistoryAllDecL, GroupDecodeDictL,
%%                        DataStoreEncR, EventHistoryAllDecR, GroupDecodeDictR,
%%                        EventHistorySurvivedDecR)
%%            end
%%    end.

%%%% @private
%%combine_provenance_store(ProvenanceStoreL, ProvenanceStoreR) ->
%%    orddict:merge(
%%        fun(_Elem, ProvenanceL, ProvenanceR) ->
%%            ext_type_provenance:plus_provenance(ProvenanceL, ProvenanceR)
%%        end,
%%        ProvenanceStoreL,
%%        ProvenanceStoreR).

%%%% @private
%%add_subset_provenance_store(SubsetUnknown, ProvenanceStore, [], _EventHistoryAll) ->
%%    orddict:store(SubsetUnknown, ProvenanceStore, orddict:new());
%%add_subset_provenance_store(SubsetUnknown, ProvenanceStore, DataStore, EventHistoryAll) ->
%%    Subset = ext_type_event_history_set:subtract(EventHistoryAll, SubsetUnknown),
%%    {SuperSubsets, NewDataStore} =
%%        orddict:fold(
%%            fun(SubsetUnknownInCover, PSInDataStore, {AccInSuperSubsets, AccInNewDataStore}) ->
%%                SubsetInCover =
%%                    ext_type_event_history_set:subtract(EventHistoryAll, SubsetUnknownInCover),
%%                case ext_type_event_history_set:is_orderly_subset(Subset, SubsetInCover) of
%%                    false ->
%%                        {AccInSuperSubsets, AccInNewDataStore};
%%                    true ->
%%                        {
%%                            ordsets:add_element(SubsetUnknownInCover, AccInSuperSubsets),
%%                            orddict:store(
%%                                SubsetUnknownInCover,
%%                                combine_provenance_store(ProvenanceStore, PSInDataStore),
%%                                AccInNewDataStore)}
%%                end
%%            end,
%%            {ordsets:new(), DataStore},
%%            DataStore),
%%    case SuperSubsets of
%%        [] ->
%%            {SubSubsets, NewNewDataStore} =
%%                orddict:fold(
%%                    fun(SubsetUnknownInCover, PSInDataStore, {AccInSubSubsets, AccInNewNewDataStore}) ->
%%                        SubsetInCover =
%%                            ext_type_event_history_set:subtract(EventHistoryAll, SubsetUnknownInCover),
%%                        case ext_type_event_history_set:is_orderly_subset(SubsetInCover, Subset) of
%%                            false ->
%%                                {
%%                                    AccInSubSubsets,
%%                                    orddict:store(
%%                                        SubsetUnknownInCover, PSInDataStore, AccInNewNewDataStore)};
%%                            true ->
%%                                NewProvenanceStore =
%%                                    combine_provenance_store(ProvenanceStore, PSInDataStore),
%%                                {
%%                                    ordsets:add_element(SubsetUnknownInCover, AccInSubSubsets),
%%                                    orddict:update(
%%                                        SubsetUnknown,
%%                                        fun(OldProvenanceStore) ->
%%                                            combine_provenance_store(
%%                                                OldProvenanceStore, NewProvenanceStore)
%%                                        end,
%%                                        NewProvenanceStore,
%%                                        AccInNewNewDataStore)}
%%                        end
%%                    end,
%%                    {ordsets:new(), orddict:new()},
%%                    DataStore),
%%            case SubSubsets of
%%                [] ->
%%                    orddict:store(SubsetUnknown, ProvenanceStore, DataStore);
%%                _ ->
%%                    NewNewDataStore
%%            end;
%%        _ ->
%%            NewDataStore
%%    end.

%%%% @private
%%encode_provenance_store(ProvenanceStoreDec, GroupEncodeDict) ->
%%    orddict:fold(
%%        fun(Elem, ProvenanceDec, AccInResult) ->
%%            orddict:store(
%%                Elem,
%%                ext_type_provenance:encode_provenance(ProvenanceDec, GroupEncodeDict),
%%                AccInResult)
%%        end,
%%        orddict:new(),
%%        ProvenanceStoreDec).

%%%% @private
%%encode_data_store(DataStoreDec, GroupEncodeDict) ->
%%    orddict:fold(
%%        fun(SubsetUnknownDec, ProvenanceStoreDec, AccInResult) ->
%%            orddict:store(
%%                ext_type_cover:encode_subset(SubsetUnknownDec, GroupEncodeDict),
%%                encode_provenance_store(ProvenanceStoreDec, GroupEncodeDict),
%%                AccInResult)
%%        end,
%%        orddict:new(),
%%        DataStoreDec).

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

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

generate_path_enc_dict_test() ->
    AllPathInfoList0 =
        [{<<"node1">>, {undefined, undefined}}],
    ExpectedOutput0 =
        orddict:from_list([{undefined, {<<"node1">>, {<<"node1">>, 1}}}]),
    ?assertEqual(ExpectedOutput0, generate_path_enc_dict(AllPathInfoList0)),

    AllPathInfoList1 =
        [
            {<<"node4">>, {<<"node1">>, <<"node2">>}},
            {<<"node1">>, {undefined, undefined}},
            {<<"node2">>, {undefined, undefined}}],
    ExpectedOutput1 =
        orddict:from_list(
            [
                {{<<"node1">>, {<<"node1">>, 1}}, {<<"node4">>, {<<"node1">>, 1}}},
                {{<<"node2">>, {<<"node2">>, 1}}, {<<"node4">>, {<<"node2">>, 1}}}]),
    ?assertEqual(ExpectedOutput1, generate_path_enc_dict(AllPathInfoList1)),

    AllPathInfoList2 =
        [
            {<<"node6">>, {<<"node4">>, <<"node5">>}},
            {<<"node4">>, {<<"node1">>, <<"node2">>}},
            {<<"node1">>, {undefined, undefined}},
            {<<"node2">>, {undefined, undefined}},
            {<<"node5">>, {<<"node2">>, <<"node3">>}},
            {<<"node2">>, {undefined, undefined}},
            {<<"node3">>, {undefined, undefined}}],
    ExpectedOutput2 =
        orddict:from_list(
            [
                {{<<"node4">>, {<<"node1">>, 1}}, {<<"node6">>, {<<"node1">>, 1}}},
                {{<<"node4">>, {<<"node2">>, 1}}, {<<"node6">>, {<<"node2">>, 2}}},
                {{<<"node5">>, {<<"node2">>, 1}}, {<<"node6">>, {<<"node2">>, 1}}},
                {{<<"node5">>, {<<"node3">>, 1}}, {<<"node6">>, {<<"node3">>, 1}}}]),
    ?assertEqual(ExpectedOutput2, generate_path_enc_dict(AllPathInfoList2)).

-endif.
