-module(ext_type_event_history_set).

-author("Junghun Yoo <junghun.yoo@cs.ox.ac.uk>").

%% API
-export([
    new_event_history_set/0,
    add_event_history/2,
    union_event_history_set/2,
    intersection_event_history_set/2,
    minus_event_history_set/2,
    is_subset/2,
    is_orderly_subset/2,
    is_found_dominant_event_history/2,
    append_cur_node/3,
    build_event_dict/1,
    generate_power_set_without_empty_set/1,
    remove_group_event_history/1,
    set_size/1]).
-export([
    build_path_append_dict_and_update/4,
    append_cur_node/2]).
-export([
    find_survived/2,
    subtract/2]).
-export([
    set_to_dict/1,
    dict_to_set/1,
    encode_set/2,
    decode_set/2,
    append_cur_node_enc/3,
    from_list/1,
    join_event_history_set/2]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([
    ext_event_history_set/0]).

-type ext_event_history_set() :: ordsets:ordset(ext_type_event_history:ext_event_history()).
-type ext_node_id() :: term().
-type ext_path_info() :: term().

-spec new_event_history_set() -> ext_event_history_set().
new_event_history_set() ->
    ordsets:new().

-spec add_event_history(ext_type_event_history:ext_event_history(), ext_event_history_set()) ->
    ext_event_history_set().
add_event_history(EventHistory, EventHistorySet) ->
    ordsets:add_element(EventHistory, EventHistorySet).

-spec union_event_history_set(ext_event_history_set(), ext_event_history_set()) ->
    ext_event_history_set().
union_event_history_set(EventHistorySetL, EventHistorySetR) ->
    UnionEventHistorySet = ordsets:union(EventHistorySetL, EventHistorySetR),
    max_event_history_set(ordsets:new(), UnionEventHistorySet).

-spec intersection_event_history_set(ext_event_history_set(), ext_event_history_set()) ->
    ext_event_history_set().
intersection_event_history_set(EventHistorySetL, EventHistorySetR) ->
    ordsets:intersection(EventHistorySetL, EventHistorySetR).

-spec minus_event_history_set(ext_event_history_set(), ext_event_history_set()) ->
    ext_event_history_set().
minus_event_history_set(EventHistorySetL, EventHistorySetR) ->
    ordsets:fold(
        fun(EventHistory, AccInNotRelatedEventHistorySetL) ->
            case is_found_dominant_event_history(EventHistory, EventHistorySetR) of
                true ->
                    AccInNotRelatedEventHistorySetL;
                false ->
                    ordsets:add_element(
                        EventHistory, AccInNotRelatedEventHistorySetL)
            end
        end,
        ordsets:new(),
        EventHistorySetL).

-spec is_subset(ext_event_history_set(), ext_event_history_set()) -> boolean().
is_subset(EventHistorySetL, EventHistorySetR) ->
    ordsets:is_subset(EventHistorySetL, EventHistorySetR).

-spec is_orderly_subset(ext_event_history_set(), ext_event_history_set()) -> boolean().
is_orderly_subset(EventHistorySetL, EventHistorySetR) ->
    is_orderly_subset_internal(EventHistorySetL, EventHistorySetR).

-spec is_found_dominant_event_history(
    ext_type_event_history:ext_event_history(), ext_event_history_set()) -> boolean().
is_found_dominant_event_history(_EventHistory, []) ->
    false;
is_found_dominant_event_history(EventHistory, [H | T]=_EventHistorySet) ->
    case ext_type_event_history:is_related_to(EventHistory, H) of
        true ->
            true;
        false ->
            is_found_dominant_event_history(EventHistory, T)
    end.

-spec append_cur_node(ext_node_id(), ext_path_info(), ext_event_history_set()) ->
    ext_event_history_set().
append_cur_node(CurNodeId, CurPathInfo, EventHistorySet) ->
    ordsets:fold(
        fun(EventHistory, AccInResult) ->
            NewEventHistory =
                ext_type_event_history:append_cur_node(CurNodeId, CurPathInfo, EventHistory),
            ordsets:add_element(NewEventHistory, AccInResult)
        end,
        ordsets:new(),
        EventHistorySet).

-spec build_event_dict(ext_event_history_set()) ->
    {ext_event_history_set(), orddict:orddict(ext_node_id(), ext_event_history_set())}.
build_event_dict(EventHistorySet) ->
    ordsets:fold(
        fun(EventHistory, {AccInNoGroupSet, AccInResultDict}) ->
            case ext_type_event_history:get_event(EventHistory) of
                group_event ->
                    {AccInNoGroupSet, AccInResultDict};
                Event ->
                    NewResultDict =
                        orddict:update(
                            ext_type_event:get_node_id(Event),
                            fun(OldEventHistorySet) ->
                                add_event_history(EventHistory, OldEventHistorySet)
                            end,
                            add_event_history(EventHistory, new_event_history_set()),
                            AccInResultDict),
                    {add_event_history(EventHistory, AccInNoGroupSet), NewResultDict}
            end
        end,
        {new_event_history_set(), orddict:new()},
        EventHistorySet).

-spec generate_power_set_without_empty_set(ext_event_history_set()) ->
    ordsets:ordset(ext_event_history_set()).
generate_power_set_without_empty_set(EventHistorySet) ->
    ordsets:fold(
        fun(EventHistory, AccInInputEventHistoryPowerSet) ->
            PowerSetCur =
                ordsets:add_element(
                    ordsets:add_element(EventHistory, ordsets:new()), ordsets:new()),
            PowerSetCurAdded =
                ordsets:fold(
                    fun(CurEHSet, AccInPowerSetCurAdded) ->
                        ordsets:add_element(
                            ordsets:add_element(EventHistory, CurEHSet),
                            AccInPowerSetCurAdded)
                    end,
                    ordsets:new(),
                    AccInInputEventHistoryPowerSet),
            ordsets:union(
                AccInInputEventHistoryPowerSet,
                ordsets:union(PowerSetCur, PowerSetCurAdded))
        end,
        ordsets:new(),
        EventHistorySet).

-spec remove_group_event_history(ext_event_history_set()) -> ext_event_history_set().
remove_group_event_history(EventHistorySet) ->
    ordsets:fold(
        fun(EventHistory, AccInResultSet) ->
            case ext_type_event_history:get_event(EventHistory) of
                group_event ->
                    AccInResultSet;
                _ ->
                    ordsets:add_element(EventHistory, AccInResultSet)
            end
        end,
        ordsets:new(),
        EventHistorySet).

-spec set_size(ext_event_history_set()) -> non_neg_integer().
set_size(EventHistorySet) ->
    ordsets:size(EventHistorySet).

-spec build_path_append_dict_and_update(
    ext_node_id(),
    ext_path_info(),
    ext_event_history_set(),
    orddict:orddict(ext_type_path:ext_dataflow_path(), ext_type_path:ext_dataflow_path())) ->
    {
        orddict:orddict(ext_type_path:ext_dataflow_path(), ext_type_path:ext_dataflow_path()),
        ext_event_history_set()}.
build_path_append_dict_and_update(CurNodeId, CurPathInfo, EventHistorySet, PathDict) ->
    ordsets:fold(
        fun(EventHistory, {AccInPathDict, AccInResultEHSet}) ->
            {NewPathDict, NewEventHistory} =
                ext_type_event_history:build_path_append_dict_and_update(
                    CurNodeId, CurPathInfo, EventHistory, AccInPathDict),
            {NewPathDict, ordsets:add_element(NewEventHistory, AccInResultEHSet)}
        end,
        {PathDict, ordsets:new()},
        EventHistorySet).

-spec append_cur_node(
    ext_event_history_set(),
    orddict:orddict(ext_type_path:ext_dataflow_path(), ext_type_path:ext_dataflow_path())) ->
    ext_event_history_set().
append_cur_node(EventHistorySet, PathDict) ->
    ordsets:fold(
        fun(EventHistory, AccInResult) ->
            NewEventHistory = ext_type_event_history:append_cur_node(EventHistory, PathDict),
            ordsets:add_element(NewEventHistory, AccInResult)
        end,
        ordsets:new(),
        EventHistorySet).

-spec find_survived(ext_event_history_set(), ordsets:ordset(ext_type_event:ext_event())) ->
    ext_event_history_set().
find_survived(EventHistorySet, EventRemoved) ->
    ordsets:fold(
        fun(EventHistory, AccInResult) ->
            case ordsets:is_element(ext_type_event_history:get_event(EventHistory), EventRemoved) of
                true ->
                    AccInResult;
                false ->
                    add_event_history(EventHistory, AccInResult)
            end
        end,
        new_event_history_set(),
        EventHistorySet).

-spec subtract(ext_event_history_set(), ext_event_history_set()) -> ext_event_history_set().
subtract(EventHistorySetL, EventHistorySetR) ->
    ordsets:subtract(EventHistorySetL, EventHistorySetR).

-spec set_to_dict(ext_event_history_set()) -> {term(), term()}.
set_to_dict(EventHistoryAll) ->
    {ResultDict, GroupEncodeDict, _GroupCntDict} =
        ordsets:fold(
            fun({ext_event_history_partial_order_group, {NodeId, EventHistorySet}}=GroupEH,
                {AccInResultDict, AccInGroupEncodeDict, AccInGroupCntDict}) ->
                CurCnt =
                    case orddict:find(NodeId, AccInGroupCntDict) of
                        error ->
                            1;
                        {ok, PrevCnt} ->
                            PrevCnt + 1
                    end,
                CurGroupEH = {ext_event_history_partial_order_group, {NodeId, CurCnt}},
                NewAccInResultDict0 =
                    ordsets:fold(
                        fun(EHInGroup, AccInNewAccInResultDict0) ->
                            orddict:update(
                                EHInGroup,
                                fun(OldGroupSet) ->
                                    ordsets:add_element(CurGroupEH, OldGroupSet)
                                end,
                                ordsets:add_element(CurGroupEH, ordsets:new()),
                                AccInNewAccInResultDict0)
                        end,
                        AccInResultDict,
                        EventHistorySet),
                NewAccInResultDict =
                    orddict:store(CurGroupEH, ordsets:new(), NewAccInResultDict0),
                NewAccInGroupEncodeDict = orddict:store(GroupEH, CurGroupEH, AccInGroupEncodeDict),
                NewAccInGroupCntDict = orddict:update_counter(NodeId, 1, AccInGroupCntDict),
                {NewAccInResultDict, NewAccInGroupEncodeDict, NewAccInGroupCntDict};
            (EventHistory, {AccInResultDict, AccInGroupEncodeDict, AccInGroupCntDict}) ->
                NewAccInResultDict =
                    orddict:update(
                        EventHistory,
                        fun(OldGroupSet) ->
                            OldGroupSet
                        end,
                        ordsets:new(),
                        AccInResultDict),
                {NewAccInResultDict, AccInGroupEncodeDict, AccInGroupCntDict}
            end,
            {orddict:new(), orddict:new(), orddict:new()},
            EventHistoryAll),
    {ResultDict, GroupEncodeDict}.

-spec dict_to_set(term()) -> {ext_event_history_set(), term()}.
dict_to_set(EventHistoryAllDict) ->
    {NonGroupEventHistoryAll, GroupDecodeDict} =
        orddict:fold(
            fun({ext_event_history_partial_order_group, {_NodeId, _NodeCnt}}, [],
                {AccInNonGroupEventHistoryAll, AccInGroupDecodeDict}) ->
                {AccInNonGroupEventHistoryAll, AccInGroupDecodeDict};
            (EventHistory, GroupSet, {AccInNonGroupEventHistoryAll, AccInGroupDecodeDict}) ->
                NewAccInNonGroupEventHistoryAll =
                    ordsets:add_element(EventHistory, AccInNonGroupEventHistoryAll),
                NewAccInGroupDict =
                    ordsets:fold(
                        fun({ext_event_history_partial_order_group, {NodeId, _NodeCnt}}=GroupEH,
                            AccInNewAccInGroupDict) ->
                            orddict:update(
                                GroupEH,
                                fun({ext_event_history_partial_order_group, {OldNodeId, OldEHSet}}) ->
                                    {
                                        ext_event_history_partial_order_group,
                                        {
                                            OldNodeId,
                                            ext_type_event_history_set:add_event_history(
                                                EventHistory, OldEHSet)}}
                                end,
                                {
                                    ext_event_history_partial_order_group,
                                    {
                                        NodeId,
                                        ext_type_event_history_set:add_event_history(
                                            EventHistory,
                                            ext_type_event_history_set:new_event_history_set())}},
                                AccInNewAccInGroupDict)
                        end,
                        AccInGroupDecodeDict,
                        GroupSet),
                {NewAccInNonGroupEventHistoryAll, NewAccInGroupDict}
            end,
            {ordsets:new(), orddict:new()},
            EventHistoryAllDict),
    EventHistoryAll =
        orddict:fold(
            fun(_GroupEHEnc, GroupEHDec, AccInEventHistoryAll) ->
                ordsets:add_element(GroupEHDec, AccInEventHistoryAll)
            end,
            NonGroupEventHistoryAll,
            GroupDecodeDict),
    {EventHistoryAll, GroupDecodeDict}.

-spec encode_set(ext_event_history_set(), term()) -> ext_event_history_set().
encode_set(EventHistorySetDec, GroupEncodeDict) ->
    ordsets:fold(
        fun({ext_event_history_partial_order_group, {_NodeId, _EHSet}}=GroupEHDec, AccInResult) ->
            GroupEHEnc = orddict:fetch(GroupEHDec, GroupEncodeDict),
            ordsets:add_element(GroupEHEnc, AccInResult);
        (EventHistory, AccInResult) ->
            ordsets:add_element(EventHistory, AccInResult)
        end,
        ordsets:new(),
        EventHistorySetDec).

-spec decode_set(ext_event_history_set(), term()) -> ext_event_history_set().
decode_set(EventHistorySetEnc, GroupdecodeDict) ->
    ordsets:fold(
        fun({ext_event_history_partial_order_group, {_NodeId, _EHSet}}=GroupEHEnc, AccInResult) ->
            GroupEHDec = orddict:fetch(GroupEHEnc, GroupdecodeDict),
            ordsets:add_element(GroupEHDec, AccInResult);
        (EventHistory, AccInResult) ->
                ordsets:add_element(EventHistory, AccInResult)
        end,
        ordsets:new(),
        EventHistorySetEnc).

-spec append_cur_node_enc(ext_node_id(), ext_path_info(), ext_event_history_set()) ->
    ext_event_history_set().
append_cur_node_enc(NodeId, PathInfo, EventHistorySetEnc) ->
    ordsets:fold(
        fun(EventHistory, AccInResult) ->
            NewEventHistory =
                case ext_type_event_history:get_event(EventHistory) of
                    group_event ->
                        EventHistory;
                    _ ->
                        ext_type_event_history:append_cur_node(NodeId, PathInfo, EventHistory)
                end,
            ordsets:add_element(NewEventHistory, AccInResult)
        end,
        ordsets:new(),
        EventHistorySetEnc).

-spec from_list([ext_type_event_history:ext_event_history()]) -> ext_event_history_set().
from_list(EventHistoryList) ->
    ordsets:from_list(EventHistoryList).

-spec join_event_history_set(ext_event_history_set(), ext_event_history_set()) ->
    ext_event_history_set().
join_event_history_set(EventHistorySetL, EventHistorySetR) ->
    ordsets:union(EventHistorySetL, EventHistorySetR).

%% @private
max_event_history_set(AccInMaxEventHistorySet, []) ->
    AccInMaxEventHistorySet;
max_event_history_set(AccInMaxEventHistorySet, [H | T]=_InputEventHistorySet) ->
    case is_found_dominant_event_history(H, ordsets:union(AccInMaxEventHistorySet, T)) of
        true ->
            max_event_history_set(AccInMaxEventHistorySet, T);
        false ->
            max_event_history_set(
                ordsets:add_element(H, AccInMaxEventHistorySet), T)
    end.

%% @private
is_orderly_subset_internal([], _EventHistorySetR) ->
    true;
is_orderly_subset_internal([H | T]=_EventHistorySetL, EventHistorySetR) ->
    case is_found_dominant_event_history(H, EventHistorySetR) of
        false ->
            false;
        true ->
            is_orderly_subset_internal(T, EventHistorySetR)
    end.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

generate_power_set_without_empty_set_test() ->
    InputSet0 = ordsets:from_list([<<"eventhistory1">>]),
    ExpectedOutput0 = ordsets:from_list([ordsets:from_list([<<"eventhistory1">>])]),
    ?assertEqual(
        ExpectedOutput0,
        ?MODULE:generate_power_set_without_empty_set(InputSet0)),

    InputSet1 = ordsets:from_list([<<"eventhistory1">>, <<"eventhistory2">>]),
    ExpectedOutput1 =
        ordsets:from_list(
            [
                ordsets:from_list([<<"eventhistory1">>]),
                ordsets:from_list([<<"eventhistory2">>]),
                ordsets:from_list([<<"eventhistory1">>, <<"eventhistory2">>])]),
    ?assertEqual(
        ExpectedOutput1,
        ?MODULE:generate_power_set_without_empty_set(InputSet1)),

    InputSet2 = ordsets:from_list([<<"eventhistory1">>, <<"eventhistory2">>, <<"eventhistory3">>]),
    ExpectedOutput2 =
        ordsets:from_list(
            [
                ordsets:from_list([<<"eventhistory1">>]),
                ordsets:from_list([<<"eventhistory2">>]),
                ordsets:from_list([<<"eventhistory3">>]),
                ordsets:from_list([<<"eventhistory1">>, <<"eventhistory2">>]),
                ordsets:from_list([<<"eventhistory1">>, <<"eventhistory3">>]),
                ordsets:from_list([<<"eventhistory2">>, <<"eventhistory3">>]),
                ordsets:from_list([<<"eventhistory1">>, <<"eventhistory2">>, <<"eventhistory3">>])]),
    ?assertEqual(
        ExpectedOutput2,
        ?MODULE:generate_power_set_without_empty_set(InputSet2)).

-endif.
