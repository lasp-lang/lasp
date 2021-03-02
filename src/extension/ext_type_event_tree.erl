-module(ext_type_event_tree).

-author("Junghun Yoo <junghun.yoo@cs.ox.ac.uk>").

%% API
-export([
    get_event_tree_leaf_set/1,
    get_node_id_from_event_tree/1,
    is_found_dominant_event_tree/2,
    get_next_event_history/4,
    is_related_to_tree_leaf/2,
    is_valid_event_tree_agg/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([
    ext_event_tree_leaf/0,
    ext_event_tree/0]).

-type ext_node_id() :: term().
-type ext_replica_id() :: term().
-type ext_event_content() :: {ext_replica_id(), pos_integer()}.
-type ext_event_id() ::
    {ext_type_event_history:ext_event_history_type(), ordsets:ordset(ext_event_content())}.
-type ext_event_tree_leaf() :: {ext_node_id(), ext_event_id()}.
-type ext_event_tree() ::
    unknown |
    undefined |
    ext_event_tree_leaf() |
    {ext_node_id(), ext_event_tree(), ext_event_tree()}.

-spec get_event_tree_leaf_set(ext_event_tree()) -> ordsets:ordset(ext_event_tree_leaf()).
get_event_tree_leaf_set(unknown) ->
    ordsets:new();
get_event_tree_leaf_set(undefined) ->
    ordsets:new();
get_event_tree_leaf_set({_NodeId, _EventId}=EventTreeLeaf) ->
    ordsets:add_element(EventTreeLeaf, ordsets:new());
get_event_tree_leaf_set({_NodeId, ChildLeft, ChildRight}) ->
    ordsets:union(
        get_event_tree_leaf_set(ChildLeft), get_event_tree_leaf_set(ChildRight)).

-spec get_node_id_from_event_tree(ext_event_tree()) -> ext_node_id().
get_node_id_from_event_tree({NodeId, _ChildLeft, _ChildRight}) ->
    NodeId;
get_node_id_from_event_tree({NodeId, _EventId}) ->
    NodeId;
get_node_id_from_event_tree(_) ->
    undefined.

-spec is_found_dominant_event_tree(ext_event_tree(), ordsets:ordset(ext_event_tree())) -> boolean().
is_found_dominant_event_tree(_EventTree, []) ->
    false;
is_found_dominant_event_tree(EventTree, [H | T]=_EventTreeSet) ->
    case EventTree /= H andalso is_related_to_event_tree(EventTree, H) of
        true ->
            true;
        false ->
            is_found_dominant_event_tree(EventTree, T)
    end.

-spec get_next_event_history(
    ext_type_event_history:ext_event_history_type(),
    ext_node_id(),
    ext_replica_id(),
    ordsets:ordset(ext_event_tree())) -> ext_event_tree_leaf().
get_next_event_history(
    ext_event_history_partial_order_independent, NodeId, ReplicaId, EventTreeLeafSet) ->
    MaxCnt =
        ordsets:fold(
            fun({_NId, {ext_event_history_partial_order_independent, [{RId, Cnt}]}}=_EventTreeLeaf,
                AccInMaxCnt) ->
                case RId == ReplicaId of
                    true ->
                        max(AccInMaxCnt, Cnt);
                    false ->
                        AccInMaxCnt
                end
            end,
            0,
            EventTreeLeafSet),
    {
        NodeId,
        {
            ext_event_history_partial_order_independent,
            ordsets:add_element({ReplicaId, MaxCnt + 1}, ordsets:new())}};
get_next_event_history(ext_event_history_total_order, NodeId, ReplicaId, EventTreeLeafSet) ->
    MaxCnt =
        ordsets:fold(
            fun({_NId, {ext_event_history_total_order, [{_RId, Cnt}]}}=_EventTreeLeaf,
                AccInMaxCnt) ->
                max(AccInMaxCnt, Cnt)
            end,
            0,
            EventTreeLeafSet),
    {
        NodeId,
        {
            ext_event_history_total_order,
            ordsets:add_element({ReplicaId, MaxCnt + 1}, ordsets:new())}}.

-spec is_related_to_tree_leaf(ext_event_tree_leaf(), ext_event_tree_leaf()) -> boolean().
is_related_to_tree_leaf({NodeId, EventIdL}=_EventTreeLeafL, {NodeId, EventIdR}=_EventTreeLeafR) ->
    is_related_to_event_id(EventIdL, EventIdR);
is_related_to_tree_leaf(_EventTreeLeafL, _EventTreeLeafR) ->
    false.

-spec is_valid_event_tree_agg(ext_event_tree()) -> boolean().
is_valid_event_tree_agg(EventTree) ->
    EventTreeAggDict = get_event_tree_agg_dict(EventTree),
    (not orddict:is_empty(EventTreeAggDict)) andalso
        is_valid_event_tree_agg_internal(EventTreeAggDict).

%% @private
is_related_to_event_tree(unknown, unknown) ->
    true;
is_related_to_event_tree(undefined, undefined) ->
    true;
is_related_to_event_tree({NodeId, EventIdL}, {NodeId, EventIdR}) ->
    is_related_to_event_id(EventIdL, EventIdR);
is_related_to_event_tree(
    {NodeId, ChildLeftL, ChildRightL}, {NodeId, ChildLeftR, ChildRightR}) ->
    is_related_to_event_tree(ChildLeftL, ChildLeftR) andalso
        is_related_to_event_tree(ChildRightL, ChildRightR);
is_related_to_event_tree(_EventTreeL, _EventTreeR) ->
    false.

%% @private
is_related_to_event_id(
    {ext_event_history_partial_order_independent, [EventContentL]},
    {ext_event_history_partial_order_independent, [EventContentR]}) ->
    EventContentL == EventContentR;
is_related_to_event_id(
    {ext_event_history_total_order, [{RIdL, CntL}]},
    {ext_event_history_total_order, [{RIdR, CntR}]}) ->
    CntL < CntR orelse (CntL == CntR andalso RIdL =< RIdR);
is_related_to_event_id(
    {ext_event_history_partial_order_group, EventIdSetL},
    {ext_event_history_partial_order_group, EventIdSetR}) ->
    ordsets:is_subset(EventIdSetL, EventIdSetR);
is_related_to_event_id(_EventTreeLeafL, _EventTreeLeafR) ->
    false.

%% @private
get_event_tree_agg_dict(EventTree) ->
    EventTreeLeafSet = get_event_tree_leaf_set(EventTree),
    ordsets:fold(
        fun({NodeId, {EventType, EventContent}}=_EventTreeLeaf, AccInResultDict) ->
            case EventType of
                ext_event_history_partial_order_group ->
                    orddict:update(
                        NodeId,
                        fun(OldContentSet) ->
                            ordsets:add_element(EventContent, OldContentSet)
                        end,
                        ordsets:add_element(EventContent, ordsets:new()),
                        AccInResultDict);
                _ ->
                    AccInResultDict
            end
        end,
        orddict:new(),
        EventTreeLeafSet).

%% @private
is_valid_event_tree_agg_internal([]) ->
    true;
is_valid_event_tree_agg_internal([{_NodeId, ContentSet} | T]=_EventTreeAggDict) ->
    case ordsets:size(ContentSet) == 1 of
        false ->
            false;
        true ->
            is_valid_event_tree_agg_internal(T)
    end.
