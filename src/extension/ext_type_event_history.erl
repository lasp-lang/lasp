-module(ext_type_event_history).

-author("Junghun Yoo <junghun.yoo@cs.ox.ac.uk>").

%% API
-export([
    next_event_history/4,
    new_group_event_history/2,
    is_related_to/2,
    append_cur_node/3,
    get_event/1,
    replace_dataflow_path/2]).
-export([
    event_history_to_atom/1,
    build_path_append_dict_and_update/4,
    append_cur_node/2]).

-export_type([
    ext_event_history_type/0,
    ext_event_history/0]).

-type ext_node_id() :: term().
-type ext_replica_id() :: term().
-type ext_path_info() :: term().
-type ext_event_history_type() ::
    ext_event_history_bottom |
    ext_event_history_partial_order_independent |
    ext_event_history_partial_order_group |
    ext_event_history_partial_order_downward_closed |
    ext_event_history_total_order.
-type ext_event_history() ::
    {ext_event_history_type(), {ext_type_event:ext_event(), ext_type_path:ext_dataflow_path()}} |
    {
        ext_event_history_partial_order_group,
        {ext_node_id(), ext_type_event_history_set:ext_event_history_set()}}.

-spec next_event_history(
    ext_event_history_type(),
    ext_node_id(),
    ext_replica_id(),
    ext_type_event_history_set:ext_event_history_set()) -> ext_event_history().
next_event_history(
    ext_event_history_partial_order_independent,
    NodeId,
    ReplicaId,
    AllEventHistory) ->
    MaxCnt =
        ordsets:fold(
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
            (
                {ext_event_history_partial_order_independent, {Event, _DFPath}=_EventHistoryContents},
                AccInMaxCnt) when erlang:is_atom(Event) ->
                {ENodeId, EReplicaId, ECount}=_RealEvent =
                    ext_type_event:atom_to_event(Event),
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
            AllEventHistory),
    {
        ext_event_history_partial_order_independent,
        {
            ext_type_event:new_event(NodeId, ReplicaId, MaxCnt + 1),
            ext_type_path:new_dataflow_path(NodeId)}};
next_event_history(
    ext_event_history_total_order,
    NodeId,
    ReplicaId,
    AllEventHistory) ->
    MaxCnt =
        ordsets:fold(
            fun(
                {
                    ext_event_history_total_order,
                    {{ENodeId, _EReplicaId, ECount}=_Event, _DFPath}=_EventHistoryContents},
                AccInMaxCnt) ->
                case ENodeId == NodeId of
                    true ->
                        max(AccInMaxCnt, ECount);
                    false ->
                        AccInMaxCnt
                end;
            (
                {ext_event_history_total_order, {Event, _DFPath}=_EventHistoryContents},
                AccInMaxCnt) when erlang:is_atom(Event) ->
                {ENodeId, _EReplicaId, ECount}=_RealEvent =
                    ext_type_event:atom_to_event(Event),
                case ENodeId == NodeId of
                    true ->
                        max(AccInMaxCnt, ECount);
                    false ->
                        AccInMaxCnt
                end;
            ({_EventHistoryTYpe, _EventHistoryContents}, AccInMaxCnt) ->
                AccInMaxCnt
            end,
            0,
            AllEventHistory),
    {
        ext_event_history_total_order,
        {
            ext_type_event:new_event(NodeId, ReplicaId, MaxCnt + 1),
            ext_type_path:new_dataflow_path(NodeId)}}.

-spec new_group_event_history(ext_node_id(), ext_type_event_history_set:ext_event_history_set()) ->
    ext_event_history().
new_group_event_history(NodeId, EventHistorySet) ->
    {ext_event_history_partial_order_group, {NodeId, EventHistorySet}}.

-spec is_related_to(ext_event_history(), ext_event_history()) -> boolean().
is_related_to(
    {ext_event_history_bottom, _EventHistoryContents}=_EventHistoryL,
    _EventHistoryR) ->
    true;
is_related_to(
    {ext_event_history_partial_order_independent, _EventHistoryContentsL}=EventHistoryL,
    {ext_event_history_partial_order_independent, _EventHistoryContentsR}=EventHistoryR) ->
    EventHistoryL == EventHistoryR;
is_related_to(
    {
        ext_event_history_partial_order_group,
        {NodeId, EventHistorySetL}}=_EventHistoryL,
    {
        ext_event_history_partial_order_group,
        {NodeId, EventHistorySetR}}=_EventHistoryR) ->
    ext_type_event_history_set:is_orderly_subset(EventHistorySetL, EventHistorySetR);
is_related_to(
    {
        ext_event_history_total_order,
        {{ENodeId, EReplicaIdL, ECountL}, DFPath}}=_EventHistoryL,
    {
        ext_event_history_total_order,
        {{ENodeId, EReplicaIdR, ECountR}, DFPath}}=_EventHistoryR) ->
    ECountL < ECountR orelse
        (ECountL == ECountR andalso EReplicaIdL =< EReplicaIdR);
is_related_to(
    {
        ext_event_history_total_order,
        {EventL, DFPath}}=_EventHistoryL,
    {
        ext_event_history_total_order,
        {EventR, DFPath}}=_EventHistoryR) when erlang:is_atom(EventL) andalso erlang:is_atom(EventR) ->
    {ENodeIdL, EReplicaIdL, ECountL}=_RealEventL = ext_type_event:atom_to_event(EventL),
    {ENodeIdR, EReplicaIdR, ECountR}=_RealEventR = ext_type_event:atom_to_event(EventR),
    ENodeIdL == ENodeIdR andalso
        (ECountL < ECountR orelse
            (ECountL == ECountR andalso EReplicaIdL =< EReplicaIdR));
is_related_to(_EventHistoryL, _EventHistoryR) ->
    false.

-spec append_cur_node(ext_node_id(), ext_path_info(), ext_event_history()) ->
    ext_event_history().
append_cur_node(
    CurNodeId,
    CurPathInfo,
    {ext_event_history_partial_order_group, {NodeId, EventHistorySet}}=_EventHistory) ->
    NewEventHistorySet =
        ext_type_event_history_set:append_cur_node(CurNodeId, CurPathInfo, EventHistorySet),
    {ext_event_history_partial_order_group, {NodeId, NewEventHistorySet}};
append_cur_node(CurNodeId, CurPathInfo, {EventType, {Event, DFPath}}=_EventHistory) ->
    NewDFPath = ext_type_path:append_cur_node(CurNodeId, CurPathInfo, DFPath),
    {EventType, {Event, NewDFPath}}.

-spec get_event(ext_event_history()) -> ext_type_event:ext_event() | group_event.
get_event({ext_event_history_partial_order_group, {_NodeId, _EventHistorySet}}) ->
    group_event;
get_event({_EventType, {Event, _DFPath}}) ->
    Event.

-spec replace_dataflow_path(ext_type_path:ext_dataflow_path(), ext_event_history()) ->
    ext_event_history().
replace_dataflow_path(NewDFPath, {EventType, {Event, _OldDFPath}}=_EventHistory) ->
    {EventType, {Event, NewDFPath}}.

-spec event_history_to_atom(ext_event_history()) -> ext_event_history().
event_history_to_atom(
    {ext_event_history_partial_order_group, {_NodeId, _EventHistorySet}}=EventHistory) ->
    EventHistory;
event_history_to_atom({EventType, {Event, DFPath}}=_EventHistory) ->
    {
        EventType,
        {ext_type_event:event_to_atom(Event), ext_type_path:dfpath_to_atom(DFPath)}}.

-spec build_path_append_dict_and_update(
    ext_node_id(),
    ext_path_info(),
    ext_event_history(),
    orddict:orddict(ext_type_path:ext_dataflow_path(), ext_type_path:ext_dataflow_path())) ->
    {
        orddict:orddict(ext_type_path:ext_dataflow_path(), ext_type_path:ext_dataflow_path()),
        ext_event_history()}.
build_path_append_dict_and_update(
    CurNodeId,
    CurPathInfo,
    {ext_event_history_partial_order_group, {NodeId, EventHistorySet}}=_EventHistory,
    PathDict) ->
    {NewPathDict, NewEventHistorySet} =
        ext_type_event_history_set:build_path_append_dict_and_update(
            CurNodeId, CurPathInfo, EventHistorySet, PathDict),
    {NewPathDict, {ext_event_history_partial_order_group, {NodeId, NewEventHistorySet}}};
build_path_append_dict_and_update(
    CurNodeId, CurPathInfo, {EventType, {Event, DFPath}}=_EventHistory, PathDict) ->
    {NewPathDict, NewDFPath} =
        case orddict:find(DFPath, PathDict) of
            {ok, NewPath} ->
                {PathDict, NewPath};
            error ->
                RealDFPath = ext_type_path:atom_to_dfpath(DFPath),
                NewRealDFPath = ext_type_path:append_cur_node(CurNodeId, CurPathInfo, RealDFPath),
                AtomNewRealDFPath = ext_type_path:dfpath_to_atom(NewRealDFPath),
                {orddict:store(DFPath, AtomNewRealDFPath, PathDict), AtomNewRealDFPath}
        end,
    {NewPathDict, {EventType, {Event, NewDFPath}}}.

-spec append_cur_node(
    ext_event_history(),
    orddict:orddict(ext_type_path:ext_dataflow_path(), ext_type_path:ext_dataflow_path())) ->
    ext_event_history().
append_cur_node(
    {ext_event_history_partial_order_group, {NodeId, EventHistorySet}}=_EventHistory, PathDict) ->
    NewEventHistorySet =
        ext_type_event_history_set:append_cur_node(EventHistorySet, PathDict),
    {ext_event_history_partial_order_group, {NodeId, NewEventHistorySet}};
append_cur_node({EventType, {Event, DFPath}}=_EventHistory, PathDict) ->
    NewDFPath = orddict:fetch(DFPath, PathDict),
    {EventType, {Event, NewDFPath}}.
