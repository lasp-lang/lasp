-module(ext_type_lwwregister_input).

-author("Junghun Yoo <junghun.yoo@cs.ox.ac.uk>").

-behaviour(type).
-behaviour(state_type).

-define(TYPE, ?MODULE).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([
    new/0,
    new/1,
    mutate/3,
    query/1,
    equal/2]).
-export([
    delta_mutate/3,
    merge/2,
    is_bottom/1,
    is_inflation/2,
    is_strict_inflation/2,
    irreducible_is_strict_inflation/2,
    digest/1,
    join_decomposition/1,
    delta/2,
    encode/2,
    decode/2]).
-export([
    query_ext/1,
    query_ext_consistent/1]).

-export_type([
    ext_type_lwwregister_input/0,
    ext_type_lwwregister_input_op/0]).

-opaque ext_type_lwwregister_input() :: {?TYPE, payload()}.
-type payload() ::
    {
        ext_type_orset_base:ext_node_type(),
        ext_type_path:ext_path_info_list(),
        ext_type_orset_base:ext_type_orset_base()}.
-type element() :: term().
-type ext_type_lwwregister_input_op() ::
    {write, element()}.

-spec new() -> ext_type_lwwregister_input().
new() ->
    erlang:error(not_implemented).

-spec new([term()]) -> ext_type_lwwregister_input().
new([AllPathInfoList, {undefined, undefined}=PrevNodeIdPair, NodeId]) ->
    CurPathInfo = ext_type_path:new_path_info(NodeId, PrevNodeIdPair),
    NewAllPathInfoList = [CurPathInfo] ++ AllPathInfoList,
    {?TYPE, {input, NewAllPathInfoList, do(new, [NewAllPathInfoList])}}.

-spec mutate(
    ext_type_lwwregister_input_op(), type:id(), ext_type_lwwregister_input()) ->
    {ok, ext_type_lwwregister_input()}.
mutate(
    {write, Elem},
    {NodeId, ReplicaId}=_Actor,
    {?TYPE, {input, AllPathInfoList, ORSetBase}=_LWWRegisterPayload}) ->
    NextEventHistory =
        do(
            next_event_history,
            [ext_event_history_total_order, NodeId, ReplicaId, ORSetBase]),
    NewORSetBase = do(insert, [NextEventHistory, Elem, ORSetBase]),
    {ok, {?TYPE, {input, AllPathInfoList, NewORSetBase}}};
mutate(_Op, _Actor, {?TYPE, _LWWRegisterPayload}=CRDT) ->
    {ok, CRDT}.

-spec query(ext_type_lwwregister_input()) -> sets:set(element()).
query({?TYPE, {input, _AllPathInfoList, ORSetBase}=_LWWRegisterPayload}) ->
    {_Subset, Result} = do(read, [ordsets:new(), ORSetBase]),
    Result.

-spec equal(ext_type_lwwregister_input(), ext_type_lwwregister_input()) -> boolean().
equal(
    {?TYPE, {NodeTypeL, AllPathInfoListL, ORSetBaseL}=_LWWRegisterPayloadL},
    {?TYPE, {NodeTypeR, AllPathInfoListR, ORSetBaseR}=_LWWRegisterPayloadR}) ->
    NodeTypeL == NodeTypeR andalso
        AllPathInfoListL == AllPathInfoListR andalso
        do(equal, [ORSetBaseL, ORSetBaseR]).

-spec delta_mutate(
    ext_type_lwwregister_input_op(), type:id(), ext_type_lwwregister_input()) ->
    {ok, ext_type_lwwregister_input()}.
delta_mutate(_Op, _Actor, {?TYPE, _LWWRegisterPayload}=CRDT) ->
    {ok, CRDT}.

-spec merge(ext_type_lwwregister_input(), ext_type_lwwregister_input()) -> ext_type_lwwregister_input().
merge(
    {?TYPE, LWWRegisterPayload},
    {?TYPE, LWWRegisterPayload}) ->
    {?TYPE, LWWRegisterPayload};
merge(
    {?TYPE, {input, AllPathInfoList, ORSetBaseL}=_LWWRegisterPayloadL},
    {?TYPE, {input, AllPathInfoList, ORSetBaseR}=_LWWRegisterPayloadR}) ->
    NewORSetBase = do(join, [input, ORSetBaseL, ORSetBaseR]),
    {?TYPE, {input, AllPathInfoList, NewORSetBase}}.

-spec is_bottom(ext_type_lwwregister_input()) -> boolean().
is_bottom({?TYPE, {input, AllPathInfoList, ORSetBase}=_LWWRegisterPayload}=_CRDT) ->
    ORSetBase == do(new, [AllPathInfoList]);
is_bottom(_CRDT) ->
    false.

-spec is_inflation(ext_type_lwwregister_input(), ext_type_lwwregister_input()) -> boolean().
is_inflation(
    {?TYPE, {input, AllPathInfoList, ORSetBaseL}}, {?TYPE, {input, AllPathInfoList, ORSetBaseR}}) ->
    do(is_inflation, [ORSetBaseL, ORSetBaseR]);
is_inflation(_CRDTL, _CRDTR) ->
    false.

-spec is_strict_inflation(ext_type_lwwregister_input(), ext_type_lwwregister_input()) -> boolean().
is_strict_inflation(
    {?TYPE, {input, AllPathInfoList, ORSetBaseL}}, {?TYPE, {input, AllPathInfoList, ORSetBaseR}}) ->
    do(is_strict_inflation, [ORSetBaseL, ORSetBaseR]);
is_strict_inflation(_CRDTL, _CRDTR) ->
    false.

irreducible_is_strict_inflation(_Arg0, _Arg1) ->
    erlang:error(not_implemented).

digest(_Arg0) ->
    erlang:error(not_implemented).

join_decomposition(_Arg0) ->
    erlang:error(not_implemented).

delta(_Arg0, _Arg1) ->
    erlang:error(not_implemented).

encode(_Arg0, _Arg1) ->
    erlang:error(not_implemented).

decode(_Arg0, _Arg1) ->
    erlang:error(not_implemented).

-spec query_ext(
    {ext_type_cover:ext_subset_in_cover(), ext_type_lwwregister_input()}) ->
    {ext_type_cover:ext_subset_in_cover(), sets:set(element())}.
query_ext({PrevSubset, {?TYPE, {input, _AllPathInfoList, ORSetBase}}}) ->
    do(read, [PrevSubset, ORSetBase]).

-spec query_ext_consistent(
    {
        ext_type_cover:ext_subset_in_cover(),
        ext_type_provenance:ext_dot_set(),
        ext_type_lwwregister_input()}) ->
    {ext_type_cover:ext_subset_in_cover(), ext_type_provenance:ext_dot_set(), sets:set(element())}.
query_ext_consistent(
    {PrevSubset, PrevCDS, {?TYPE, {input, AllPathInfoList, ORSetBase}}}) ->
    do(consistent_read, [AllPathInfoList, PrevSubset, PrevCDS, ORSetBase]).

%% @private
do(Function, Args) ->
    ORSetBase = lasp_config:get(ext_type_version, ext_type_orset_base_v1),
    erlang:apply(ORSetBase, Function, Args).
