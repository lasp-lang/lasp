-module(ext_type_aworset_intermediate).

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
    ext_type_aworset_intermediate/0,
    ext_type_aworset_intermediate_op/0]).

-opaque ext_type_aworset_intermediate() :: {?TYPE, payload()}.
-type payload() ::
    {
        ext_type_orset_base:ext_node_type(),
        ext_type_path:ext_path_info_list(),
        ext_type_orset_base:ext_type_orset_base()}.
-type element() :: term().
-type ext_type_aworset_intermediate_op() :: no_op.

-spec new() -> ext_type_aworset_intermediate().
new() ->
    erlang:error(not_implemented).

-spec new([term()]) -> ext_type_aworset_intermediate().
new([AllPathInfoList, PrevNodeIdPair, NodeId]) ->
    CurPathInfo = ext_type_path:new_path_info(NodeId, PrevNodeIdPair),
    NewAllPathInfoList = [CurPathInfo] ++ AllPathInfoList,
    {?TYPE, {intermediate, NewAllPathInfoList, do(new, [NewAllPathInfoList])}}.

-spec mutate(
    ext_type_aworset_intermediate_op(), type:id(), ext_type_aworset_intermediate()) ->
    {ok, ext_type_aworset_intermediate()}.
mutate(no_op, _Actor, {?TYPE, _AWORSetPayload}=CRDT) ->
    {ok, CRDT}.

-spec query(ext_type_aworset_intermediate()) -> sets:set(element()).
query({?TYPE, {intermediate, _AllPathInfoList, ORSetBase}=_AWORSetPayload}) ->
    {_Subset, Result} = do(read, [ordsets:new(), ORSetBase]),
    Result.

-spec equal(ext_type_aworset_intermediate(), ext_type_aworset_intermediate()) -> boolean().
equal(
    {?TYPE, {NodeTypeL, AllPathInfoListL, ORSetBaseL}=_AWORSetPayloadL},
    {?TYPE, {NodeTypeR, AllPathInfoListR, ORSetBaseR}=_AWORSetPayloadR}) ->
    NodeTypeL == NodeTypeR andalso
        AllPathInfoListL == AllPathInfoListR andalso
        do(equal, [ORSetBaseL, ORSetBaseR]).

-spec delta_mutate(
    ext_type_aworset_intermediate_op(), type:id(), ext_type_aworset_intermediate()) ->
    {ok, ext_type_aworset_intermediate()}.
delta_mutate(_Op, _Actor, {?TYPE, _AWORSetPayload}=CRDT) ->
    {ok, CRDT}.

-spec merge(ext_type_aworset_intermediate(), ext_type_aworset_intermediate()) ->
    ext_type_aworset_intermediate().
merge(
    {?TYPE, AWORSetPayload},
    {?TYPE, AWORSetPayload}) ->
%%    lager:info("jeff Same payload"),
    {?TYPE, AWORSetPayload};
merge(
    {?TYPE, {intermediate, AllPathInfoList, ORSetBaseL}=_AWORSetPayloadL},
    {?TYPE, {intermediate, AllPathInfoList, ORSetBaseR}=_AWORSetPayloadR}) ->
    NewORSetBase = do(join, [intermediate, ORSetBaseL, ORSetBaseR]),
    {?TYPE, {intermediate, AllPathInfoList, NewORSetBase}}.

-spec is_bottom(ext_type_aworset_intermediate()) -> boolean().
is_bottom(
    {?TYPE, {intermediate, AllPathInfoList, ORSetBase}=_AWORSetPayload}=_CRDT) ->
    ORSetBase == do(new, [AllPathInfoList]);
is_bottom(_CRDT) ->
    false.

-spec is_inflation(ext_type_aworset_intermediate(), ext_type_aworset_intermediate()) -> boolean().
is_inflation(
    {?TYPE, {intermediate, AllPathInfoList, ORSetBaseL}=_AWORSetPayloadL},
    {?TYPE, {intermediate, AllPathInfoList, ORSetBaseR}=_AWORSetPayloadR}) ->
    do(is_inflation, [ORSetBaseL, ORSetBaseR]);
is_inflation(_CRDTL, _CRDTR) ->
    false.

-spec is_strict_inflation(ext_type_aworset_intermediate(), ext_type_aworset_intermediate()) ->
    boolean().
is_strict_inflation(
    {?TYPE, {intermediate, AllPathInfoList, ORSetBaseL}=_AWORSetPayloadL},
    {?TYPE, {intermediate, AllPathInfoList, ORSetBaseR}=_AWORSetPayloadR}) ->
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
    {ext_type_cover:ext_subset_in_cover(), ext_type_aworset_intermediate()}) ->
    {ext_type_cover:ext_subset_in_cover(), sets:set(element())}.
query_ext({PrevSubset, {?TYPE, {intermediate, _AllPathInfoList, ORSetBase}}}) ->
    do(read, [PrevSubset, ORSetBase]).

-spec query_ext_consistent(
    {
        ext_type_cover:ext_subset_in_cover(),
        ext_type_provenance:ext_dot_set(),
        ext_type_aworset_intermediate()}) ->
    {ext_type_cover:ext_subset_in_cover(), ext_type_provenance:ext_dot_set(), sets:set(element())}.
query_ext_consistent(
    {PrevSubset, PrevCDS, {?TYPE, {intermediate, AllPathInfoList, ORSetBase}}}) ->
    do(consistent_read, [AllPathInfoList, PrevSubset, PrevCDS, ORSetBase]).

%% @private
do(Function, Args) ->
    ORSetBase = lasp_config:get(ext_type_version, ext_type_orset_base_v1),
    erlang:apply(ORSetBase, Function, Args).
