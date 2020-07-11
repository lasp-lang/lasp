-module(ext_type_lasp_ops).

-author("Junghun Yoo <junghun.yoo@cs.ox.ac.uk>").

-export([
    map/3,
    filter/3,
    product/5]).

-export([
    set_count/2,
    group_by_sum/3,
    order_by/3]).

map({ObjectId, _ReplicaId}=ResultId,
    Function,
    {ext_type_aworset_input, {input, [H | _T]=AllPathInfoList, BORSet}}) ->
    {ChildLeft, _GrandChildrenPair} = H,
    ResultPathInfo = {ObjectId, {ChildLeft, undefined}},
    ResultAllPathInfoList = [ResultPathInfo] ++ AllPathInfoList,
    ResultBORSet = internal_map(ResultId, Function, ResultAllPathInfoList, ResultPathInfo, BORSet),
    {ext_type_aworset_intermediate, {intermediate, ResultAllPathInfoList, ResultBORSet}};
map({ObjectId, _ReplicaId}=ResultId,
    Function,
    {ext_type_aworset_intermediate, {intermediate, [H | _T]=AllPathInfoList, BORSet}}) ->
    {ChildLeft, _GrandChildrenPair} = H,
    ResultPathInfo = {ObjectId, {ChildLeft, undefined}},
    ResultAllPathInfoList = [ResultPathInfo] ++ AllPathInfoList,
    ResultBORSet = internal_map(ResultId, Function, ResultAllPathInfoList, ResultPathInfo, BORSet),
    {ext_type_aworset_intermediate, {intermediate, ResultAllPathInfoList, ResultBORSet}}.

filter(
    {ObjectId, _ReplicaId}=ResultId,
    Function,
    {ext_type_aworset_input, {input, [H | _T]=AllPathInfoList, BORSet}}) ->
    {ChildLeft, _GrandChildrenPair} = H,
    ResultPathInfo = {ObjectId, {ChildLeft, undefined}},
    ResultAllPathInfoList = [ResultPathInfo] ++ AllPathInfoList,
    ResultBORSet =
        internal_filter(ResultId, Function, ResultAllPathInfoList, ResultPathInfo, BORSet),
    {ext_type_aworset_intermediate, {intermediate, ResultAllPathInfoList, ResultBORSet}};
filter(
    {ObjectId, _ReplicaId}=ResultId,
    Function,
    {ext_type_aworset_intermediate, {intermediate, [H | _T]=AllPathInfoList, BORSet}}) ->
    {ChildLeft, _GrandChildrenPair} = H,
    ResultPathInfo = {ObjectId, {ChildLeft, undefined}},
    ResultAllPathInfoList = [ResultPathInfo] ++ AllPathInfoList,
    ResultBORSet =
        internal_filter(ResultId, Function, ResultAllPathInfoList, ResultPathInfo, BORSet),
    {ext_type_aworset_intermediate, {intermediate, ResultAllPathInfoList, ResultBORSet}}.

product(
    {ObjectId, _ReplicaId}=ResultId,
    _LeftId,
    _RightId,
    {ext_type_aworset_input, {input, [HL | _TL]=AllPathInfoListL, BORSetL}},
    {ext_type_aworset_input, {input, [HR | _TR]=AllPathInfoListR, BORSetR}}) ->
    {ChildLeft, _} = HL,
    {ChildRight, _} = HR,
    ResultPathInfo = {ObjectId, {ChildLeft, ChildRight}},
    ResultAllPathInfoList = [ResultPathInfo] ++ AllPathInfoListL ++ AllPathInfoListR,
    ResultBORSet =
        internal_product(ResultId, ResultAllPathInfoList, ResultPathInfo, BORSetL, BORSetR),
    {ext_type_aworset_intermediate, {intermediate, ResultAllPathInfoList, ResultBORSet}};
product(
    {ObjectId, _ReplicaId}=ResultId,
    _LeftId,
    _RightId,
    {ext_type_aworset_input, {input, [HL | _TL]=AllPathInfoListL, BORSetL}},
    {ext_type_aworset_intermediate, {intermediate, [HR | _TR]=AllPathInfoListR, BORSetR}}) ->
    {ChildLeft, _} = HL,
    {ChildRight, _} = HR,
    ResultPathInfo = {ObjectId, {ChildLeft, ChildRight}},
    ResultAllPathInfoList = [ResultPathInfo] ++ AllPathInfoListL ++ AllPathInfoListR,
    ResultBORSet =
        internal_product(ResultId, ResultAllPathInfoList, ResultPathInfo, BORSetL, BORSetR),
    {ext_type_aworset_intermediate, {intermediate, ResultAllPathInfoList, ResultBORSet}};
product(
    {ObjectId, _ReplicaId}=ResultId,
    _LeftId,
    _RightId,
    {ext_type_aworset_intermediate, {intermediate, [HL | _TL]=AllPathInfoListL, BORSetL}},
    {ext_type_aworset_input, {input, [HR | _TR]=AllPathInfoListR, BORSetR}}) ->
    {ChildLeft, _} = HL,
    {ChildRight, _} = HR,
    ResultPathInfo = {ObjectId, {ChildLeft, ChildRight}},
    ResultAllPathInfoList = [ResultPathInfo] ++ AllPathInfoListL ++ AllPathInfoListR,
    ResultBORSet =
        internal_product(ResultId, ResultAllPathInfoList, ResultPathInfo, BORSetL, BORSetR),
    {ext_type_aworset_intermediate, {intermediate, ResultAllPathInfoList, ResultBORSet}};
product(
    {ObjectId, _ReplicaId}=ResultId,
    _LeftId,
    _RightId,
    {ext_type_aworset_intermediate, {intermediate, [HL | _TL]=AllPathInfoListL, BORSetL}},
    {ext_type_aworset_intermediate, {intermediate, [HR | _TR]=AllPathInfoListR, BORSetR}}) ->
    {ChildLeft, _} = HL,
    {ChildRight, _} = HR,
    ResultPathInfo = {ObjectId, {ChildLeft, ChildRight}},
    ResultAllPathInfoList = [ResultPathInfo] ++ AllPathInfoListL ++ AllPathInfoListR,
    ResultBORSet =
        internal_product(ResultId, ResultAllPathInfoList, ResultPathInfo, BORSetL, BORSetR),
    {ext_type_aworset_intermediate, {intermediate, ResultAllPathInfoList, ResultBORSet}};
product(
    {ObjectId, _ReplicaId}=ResultId,
    _LeftId,
    _RightId,
    {ext_type_aggresult_intermediate, {intermediate, [HL | _TL]=AllPathInfoListL, BORSetL}},
    {ext_type_lwwregister_input, {input, [HR | _TR]=AllPathInfoListR, BORSetR}}) ->
    {ChildLeft, _} = HL,
    {ChildRight, _} = HR,
    ResultPathInfo = {ObjectId, {ChildLeft, ChildRight}},
    ResultAllPathInfoList = [ResultPathInfo] ++ AllPathInfoListL ++ AllPathInfoListR,
    ResultBORSet =
        internal_product(ResultId, ResultAllPathInfoList, ResultPathInfo, BORSetL, BORSetR),
    {ext_type_aworset_intermediate, {intermediate, ResultAllPathInfoList, ResultBORSet}};
product(
    {ObjectId, _ReplicaId}=ResultId,
    _LeftId,
    _RightId,
    {ext_type_aggresult_intermediate, {intermediate, [HL | _TL]=AllPathInfoListL, BORSetL}},
    {ext_type_aworset_intermediate, {intermediate, [HR | _TR]=AllPathInfoListR, BORSetR}}) ->
    {ChildLeft, _} = HL,
    {ChildRight, _} = HR,
    ResultPathInfo = {ObjectId, {ChildLeft, ChildRight}},
    ResultAllPathInfoList = [ResultPathInfo] ++ AllPathInfoListL ++ AllPathInfoListR,
    ResultBORSet =
        internal_product(ResultId, ResultAllPathInfoList, ResultPathInfo, BORSetL, BORSetR),
    {ext_type_aworset_intermediate, {intermediate, ResultAllPathInfoList, ResultBORSet}}.

set_count(
    {ObjectId, _ReplicaId}=ResultId,
    {ext_type_aworset_input, {input, [H | _T]=AllPathInfoList, BORSet}}) ->
    {ChildLeft, _GrandChildrenPair} = H,
    ResultPathInfo = {ObjectId, {ChildLeft, undefined}},
    ResultAllPathInfoList = [ResultPathInfo] ++ AllPathInfoList,
    ResultBORSet = internal_set_count(ResultId, ResultAllPathInfoList, ResultPathInfo, BORSet),
    {ext_type_aggresult_intermediate, {intermediate, ResultAllPathInfoList, ResultBORSet}};
set_count(
    {ObjectId, _ReplicaId}=ResultId,
    {ext_type_aworset_intermediate, {intermediate, [H | _T]=AllPathInfoList, BORSet}}) ->
    {ChildLeft, _GrandChildrenPair} = H,
    ResultPathInfo = {ObjectId, {ChildLeft, undefined}},
    ResultAllPathInfoList = [ResultPathInfo] ++ AllPathInfoList,
    ResultBORSet = internal_set_count(ResultId, ResultAllPathInfoList, ResultPathInfo, BORSet),
    {ext_type_aggresult_intermediate, {intermediate, ResultAllPathInfoList, ResultBORSet}}.

group_by_sum(
    {ObjectId, _ReplicaId}=ResultId,
    SumFunction,
    {ext_type_aworset_input, {input, [H | _T]=AllPathInfoList, BORSet}}) ->
    {ChildLeft, _GrandChildrenPair} = H,
    ResultPathInfo = {ObjectId, {ChildLeft, undefined}},
    ResultAllPathInfoList = [ResultPathInfo] ++ AllPathInfoList,
    ResultBORSet =
        internal_group_by_sum(ResultId, SumFunction, ResultAllPathInfoList, ResultPathInfo, BORSet),
    {ext_type_aggresult_intermediate, {intermediate, ResultAllPathInfoList, ResultBORSet}};
group_by_sum(
    {ObjectId, _ReplicaId}=ResultId,
    SumFunction,
    {ext_type_aworset_intermediate, {intermediate, [H | _T]=AllPathInfoList, BORSet}}) ->
    {ChildLeft, _GrandChildrenPair} = H,
    ResultPathInfo = {ObjectId, {ChildLeft, undefined}},
    ResultAllPathInfoList = [ResultPathInfo] ++ AllPathInfoList,
    ResultBORSet =
        internal_group_by_sum(ResultId, SumFunction, ResultAllPathInfoList, ResultPathInfo, BORSet),
    {ext_type_aggresult_intermediate, {intermediate, ResultAllPathInfoList, ResultBORSet}}.

order_by(
    {ObjectId, _ReplicaId}=ResultId,
    CompareFunction,
    {ext_type_aworset_input, {input, [H | _T]=AllPathInfoList, BORSet}}) ->
    {ChildLeft, _GrandChildrenPair} = H,
    ResultPathInfo = {ObjectId, {ChildLeft, undefined}},
    ResultAllPathInfoList = [ResultPathInfo] ++ AllPathInfoList,
    ResultBORSet =
        internal_order_by(ResultId, CompareFunction, ResultAllPathInfoList, ResultPathInfo, BORSet),
    {ext_type_aggresult_intermediate, {intermediate, ResultAllPathInfoList, ResultBORSet}};
order_by(
    {ObjectId, _ReplicaId}=ResultId,
    CompareFunction,
    {ext_type_aworset_intermediate, {intermediate, [H | _T]=AllPathInfoList, BORSet}}) ->
    {ChildLeft, _GrandChildrenPair} = H,
    ResultPathInfo = {ObjectId, {ChildLeft, undefined}},
    ResultAllPathInfoList = [ResultPathInfo] ++ AllPathInfoList,
    ResultBORSet =
        internal_order_by(ResultId, CompareFunction, ResultAllPathInfoList, ResultPathInfo, BORSet),
    {ext_type_aggresult_intermediate, {intermediate, ResultAllPathInfoList, ResultBORSet}};
order_by(
    {ObjectId, _ReplicaId}=ResultId,
    CompareFunction,
    {ext_type_aggresult_intermediate, {intermediate, [H | _T]=AllPathInfoList, BORSet}}) ->
    {ChildLeft, _GrandChildrenPair} = H,
    ResultPathInfo = {ObjectId, {ChildLeft, undefined}},
    ResultAllPathInfoList = [ResultPathInfo] ++ AllPathInfoList,
    ResultBORSet =
        internal_order_by(ResultId, CompareFunction, ResultAllPathInfoList, ResultPathInfo, BORSet),
    {ext_type_aggresult_intermediate, {intermediate, ResultAllPathInfoList, ResultBORSet}}.

%% @private
internal_map(ResultId, Function, ResultAllPathInfoList, PathInfo, BORSet) ->
    do(map, [ResultId, Function, ResultAllPathInfoList, PathInfo, BORSet]).

%% @private
internal_filter(ResultId, Function, ResultAllPathInfoList, PathInfo, BORSet) ->
    do(filter, [ResultId, Function, ResultAllPathInfoList, PathInfo, BORSet]).

%% @private
internal_product(ResultId, ResultAllPathInfoList, PathInfo, BORSetL, BORSetR) ->
    do(product, [ResultId, ResultAllPathInfoList, PathInfo, BORSetL, BORSetR]).

%% @private
internal_set_count(ResultId, AllPathInfoList, ResultPathInfo, BORSet) ->
    do(set_count, [ResultId, AllPathInfoList, ResultPathInfo, BORSet]).

%% @private
internal_group_by_sum(ResultId, SumFunction, AllPathInfoList, ResultPathInfo, BORSet) ->
    do(group_by_sum, [ResultId, SumFunction, AllPathInfoList, ResultPathInfo, BORSet]).

%% @private
internal_order_by(ResultId, CompareFunction, AllPathInfoList, ResultPathInfo, BORSet) ->
    do(order_by, [ResultId, CompareFunction, AllPathInfoList, ResultPathInfo, BORSet]).

%% @private
do(Function, Args) ->
    ORSetBase = lasp_config:get(ext_type_version, ext_type_orset_base_v1),
    erlang:apply(ORSetBase, Function, Args).
