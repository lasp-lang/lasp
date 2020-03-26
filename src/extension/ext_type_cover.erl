-module(ext_type_cover).

-author("Junghun Yoo <junghun.yoo@cs.ox.ac.uk>").

%% API
-export([
    new_subset_in_cover/0,
    new_subset_in_cover/1,
    new_cover/0,
    new_cover/1,
    add_subset/2,
    find_all_super_subsets/2,
    append_cur_node/3,
    select_subset/1,
    prune_subset/2]).
-export([
    append_cur_node/2]).
-export([
    new_cover_from_list/1,
    find_all_sub_subsets/2,
    select_subset_reverse/2,
    generate_cover/2]).

-export_type([
    ext_subset_in_cover/0,
    ext_cover/0]).

-type ext_subset_in_cover() :: ext_type_event_history_set:ext_event_history_set().
-type ext_cover() :: ordsets:ordset(ext_subset_in_cover()).
-type ext_node_id() :: term().
-type ext_path_info() :: term().

-spec new_subset_in_cover() -> ext_subset_in_cover().
new_subset_in_cover() ->
    ext_type_event_history_set:new_event_history_set().

-spec new_subset_in_cover(ext_type_event_history:ext_event_history()) -> ext_subset_in_cover().
new_subset_in_cover(EventHistory) ->
    ext_type_event_history_set:add_event_history(
        EventHistory, ext_type_event_history_set:new_event_history_set()).

-spec new_cover() -> ext_cover().
new_cover() ->
    ordsets:new().

-spec new_cover(ext_subset_in_cover()) -> ext_cover().
new_cover(SubsetInCover) ->
    ordsets:add_element(SubsetInCover, ordsets:new()).

-spec add_subset(ext_subset_in_cover(), ext_cover()) -> ext_cover().
add_subset(SubsetInCover, Cover) ->
    ordsets:add_element(SubsetInCover, Cover).

-spec find_all_super_subsets(ext_subset_in_cover(), ext_cover()) ->
    ordsets:ordset(ext_subset_in_cover()).
find_all_super_subsets(Subset, Cover) ->
    find_all_super_subsets_internal(ordsets:new(), Subset, Cover).

-spec append_cur_node(ext_node_id(), ext_path_info(), ext_subset_in_cover()) ->
    ext_subset_in_cover().
append_cur_node(CurNodeId, CurPathInfo, SubsetInCover) ->
    ext_type_event_history_set:append_cur_node(CurNodeId, CurPathInfo, SubsetInCover).

-spec select_subset(ordsets:ordset(ext_subset_in_cover())) -> ext_subset_in_cover().
select_subset(Subsets) ->
    ordsets:fold(
        fun(Subset, AccInResult) ->
            case ext_type_event_history_set:set_size(Subset) >=
                ext_type_event_history_set:set_size(AccInResult) of
                true ->
                    Subset;
                false ->
                    AccInResult
            end
        end,
        new_subset_in_cover(),
        Subsets).

-spec prune_subset(ext_subset_in_cover(), ext_type_event_history_set:ext_event_history_set()) ->
    ext_subset_in_cover().
prune_subset(Subset, EventHistorySet) ->
    ordsets:intersection(Subset, EventHistorySet).

-spec append_cur_node(
    ext_subset_in_cover(),
    orddict:orddict(ext_type_path:ext_dataflow_path(), ext_type_path:ext_dataflow_path())) ->
    ext_subset_in_cover().
append_cur_node(SubsetInCover, PathDict) ->
    ext_type_event_history_set:append_cur_node(SubsetInCover, PathDict).

-spec new_cover_from_list([ext_subset_in_cover()]) -> ext_cover().
new_cover_from_list(SubsetList) ->
    ordsets:from_list(SubsetList).

-spec find_all_sub_subsets(ext_subset_in_cover(), ext_cover()) ->
    ordsets:ordset(ext_subset_in_cover()).
find_all_sub_subsets(Subset, Cover) ->
    find_all_sub_subsets_internal(ordsets:new(), Subset, Cover).

-spec select_subset_reverse(
    ordsets:ordset(ext_subset_in_cover()), ext_type_event_history_set:ext_event_history_set()) ->
    ext_subset_in_cover().
select_subset_reverse(Subsets, EventHistoryAll) ->
    ordsets:fold(
        fun(Subset, AccInResult) ->
            case ext_type_event_history_set:set_size(Subset) =<
                ext_type_event_history_set:set_size(AccInResult) of
                true ->
                    Subset;
                false ->
                    AccInResult
            end
        end,
        EventHistoryAll,
        Subsets).

-spec generate_cover([ext_subset_in_cover()], ext_type_event_history_set:ext_event_history_set()) ->
    ext_cover().
generate_cover(SubsetUnknownList, EventHistoryAll) ->
    lists:foldl(
        fun(SubsetUnknown, AccInResult) ->
            ordsets:add_element(
                ext_type_event_history_set:subtract(EventHistoryAll, SubsetUnknown), AccInResult)
        end,
        ordsets:new(),
        SubsetUnknownList).

%% @private
find_all_super_subsets_internal(AccSupersets, _Subset, []) ->
    AccSupersets;
find_all_super_subsets_internal(AccSupersets, Subset, [H | T]=_Cover) ->
    case ext_type_event_history_set:is_orderly_subset(Subset, H) of
        true ->
            find_all_super_subsets_internal(
                ordsets:add_element(H, AccSupersets), Subset, T);
        false ->
            find_all_super_subsets_internal(AccSupersets, Subset, T)
    end.

%% @private
find_all_sub_subsets_internal(AccSupersets, _Subset, []) ->
    AccSupersets;
find_all_sub_subsets_internal(AccSupersets, Subset, [H | T]=_Cover) ->
    case ext_type_event_history_set:is_orderly_subset(H, Subset) of
        true ->
            find_all_sub_subsets_internal(
                ordsets:add_element(H, AccSupersets), Subset, T);
        false ->
            find_all_sub_subsets_internal(AccSupersets, Subset, T)
    end.
