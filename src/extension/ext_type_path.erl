-module(ext_type_path).

-author("Junghun Yoo <junghun.yoo@cs.ox.ac.uk>").

%% API
-export([
    new_path_info/2,
    new_dataflow_path/1,
    append_cur_node/3,
    build_dataflow_path_dict/1]).
-export([
    dfpath_to_atom/1,
    atom_to_dfpath/1,
    build_atom_dataflow_path_dict/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([
    ext_path_info/0,
    ext_path_info_list/0,
    ext_dataflow_path/0]).

-define(SEPARATOR_LIST, "|").

-type ext_node_id() :: term().
-type ext_cur_node_id() :: ext_node_id().
-type ext_prev_node_id() ::
    undefined |
    ext_node_id().
-type ext_path_info() :: {ext_cur_node_id(), {ext_prev_node_id(), ext_prev_node_id()}}.
-type ext_path_info_list() :: [ext_path_info()].
-type ext_dataflow_path_root() :: ext_node_id().
-type ext_dataflow_path_child() ::
    undefined |
    unknown |
    ext_dataflow_path().
-type ext_dataflow_path() ::
    {ext_dataflow_path_root(), {ext_dataflow_path_child(), ext_dataflow_path_child()}} |
    atom().

-spec new_path_info(ext_node_id(), {ext_prev_node_id(), ext_prev_node_id()}) -> ext_path_info().
new_path_info(CurNodeId, {_PrevLeft, _PrevRight}=PrevNodeIdPair) ->
    {CurNodeId, PrevNodeIdPair}.

-spec new_dataflow_path(ext_node_id()) -> ext_dataflow_path().
new_dataflow_path(InputNodeId) ->
    {InputNodeId, {undefined, undefined}}.

-spec append_cur_node(ext_cur_node_id(), ext_path_info(), ext_dataflow_path()) -> ext_dataflow_path().
append_cur_node(CurNodeId, {CurNodeId, {PrevLeft, PrevRight}}=_CurPathInfo, {Root, _Children}=DFPath) ->
    case Root of
        PrevLeft ->
            case PrevRight of
                undefined ->
                    {CurNodeId, {DFPath, undefined}};
                _ ->
                    {CurNodeId, {DFPath, unknown}}
            end;
        PrevRight ->
            case PrevLeft of
                undefined ->
                    {CurNodeId, {undefined, DFPath}};
                _ ->
                    {CurNodeId, {unknown, DFPath}}
            end
    end.

-spec build_dataflow_path_dict([ext_path_info()]) ->
    orddict:orddict(ext_node_id(), ordsets:ordset(ext_dataflow_path())).
build_dataflow_path_dict(AllPathInfoList) ->
    % `AllPathInfoList' is well-ordered !!!
    {_DFPathSet, ResultDict} =
        lists:foldl(
            fun({CurNodeId, PrevNodeIdPair}=PathInfo, {AccInDFPathSet, AccInResultDict}) ->
                {NewAccInDFPathSet, NewAccInResultDict} =
                    case AccInDFPathSet of
                        [] ->
                            NewDFPathSet = tail_path_info([], PathInfo),
                            NewResultDict =
                                case PrevNodeIdPair of
                                    {undefined, undefined} ->
                                        orddict:store(CurNodeId, NewDFPathSet, AccInResultDict);
                                    _ ->
                                        AccInResultDict
                                end,
                            {NewDFPathSet, NewResultDict};
                        _ ->
                            ordsets:fold(
                                fun(DFPath, {AccInNewDFPathSet, AccInNewResultDict}) ->
                                    NewDFPathSet = tail_path_info(DFPath, PathInfo),
                                    NewResultDict =
                                        case PrevNodeIdPair == {undefined, undefined} andalso
                                            (not ordsets:is_element(DFPath, NewDFPathSet)) of
                                            true ->
                                                orddict:update(
                                                    CurNodeId,
                                                    fun(OldDFPathSet) ->
                                                        ordsets:union(OldDFPathSet, NewDFPathSet)
                                                    end,
                                                    NewDFPathSet,
                                                    AccInNewResultDict);
                                            false ->
                                                AccInNewResultDict
                                        end,
                                    {ordsets:union(AccInNewDFPathSet, NewDFPathSet), NewResultDict}
                                end,
                                {ordsets:new(), AccInResultDict},
                                AccInDFPathSet)
                    end,
                {NewAccInDFPathSet, NewAccInResultDict}
            end,
            {ordsets:new(), orddict:new()},
            AllPathInfoList),
    ResultDict.

-spec dfpath_to_atom(ext_dataflow_path()) -> ext_dataflow_path().
dfpath_to_atom(DFPath) ->
    StringDFPath = dfpath_to_string_internal(DFPath),
    erlang:list_to_atom(StringDFPath).

-spec atom_to_dfpath(ext_dataflow_path()) -> ext_dataflow_path().
atom_to_dfpath(AtomDFPath) ->
    StringDFPath = erlang:atom_to_list(AtomDFPath),
    TokensDFPath = string:tokens(StringDFPath, ?SEPARATOR_LIST),
    string_to_dfpath_internal(TokensDFPath).

-spec build_atom_dataflow_path_dict([ext_path_info()]) ->
    orddict:orddict(ext_node_id(), ordsets:ordset(ext_dataflow_path())).
build_atom_dataflow_path_dict(AllPathInfoList) ->
    DataflowPathDict = build_dataflow_path_dict(AllPathInfoList),
    orddict:fold(
        fun(NodeId, DFPathSet, AccInAtomDataflowPathDict) ->
            AtomDFPathSet =
                ordsets:fold(
                    fun(DFPath, AccInAtomDFPathSet) ->
                        AtomDFPath = dfpath_to_atom(DFPath),
                        ordsets:add_element(AtomDFPath, AccInAtomDFPathSet)
                    end,
                    ordsets:new(),
                    DFPathSet),
            orddict:store(NodeId, AtomDFPathSet, AccInAtomDataflowPathDict)
        end,
        orddict:new(),
        DataflowPathDict).

%% @private
tail_path_info_internal([], Path) ->
    Path;
tail_path_info_internal(undefined, _Path) ->
    undefined;
tail_path_info_internal(unknown, _Path) ->
    unknown;
tail_path_info_internal({Root, {LeftChild, RightChild}}, Path) ->
    {
        Root,
        {
            tail_path_info_internal(LeftChild, Path),
            tail_path_info_internal(RightChild, Path)}};
tail_path_info_internal(ChildNodeId, {ChildNodeId, _PrevNodeIdPair}=Path) ->
    Path;
tail_path_info_internal(ChildNodeId, {_CurNodeId, _PrevNodeIdPair}=_Path) ->
    ChildNodeId.

%% @private
tail_path_info(DFPath, {_CurNodeId, {undefined, _PrevRight}}=PathInfo) ->
    ordsets:add_element(tail_path_info_internal(DFPath, PathInfo), ordsets:new());
tail_path_info(DFPath, {_CurNodeId, {_PrevLeft, undefined}}=PathInfo) ->
    ordsets:add_element(tail_path_info_internal(DFPath, PathInfo), ordsets:new());
tail_path_info(DFPath, {CurNodeId, {PrevLeft, PrevRight}}=_PathInfo) ->
    ordsets:add_element(
        tail_path_info_internal(DFPath, {CurNodeId, {PrevLeft, unknown}}),
        ordsets:add_element(
            tail_path_info_internal(DFPath, {CurNodeId, {unknown, PrevRight}}), ordsets:new())).

%% @private
dfpath_to_string_internal(undefined) ->
    erlang:atom_to_list(undefined);
dfpath_to_string_internal(unknown) ->
    erlang:atom_to_list(unknown);
dfpath_to_string_internal({CurNodeId, {PrevLeft, PrevRight}}) ->
    erlang:binary_to_list(CurNodeId) ++ ?SEPARATOR_LIST ++
        "(" ++ ?SEPARATOR_LIST ++ dfpath_to_string_internal(PrevLeft) ++ ?SEPARATOR_LIST ++
        ")" ++ ?SEPARATOR_LIST ++ "(" ++ ?SEPARATOR_LIST ++
        dfpath_to_string_internal(PrevRight) ++ ?SEPARATOR_LIST ++ ")".

%% @private
string_to_dfpath_internal(["undefined"]) ->
    undefined;
string_to_dfpath_internal(["unknown"]) ->
    unknown;
string_to_dfpath_internal(StringDFPath) ->
    [StringCurNodeId | StringListPrevNodeIdPair] = StringDFPath,
    {StringListPrevLeft, StringListPrevRight, _NumOfLeft, _DoneLeft} =
        lists:foldl(
            fun(String,
                {AccInStringListPrevLeft, AccInStringListPrevRight, AccInNumOfLeft, AccInDoneLeft}) ->
                case String of
                    "(" ->
                        case AccInNumOfLeft of
                            0 ->
                                {
                                    AccInStringListPrevLeft,
                                    AccInStringListPrevRight,
                                    AccInNumOfLeft + 1,
                                    AccInDoneLeft};
                            _ ->
                                case AccInDoneLeft of
                                    false ->
                                        {
                                            AccInStringListPrevLeft ++ [String],
                                            AccInStringListPrevRight,
                                            AccInNumOfLeft + 1,
                                            AccInDoneLeft};
                                    true ->
                                        {
                                            AccInStringListPrevLeft,
                                            AccInStringListPrevRight ++ [String],
                                            AccInNumOfLeft + 1,
                                            AccInDoneLeft}
                                end
                        end;
                    ")" ->
                        case AccInNumOfLeft of
                            1 ->
                                {
                                    AccInStringListPrevLeft,
                                    AccInStringListPrevRight,
                                    AccInNumOfLeft - 1,
                                    true};
                            _ ->
                                case AccInDoneLeft of
                                    false ->
                                        {
                                            AccInStringListPrevLeft ++ [String],
                                            AccInStringListPrevRight,
                                            AccInNumOfLeft - 1,
                                            AccInDoneLeft};
                                    true ->
                                        {
                                            AccInStringListPrevLeft,
                                            AccInStringListPrevRight ++ [String],
                                            AccInNumOfLeft - 1,
                                            AccInDoneLeft}
                                end
                        end;
                    _ ->
                        case AccInDoneLeft of
                            false ->
                                {
                                    AccInStringListPrevLeft ++ [String],
                                    AccInStringListPrevRight,
                                    AccInNumOfLeft,
                                    AccInDoneLeft};
                            true ->
                                {
                                    AccInStringListPrevLeft,
                                    AccInStringListPrevRight ++ [String],
                                    AccInNumOfLeft,
                                    AccInDoneLeft}
                        end
                end
            end,
            {[], [], 0, false},
            StringListPrevNodeIdPair),
    {
        erlang:list_to_binary(StringCurNodeId),
        {
            string_to_dfpath_internal(StringListPrevLeft),
            string_to_dfpath_internal(StringListPrevRight)}}.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

tail_path_info_leaf_test() ->
    DFPath = {<<"node1">>, {undefined, undefined}},

    PathInfo0 = {<<"node2">>, {undefined, undefined}},
    ?assertEqual(
        ordsets:from_list([DFPath]),
        tail_path_info(DFPath, PathInfo0)),

    PathInfo1 = {<<"node1">>, {undefined, undefined}},
    ?assertEqual(
        ordsets:from_list([DFPath]),
        tail_path_info(DFPath, PathInfo1)),

    PathInfo2 = {<<"node3">>, {<<"node1">>, undefined}},
    ?assertEqual(
        ordsets:from_list([DFPath]),
        tail_path_info(DFPath, PathInfo2)),

    PathInfo3 = {<<"node3">>, {<<"node1">>, <<"node2">>}},
    ?assertEqual(
        ordsets:from_list([DFPath]),
        tail_path_info(DFPath, PathInfo3)).

tail_path_info_single_left_child_test() ->
    DFPath = {<<"node1">>, {<<"node2">>, undefined}},

    PathInfo0 = {<<"node2">>, {undefined, undefined}},
    ?assertEqual(
        ordsets:from_list([{<<"node1">>, {{<<"node2">>, {undefined, undefined}}, undefined}}]),
        tail_path_info(DFPath, PathInfo0)),

    PathInfo1 = {<<"node3">>, {undefined, undefined}},
    ?assertEqual(
        ordsets:from_list([DFPath]),
        tail_path_info(DFPath, PathInfo1)),

    PathInfo2 = {<<"node2">>, {<<"node3">>, <<"node4">>}},
    ?assertEqual(
        ordsets:from_list(
            [
                {<<"node1">>, {{<<"node2">>, {<<"node3">>, unknown}}, undefined}},
                {<<"node1">>, {{<<"node2">>, {unknown, <<"node4">>}}, undefined}}]),
        tail_path_info(DFPath, PathInfo2)),

    PathInfo3 = {<<"node3">>, {<<"node4">>, <<"node5">>}},
    ?assertEqual(
        ordsets:from_list([DFPath]),
        tail_path_info(DFPath, PathInfo3)).

build_dataflow_path_dict_test() ->
    AllPathInfoList0 =
        [{<<"node1">>, {undefined, undefined}}],

    ExpectedOutput0 =
        orddict:from_list(
            [
                {
                    <<"node1">>,
                    ordsets:from_list(
                        [{<<"node1">>, {undefined, undefined}}])}]),

    ?assertEqual(
        ExpectedOutput0,
        ?MODULE:build_dataflow_path_dict(AllPathInfoList0)),

    AllPathInfoList1 =
        [
            {<<"node4">>, {<<"node1">>, <<"node2">>}},
            {<<"node2">>, {undefined, undefined}},
            {<<"node1">>, {undefined, undefined}}],

    ExpectedOutput1 =
        orddict:from_list(
            [
                {
                    <<"node1">>,
                    ordsets:from_list(
                        [{<<"node4">>, {{<<"node1">>, {undefined, undefined}}, unknown}}])},
                {
                    <<"node2">>,
                    ordsets:from_list(
                        [{<<"node4">>, {unknown, {<<"node2">>, {undefined, undefined}}}}])}]),

    ?assertEqual(
        ExpectedOutput1,
        ?MODULE:build_dataflow_path_dict(AllPathInfoList1)),

    AllPathInfoList2 =
        [
            {<<"node6">>, {<<"node4">>, <<"node5">>}},
            {<<"node5">>, {<<"node2">>, <<"node3">>}},
            {<<"node4">>, {<<"node1">>, <<"node2">>}},
            {<<"node3">>, {undefined, undefined}},
            {<<"node2">>, {undefined, undefined}},
            {<<"node1">>, {undefined, undefined}}],

    ExpectedOutput2 =
        orddict:from_list(
            [
                {
                    <<"node1">>,
                    ordsets:from_list(
                        [{<<"node6">>, {{<<"node4">>, {{<<"node1">>, {undefined, undefined}}, unknown}}, unknown}}])},
                {
                    <<"node2">>,
                    ordsets:from_list(
                        [
                            {<<"node6">>, {{<<"node4">>, {unknown, {<<"node2">>, {undefined, undefined}}}}, unknown}},
                            {<<"node6">>, {unknown, {<<"node5">>, {{<<"node2">>, {undefined, undefined}}, unknown}}}}])},
                {
                    <<"node3">>,
                    ordsets:from_list(
                        [{<<"node6">>, {unknown, {<<"node5">>, {unknown, {<<"node3">>, {undefined, undefined}}}}}}])}]),

    ?assertEqual(
        ExpectedOutput2,
        ?MODULE:build_dataflow_path_dict(AllPathInfoList2)).

dfpath_to_atom_test() ->
    DFPath0 = {<<"node1">>, {undefined, undefined}},
    AtomDFPath0 = 'node1|(|undefined|)|(|undefined|)',

    ?assertEqual(AtomDFPath0, ?MODULE:dfpath_to_atom(DFPath0)),

    ?assertEqual(DFPath0, ?MODULE:atom_to_dfpath(AtomDFPath0)),

    DFPath1 = {<<"node4">>, {{<<"node1">>, {undefined, undefined}}, unknown}},
    AtomDFPath1 = 'node4|(|node1|(|undefined|)|(|undefined|)|)|(|unknown|)',

    ?assertEqual(AtomDFPath1, ?MODULE:dfpath_to_atom(DFPath1)),

    ?assertEqual(DFPath1, ?MODULE:atom_to_dfpath(AtomDFPath1)),

    DFPath2 = {<<"node6">>, {unknown, {<<"node5">>, {unknown, {<<"node3">>, {undefined, undefined}}}}}},
    AtomDFPath2 = 'node6|(|unknown|)|(|node5|(|unknown|)|(|node3|(|undefined|)|(|undefined|)|)|)',

    ?assertEqual(AtomDFPath2, ?MODULE:dfpath_to_atom(DFPath2)),

    ?assertEqual(DFPath2, ?MODULE:atom_to_dfpath(AtomDFPath2)).

-endif.
