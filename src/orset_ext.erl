-module(orset_ext).

-export([intersect/2,
         map/2,
         union/2,
         product/2,
         filter/2]).

-spec union(orset:orset(), orset:orset()) -> orset:orset().
union(LValue, RValue) ->
    orset:merge(LValue, RValue).

-spec product(orset:orset(), orset:orset()) -> orset:orset().
product({orset, LValue}, {orset, RValue}) ->
    FolderFun = fun({X, XCausality}, {orset, Acc}) ->
        {orset, Acc ++ [{{X, Y}, causal_product(XCausality, YCausality)} || {Y, YCausality} <- RValue]}
    end,
    lists:foldl(FolderFun, new(), LValue).

-spec intersect(orset:orset(), orset:orset()) -> orset:orset().
intersect({orset, LValue}, RValue) ->
    lists:foldl(intersect_folder(RValue), new(), LValue).

%% @private
intersect_folder({orset, RValue}) ->
    fun({X, XCausality}, {orset, Acc}) ->
            Values = case lists:keyfind(X, 1, RValue) of
                         {_Y, YCausality} ->
                             [{X, causal_union(XCausality, YCausality)}];
                         false ->
                             []
                     end,
            {orset, Acc ++ Values}
    end.

-spec map(fun(), orset:orset()) -> orset:orset().
map(Function, {orset, V}) ->
    FolderFun = fun({X, Causality}, {orset, Acc}) ->
                        {orset, Acc ++ [{Function(X), Causality}]}
                end,
    lists:foldl(FolderFun, new(), V).

-spec filter(fun((_) -> boolean()), orset:orset()) -> orset:orset().
filter(Function, {orset, V}) ->
    FolderFun = fun({X, Causality}, {orset, Acc}) ->
                        case Function(X) of
                            true ->
                                {orset, Acc ++ [{X, Causality}]};
                            false ->
                                {orset, Acc}
                        end
                end,
    lists:foldl(FolderFun, new(), V).

%% @private
new() ->
    orset:new().

%% @private
causal_product(Xs, Ys) ->
    lists:foldl(fun({X, XDeleted}, XAcc) ->
                lists:foldl(fun({Y, YDeleted}, YAcc) ->
                            [{[X, Y], XDeleted orelse YDeleted}] ++ YAcc
                    end, [], Ys) ++ XAcc
        end, [], Xs).

%% @private
causal_union(Xs, Ys) ->
        Xs ++ Ys.
