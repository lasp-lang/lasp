%% -*- coding: utf-8 -*-
%%
%% @doc A Sum C-CRDT: for counting things!
%%
%% @author Christopher Meiklejohn <christopher.meiklejohn@gmail.com>
%%
%% @reference David Navalho, Sérgio Duarte, Nuno Preguiça, Marc Shapiro
%% (2013) Incremental Stream Processing with Computational Conflict-free
%% Replicated Data Types [http://asc.di.fct.unl.pt/~nmp/pubs/clouddp-2013.pdf]
%%
%% @end

-module(riak_dt_sum).
-behaviour(riak_dt).

-export([new/0, new/2, value/1, value/2,
         update/3, merge/2, merge_delta/2, equal/2, to_binary/1, from_binary/1, stats/1, stat/2]).
-export([to_binary/2, from_binary/2, current_version/1, change_versions/3]).
-export([parent_clock/2, update/4]).

-export_type([sum/0, sum_op/0]).

-opaque sum()         :: [{Actor::riak_dt:actor(),
                           Inc::pos_integer(), Dec::pos_integer()}].
-type sum_op()        :: add_op() | subtract_op().
-type add_op()        :: add | {add, pos_integer()}.
-type subtract_op()   :: subtract | {subtract, pos_integer()}.
-type sum_q()         :: additions | subtractions.
-type sum_binary()    :: <<_:16,_:_*8>>.

-type version()       :: 1.

%% @doc Create a new, empty `sum()'
-spec new() -> sum().
new() ->
    [].

%% @doc Create a `sum()' with an initial `Value' for `Actor'.
-spec new(term(), integer()) -> sum().
new(Actor, Value) when Value > 0 ->
    update({add, Value}, Actor, new());
new(Actor, Value) when Value < 0 ->
    update({subtract, Value * -1}, Actor, new());
new(_Actor, _Zero) ->
    new().

%% @doc no-op
-spec parent_clock(riak_dt_vclock:vclock(), sum()) -> sum().
parent_clock(_Clock, Cntr) ->
    Cntr.

%% @doc The single, total value of a `sum()'
-spec value(sum()) -> integer().
value(Sum) ->
    lists:sum([Inc - Dec || {_Act,Inc,Dec} <- Sum]).

%% @doc query the parts of a `sum()'
%% valid queries are `positive' or `negative'.
-spec value(sum_q(), sum()) -> integer().
value(additions, Sum) ->
    lists:sum([Inc || {_Act,Inc,_Dec} <- Sum]);
value(subtractions, Sum) ->
    lists:sum([Dec || {_Act,_Inc,Dec} <- Sum]).

%% @doc Update a `sum()'. The first argument is either the atom
%% `add' or `subtract' or the two tuples `{add, pos_integer()}' or
%% `{subtract, pos_integer()}'. In the case of the former, the operation's amount
%% is `1'. Otherwise it is the value provided in the tuple's second element.
%% `Actor' is any term, and the 3rd argument is the `sum()' to update.
%%
%% returns the updated `sum()'
-spec update(sum_op(), riak_dt:actor() | riak_dt:dot(), sum()) -> {ok, sum()}.
update(Op, {Actor, _Cnt}, Sum) ->
    update(Op, Actor, Sum);
update(add, Actor, Sum) ->
    update({add, 1}, Actor, Sum);
update(subtract, Actor, Sum) ->
    update({subtract, 1}, Actor, Sum);
update({_IncrDecr, 0}, _Actor, Sum) ->
    {ok, Sum};
update({add, By}, Actor, Sum) when is_integer(By), By > 0 ->
    {ok, add_by(By, Actor, Sum)};
update({add, By}, Actor, Sum) when is_integer(By), By < 0 ->
    update({subtract, -By}, Actor, Sum);
update({subtract, By}, Actor, Sum) when is_integer(By), By > 0 ->
    {ok, subtract_by(By, Actor, Sum)}.

update(Op, Actor, Cntr, _Ctx) ->
    update(Op, Actor, Cntr).

%% @doc Merge two `sum()'s to a single `sum()'. This is the Least Upper Bound
%% function described in the literature.
-spec merge(sum(), sum()) -> sum().
merge(SumA, SumB) ->
    merge(SumA, SumB, []).

merge([],[],Acc) ->
    lists:reverse(Acc);
merge(LeftOver, [], Acc) ->
    lists:reverse(Acc,LeftOver);
merge([], RightOver, Acc) ->
    lists:reverse(Acc,RightOver);
merge([{Act,IncA,DecA}=ACntA|RestA],SumB, Acc) ->
    case lists:keytake(Act, 1, SumB) of
        {value, {Act,IncB,DecB}, ModSumB} ->
            ACntB = {Act,max(IncA,IncB),max(DecA,DecB)},
            merge(RestA, ModSumB, [ACntB|Acc]);
        false ->
            merge(RestA, SumB, [ACntA|Acc])
    end.

%% @doc Merge delta; two `sum()'s to a single `sum()'.
-spec merge_delta(sum(), sum()) -> sum().
merge_delta(SumA, SumB) ->
    merge_delta(SumA, SumB, []).

merge_delta([],[],Acc) ->
    lists:reverse(Acc);
merge_delta(LeftOver, [], Acc) ->
    lists:reverse(Acc,LeftOver);
merge_delta([], RightOver, Acc) ->
    lists:reverse(Acc,RightOver);
merge_delta([{Act,IncA,DecA}=ACntA|RestA],SumB, Acc) ->
    case lists:keytake(Act, 1, SumB) of
        {value, {Act,IncB,DecB}, ModSumB} ->
            ACntB = {Act,IncA+IncB,DecA+DecB},
            merge_delta(RestA, ModSumB, [ACntB|Acc]);
        false ->
            merge_delta(RestA, SumB, [ACntA|Acc])
    end.

%% @doc Are two `sum()'s structurally equal? This is not `value/1' equality.
%% Two counters might represent the total `-42', and not be `equal/2'. Equality here is
%% that both counters represent exactly the same information.
-spec equal(sum(), sum()) -> boolean().
equal(SumA, SumB) ->
    lists:sort(SumA) =:= lists:sort(SumB).

-spec stats(sum()) -> [{atom(), number()}].
stats(PNCounter) ->
    [{actor_count, stat(actor_count, PNCounter)}].

-spec stat(atom(), sum()) -> non_neg_integer() | undefined.
stat(actor_count, PNCounter) ->
    length(PNCounter);
stat(_, _) -> undefined.

-include_lib("riak_dt/include/riak_dt_tags.hrl").
-define(DT_SUM_TAG, 86).
-define(TAG, ?DT_SUM_TAG).
-define(V1_VERS, 1).

%% @doc Encode an effecient binary representation of `sum()'
-spec to_binary(sum()) -> sum_binary().
to_binary(Sum) ->
    to_binary(?V1_VERS, Sum).

%% @doc Decode a binary encoded PN-Counter
-spec from_binary(sum_binary()) -> sum().
from_binary(Binary = <<?TAG:8/integer, _/binary>>) ->
    from_binary(?V1_VERS, Binary).

-spec to_binary(version(), sum()) -> sum_binary().
to_binary(?V1_VERS, Sum) ->
    Version = current_version(Sum),
    V2 = change_versions(Version, ?V1_VERS, Sum),
    <<?TAG:8/integer, ?V1_VERS:8/integer, (riak_dt:to_binary(V2))/binary>>.

-spec from_binary(version(), binary()) -> sum().
from_binary(?V1_VERS, <<?TAG:8/integer, ?V1_VERS:8/integer, PNBin/binary>>) ->
    riak_dt:from_binary(PNBin).

-spec current_version(sum()) -> version().
current_version(Sum) when is_list(Sum) ->
    ?V1_VERS.

-spec change_versions(version(), version(), sum()) -> sum().
change_versions(Version, Version, Sum)  ->
    Sum.

% Priv
-spec add_by(pos_integer(), term(), sum()) -> sum().
add_by(Add, Actor, Sum) ->
    case lists:keytake(Actor, 1, Sum) of
        false ->
            [{Actor,Add,0}|Sum];
        {value, {Actor,Inc,Dec}, ModSum} ->
            [{Actor,Inc+Add,Dec}|ModSum]
    end.

-spec subtract_by(pos_integer(), term(), sum()) -> sum().
subtract_by(Subtract, Actor, Sum) ->
    case lists:keytake(Actor, 1, Sum) of
        false ->
            [{Actor,0,Subtract}|Sum];
        {value, {Actor,Inc,Dec}, ModSum} ->
            [{Actor,Inc,Dec+Subtract}|ModSum]
    end.
