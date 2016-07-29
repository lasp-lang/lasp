%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Christopher Meiklejohn.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(lasp_sql_materialized_view).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("lasp.hrl").

-export([create/1,
         create/2,
         get_value/2,
         create_table_with_values/2]).

-record(state, {output_id, set_id, projection_output_id, predicate_output_id}).

-define(DEFAULT, <<"hi">>).

%% @doc Insert in a SQL table the given rows.
create_table_with_values(TableName, Args) when is_atom(TableName) ->
    {Name, Type} = generate_identifier(TableName),
    lasp:declare(Name, Type),
    lists:foreach(fun(Row) ->
        RowMap = maps:from_list(Row),
        lasp:update({Name, ?SET}, {add, RowMap}, a)
    end, Args).

%% @doc Given a SQL table and a list of rows, return the values of those rows.
get_value(TableName, Rows) ->
    Name = case is_atom(TableName) of
        true -> generate_identifier(TableName);
        _ -> TableName
    end,
    {ok, Set} = lasp:query(Name),
    lists:map(fun(M) ->
        lists:foldl(fun(Row, Acc) ->
            [maps:get(Row, M) | Acc]
        end, [], lists:reverse(Rows))
    end, sets:to_list(Set)).

%% @doc Create a SQL view from a textual specification.
create(Specification) when is_list(Specification) ->

    %% Create a node for the result of the predicate.
    {ok, {OutputId, _, _, _}} = lasp:declare(?SET),

    %% Create using a given output.
    create(OutputId, Specification).

%% @doc Create a SQL view from a textual specification.
create(OutputId, Specification) when is_list(Specification) ->

    %% Tokenize the string.
    {ok, Tokens, _EndLine} = ?SQL_LEXER:string(Specification),

    ct:pal("Tokens: ~p", [Tokens]),

    %% Parse the tokens.
    {ok, ParseTree} = ?SQL_PARSER:parse(Tokens),

    ct:pal("Parse Tree: ~p", [ParseTree]),

    %% Create and return identifier.
    OutputId = materialize(ParseTree, #state{output_id=OutputId}),

    %% Return output identifier.
    {ok, OutputId}.

%% Entry point to evaluation of the parse tree.
materialize({query, Projections, {from, Collection}, Predicates}, State0) ->
    %% Convert collection identifier to binary.
    CollectionId = generate_identifier(Collection),

    %% Materialize a dataflow graph for the predicate tree.
    {PredicateOutputId, State1} = materialize(Predicates,
                                              State0#state{set_id=CollectionId}),

    %% Materialize projections.
    {ProjectionOutputId, _State} = materialize(Projections,
                                               State1#state{predicate_output_id=PredicateOutputId}),

    %% Return the top node of the DAG.
    ProjectionOutputId;

materialize({select, Projections},
            #state{output_id=OutputId, predicate_output_id=PredicateOutputId}=State) ->

    %% Apply the projection.
    lasp:map(PredicateOutputId,
             fun(Tuple) -> extract(Projections, Tuple) end,
             OutputId),

    {OutputId, State};

materialize({where, Predicates}, State) ->
    materialize(Predicates, State);

materialize({intersection, Left, Right}, State0) ->
    %% Build predicates on the left.
    {LeftId, State1} = materialize(Left, State0),

    %% Build predicates on the right.
    {RightId, State} = materialize(Right, State1),

    %% Create a node for the result of the predicate.
    {ok, {OutputId, _, _, _}} = lasp:declare(?SET),

    %% Intersection.
    lasp:intersection(LeftId, RightId, OutputId),

    {OutputId, State};

materialize({union, Left, Right}, State0) ->
    %% Build predicates on the left.
    {LeftId, State1} = materialize(Left, State0),

    %% Build predicates on the right.
    {RightId, State} = materialize(Right, State1),

    %% Create a node for the result of the predicate.
    {ok, {OutputId, _, _, _}} = lasp:declare(?SET),

    %% Union.
    lasp:union(LeftId, RightId, OutputId),

    {OutputId, State};

%% Predicate handling.
materialize({predicate, {var, Variable}, Comparator, Element},
            #state{set_id=SetId}=State) ->
    %% Create a node for the result of the predicate.
    {ok, {OutputId, _, _, _}} = lasp:declare(?SET),

    %% Filter the source into the predicate.
    lasp:filter(SetId,
                fun(Tuple) -> comparator(Tuple, Variable, Comparator, Element) end,
                OutputId),

    {OutputId, State}.

comparator(Tuple, Variable, Comparator, Element) ->
    %% Select element out of map for comparison.
    Var = maps:get(Variable, Tuple),

    case Comparator of
        '=>' ->
             Var >= Element;
        '>' ->
             Var > Element;
        '<' ->
            Var < Element;
        '<=' ->
             Var =< Element;
        '=' ->
             Var =:= Element;
        _ ->
            exit({error, comparator_unsupported})
    end.

extract(Variables, Tuple) ->
    lists:foldl(fun(Variable, Map) ->
                        maps:put(Variable, maps:get(Variable, Tuple, undefined), Map)
                end, maps:new(), Variables).

generate_identifier(Id) when is_atom(Id) ->
    {list_to_binary(atom_to_list(Id)), ?SET}.
