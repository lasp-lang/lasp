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

-export([create/1]).

-record(state, {set_id}).

-define(DEFAULT, <<"hi">>).

%% @doc Create a SQL view from a textual specification.
create(Specification) when is_list(Specification) ->
    %% Tokenize the string.
    {ok, Tokens, _EndLine} = ?SQL_LEXER:string(Specification),

    %% Parse the tokens.
    {ok, ParseTree} = ?SQL_PARSER:parse(Tokens),

    ct:pal("Parse Tree: ~p", [ParseTree]),

    %% Create.
    {Result, _State} = materialize(ParseTree, #state{set_id=?DEFAULT}),

    ct:pal("Result: ~p", [Result]),

    ok.

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
        '>=' ->
             Var >= Element;
        '>' ->
             Var < Element;
        '<' ->
            Var < Element;
        '=<' ->
             Var =< Element;
        _ ->
            %% TODO
            {error, undefined}
    end.
