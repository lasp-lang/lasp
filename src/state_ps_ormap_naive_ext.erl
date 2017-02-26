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

-module(state_ps_ormap_naive_ext).
-author("Junghun Yoo <junghun.yoo@cs.ox.ac.uk>").

-export([map/2,
         filter/2]).

map(Function,
    {state_ps_ormap_naive, {{{ValueType, SubValueType}, ValueProvenanceStore},
                            SubsetEventsSurvived,
                            {ev_set, AllEventsEV}}}) ->
    {NewValueProvenanceStore, NewSubsetEventsSurvived, {ev_set, NewAllEventsEV}} =
        orddict:fold(
            fun(Key,
                SubProvenanceStore,
                {AccInValueProvenanceStore,
                 AccInSubsetEventsSurvived,
                 {ev_set, AccInAllEventsEV}}) ->
                case state_ps_type:is_ps_type(ValueType) of
                    true ->
                        {ValueType, {MappedSubProvenanceStore,
                                     MappedSubsetEvents,
                                     {ev_set, MappedAllEvents}}} =
                            Function(
                                {ValueType, {SubProvenanceStore,
                                             AccInSubsetEventsSurvived,
                                             {ev_set, AccInAllEventsEV}}}),
                        {orddict:store(
                            Key,
                            MappedSubProvenanceStore,
                            AccInValueProvenanceStore),
                         MappedSubsetEvents,
                         {ev_set, MappedAllEvents}};
                    false ->
                        {AccInValueProvenanceStore,
                         AccInSubsetEventsSurvived,
                         AccInAllEventsEV}
                end
            end,
            {orddict:new(), SubsetEventsSurvived, {ev_set, AllEventsEV}},
            ValueProvenanceStore),
    {state_ps_ormap_naive, {{{ValueType, SubValueType}, NewValueProvenanceStore},
                            NewSubsetEventsSurvived,
                            {ev_set, NewAllEventsEV}}}.

filter(
    Function,
    {state_ps_ormap_naive, {{{ValueType, SubValueType}, ValueProvenanceStore},
                            SubsetEventsSurvived,
                            {ev_set, AllEventsEV}}}) ->
    NewValueProvenanceStore =
        orddict:fold(
            fun(Key,
                SubProvenanceStore,
                AccInValueProvenanceStore) ->
                case state_ps_type:is_ps_type(ValueType) of
                    true ->
                        FilteredResult =
                            Function(
                                {ValueType, {SubProvenanceStore,
                                             SubsetEventsSurvived,
                                             {ev_set, AllEventsEV}}}),
                        case FilteredResult of
                            true ->
                                orddict:store(
                                    Key,
                                    SubProvenanceStore,
                                    AccInValueProvenanceStore);
                            false ->
                                AccInValueProvenanceStore
                        end;
                    false ->
                        AccInValueProvenanceStore
                end
            end,
            orddict:new(),
            ValueProvenanceStore),
    {state_ps_ormap_naive, {{{ValueType, SubValueType}, NewValueProvenanceStore},
                            SubsetEventsSurvived,
                            {ev_set, AllEventsEV}}}.
