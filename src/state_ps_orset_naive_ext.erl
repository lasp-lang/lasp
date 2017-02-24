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

-module(state_ps_orset_naive_ext).
-author("Junghun Yoo <junghun.yoo@cs.ox.ac.uk>").

-export([intersect/2,
         map/2,
         union/2,
         product/2,
         filter/2]).

union(LValue, RValue) ->
    state_ps_orset_naive:merge(LValue, RValue).

product({state_ps_orset_naive, {ProvenanceStoreL,
                                SubsetEventsSurvivedL,
                                {ev_set, AllEventsEVL}}=_ORSetL},
        {state_ps_orset_naive, {ProvenanceStoreR,
                                SubsetEventsSurvivedR,
                                {ev_set, AllEventsEVR}}=_ORSetR}) ->
    ProductAllEventsEV = {ev_set, ordsets:union(AllEventsEVL, AllEventsEVR)},
    ProductSubsetEventsSurvived =
        ordsets:union(
            [ordsets:intersection(
                SubsetEventsSurvivedL, SubsetEventsSurvivedR)] ++
            [ordsets:subtract(SubsetEventsSurvivedL, AllEventsEVR)] ++
            [ordsets:subtract(SubsetEventsSurvivedR, AllEventsEVL)]),
    ProductProvenanceStore =
        orddict:fold(
            fun(ElemL, ProvenanceL, AccInProductProvenanceStoreL) ->
                case ordsets:is_subset(
                    state_ps_type:get_events_from_provenance(ProvenanceL),
                    ProductSubsetEventsSurvived) of
                    false ->
                        AccInProductProvenanceStoreL;
                    true ->
                        orddict:fold(
                            fun(
                                ElemR,
                                ProvenanceR,
                                AccInProductProvenanceStoreR) ->
                                case ordsets:is_subset(
                                    state_ps_type:get_events_from_provenance(
                                            ProvenanceR),
                                    ProductSubsetEventsSurvived) of
                                    false ->
                                        AccInProductProvenanceStoreR;
                                    true ->
                                        ProductElem = {ElemL, ElemR},
                                        ProductProvenance =
                                            state_ps_type:cross_provenance(
                                                    ProvenanceL, ProvenanceR),
                                        orddict:store(
                                            ProductElem,
                                            ProductProvenance,
                                            AccInProductProvenanceStoreR)
                                end
                            end,
                            AccInProductProvenanceStoreL,
                            ProvenanceStoreR)
                end
            end,
            orddict:new(),
            ProvenanceStoreL),
    {state_ps_orset_naive, {ProductProvenanceStore,
                            ProductSubsetEventsSurvived,
                            ProductAllEventsEV}}.

intersect({state_ps_orset_naive, {ProvenanceStoreL,
                                  SubsetEventsSurvivedL,
                                  {ev_set, AllEventsEVL}}=_ORSetL},
          {state_ps_orset_naive, {ProvenanceStoreR,
                                  SubsetEventsSurvivedR,
                                  {ev_set, AllEventsEVR}}=_ORSetR}) ->
    IntersectAllEventsEV = {ev_set, ordsets:union(AllEventsEVL, AllEventsEVR)},
    IntersectSubsetEventsSurvived =
        ordsets:union(
            [ordsets:intersection(
                SubsetEventsSurvivedL, SubsetEventsSurvivedR)] ++
            [ordsets:subtract(SubsetEventsSurvivedL, AllEventsEVR)] ++
            [ordsets:subtract(SubsetEventsSurvivedR, AllEventsEVL)]),
    IntersectProvenanceStore =
        orddict:fold(
            fun(ElemL, ProvenanceL, AccInIntersectProvenanceStoreL) ->
                case ordsets:is_subset(
                    state_ps_type:get_events_from_provenance(ProvenanceL),
                    IntersectSubsetEventsSurvived) of
                    false ->
                        AccInIntersectProvenanceStoreL;
                    true ->
                        orddict:fold(
                            fun(
                                ElemR,
                                ProvenanceR,
                                AccInIntersectProvenanceStoreR) ->
                                case ordsets:is_subset(
                                    state_ps_type:get_events_from_provenance(
                                        ProvenanceR),
                                    IntersectSubsetEventsSurvived) of
                                    false ->
                                        AccInIntersectProvenanceStoreR;
                                    true ->
                                        case ElemL == ElemR of
                                            true ->
                                                ProductProvenance =
                                                    state_ps_type:cross_provenance(
                                                        ProvenanceL, ProvenanceR),
                                                orddict:store(
                                                    ElemL,
                                                    ProductProvenance,
                                                    AccInIntersectProvenanceStoreR);
                                            false ->
                                                AccInIntersectProvenanceStoreR
                                        end
                                end
                            end,
                            AccInIntersectProvenanceStoreL,
                            ProvenanceStoreR)
                end
            end,
            orddict:new(),
            ProvenanceStoreL),
    {state_ps_orset_naive, {IntersectProvenanceStore,
                            IntersectSubsetEventsSurvived,
                            IntersectAllEventsEV}}.

map(Function, {state_ps_orset_naive, {ProvenanceStore,
                                      SubsetEventsSurvived,
                                      {ev_set, AllEventsEV}}}) ->
    MapProvenanceStore =
        orddict:fold(
            fun(Elem, Provenance, AccInMapProvenanceStore) ->
                orddict:update(
                    Function(Elem),
                    fun(OldProvenance) ->
                        ordsets:union(OldProvenance, Provenance)
                    end,
                    Provenance,
                    AccInMapProvenanceStore)
            end, orddict:new(), ProvenanceStore),
    {state_ps_orset_naive, {MapProvenanceStore,
                            SubsetEventsSurvived,
                            {ev_set, AllEventsEV}}}.

filter(Function, {state_ps_orset_naive, {ProvenanceStore,
                                         SubsetEventsSurvived,
                                         {ev_set, AllEventsEV}}}) ->
    FilterProvenanceStore =
        orddict:fold(
            fun(Elem, Provenance, AccInFilterProvenanceStore) ->
                case Function(Elem) of
                    true ->
                        orddict:store(
                            Elem, Provenance, AccInFilterProvenanceStore);
                    false ->
                        AccInFilterProvenanceStore
                end
            end, orddict:new(), ProvenanceStore),
    {state_ps_orset_naive, {FilterProvenanceStore,
                            SubsetEventsSurvived,
                            {ev_set, AllEventsEV}}}.
