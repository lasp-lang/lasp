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

-module(state_ps_type_ext).

-author("Junghun Yoo <junghun.yoo@cs.ox.ac.uk>").

-export([
    map/2,
    filter/2,
    union/2,
    product/2,
    length/2,
    singleton/2,
    unsingleton/1,
    group_by_first/2,
    threshold_met/2,
    threshold_met_strict/2]).

map(Function, {state_ps_aworset_naive, Payload}) ->
    NewPayload = map_internal(Function, Payload),
    {state_ps_aworset_naive, NewPayload};
map(Function, {state_ps_singleton_orset_naive, Payload}) ->
    NewPayload = map_internal(Function, Payload),
    {state_ps_singleton_orset_naive, NewPayload};
map(Function, {state_ps_group_by_orset_naive, Payload}) ->
    NewPayload = map_internal(Function, Payload),
    {state_ps_group_by_orset_naive, NewPayload}.

filter(Function, {state_ps_aworset_naive, Payload}) ->
    NewPayload = filter_internal(Function, Payload),
    {state_ps_aworset_naive, NewPayload}.

union(
    {state_ps_aworset_naive, PayloadL}, {state_ps_aworset_naive, PayloadR}) ->
    NewPayload = union_internal(PayloadL, PayloadR),
    {state_ps_aworset_naive, NewPayload}.

product(
    {state_ps_aworset_naive, PayloadL}, {state_ps_aworset_naive, PayloadR}) ->
    NewPayload = product_internal(PayloadL, PayloadR),
    {state_ps_aworset_naive, NewPayload};
product(
    {state_ps_aworset_naive, PayloadL}, {state_ps_size_t_naive, PayloadR}) ->
    NewPayload = product_internal(PayloadL, PayloadR),
    {state_ps_aworset_naive, NewPayload};
product(
    {state_ps_size_t_naive, PayloadL},
    {state_ps_lwwregister_naive, PayloadR}) ->
    NewPayload = product_internal(PayloadL, PayloadR),
    {state_ps_aworset_naive, NewPayload};
product(
    {state_ps_singleton_orset_naive, PayloadL},
    {state_ps_aworset_naive, PayloadR}) ->
    NewPayload = product_internal(PayloadL, PayloadR),
    {state_ps_aworset_naive, NewPayload}.

length(ObjectId, {state_ps_aworset_naive, PayloadR}) ->
    NewCRDT = length_internal(ObjectId, {state_ps_aworset_naive, PayloadR}),
    NewCRDT.

singleton(ObjectId, {state_ps_aworset_naive, PayloadR}) ->
    NewCRDT = singleton_internal(ObjectId, {state_ps_aworset_naive, PayloadR}),
    NewCRDT;
singleton(ObjectId, {state_ps_group_by_orset_naive, PayloadR}) ->
    NewCRDT =
        singleton_internal(ObjectId, {state_ps_group_by_orset_naive, PayloadR}),
    NewCRDT.

unsingleton({state_ps_singleton_orset_naive, PayloadR}) ->
    NewCRDT = unsingleton_internal({state_ps_singleton_orset_naive, PayloadR}),
    NewCRDT.

group_by_first(ObjectId, {state_ps_aworset_naive, PayloadR}) ->
    NewCRDT =
        group_by_first_internal(ObjectId, {state_ps_aworset_naive, PayloadR}),
    NewCRDT.

threshold_met(
    Threshold,
    {state_ps_size_t_naive, {ProvenanceStore, _, _}=_Payload}=CRDT) ->
    case orddict:size(ProvenanceStore) > 1 of
        false ->
            state_ps_size_t_naive:is_inflation(Threshold, CRDT);
        true ->
            false
    end;
threshold_met(
    Threshold,
    {state_ps_singleton_orset_naive, {ProvenanceStore, _, _}=_Payload}=CRDT) ->
    case orddict:size(ProvenanceStore) > 1 of
        false ->
            state_ps_singleton_orset_naive:is_inflation(Threshold, CRDT);
        true ->
            false
    end;
threshold_met(
    Threshold,
    {state_ps_group_by_orset_naive, {ProvenanceStore, _, _}=_Payload}=CRDT) ->
    {FoundDupGroup, _GroupSet} =
        orddict:fold(
            fun({Fst, _SndSet}, _Provenance, {AccFoundDupGroup, AccGroupSet}) ->
                case AccFoundDupGroup of
                    true ->
                        {true, ordsets:new()};
                    false ->
                        NewFoundDupGroup = ordsets:is_element(Fst, AccGroupSet),
                        NewGroupSet = ordsets:add_element(Fst, AccGroupSet),
                        {NewFoundDupGroup, NewGroupSet}
                end
            end,
            {false, ordsets:new()},
            ProvenanceStore),
    case FoundDupGroup of
        false ->
            state_ps_group_by_orset_naive:is_inflation(Threshold, CRDT);
        true ->
            false
    end.

threshold_met_strict(
    Threshold,
    {state_ps_size_t_naive, {ProvenanceStore, _, _}=_Payload}=CRDT) ->
    case orddict:size(ProvenanceStore) > 1 of
        false ->
            state_ps_size_t_naive:is_strict_inflation(Threshold, CRDT);
        true ->
            false
    end;
threshold_met_strict(
    Threshold,
    {state_ps_singleton_orset_naive, {ProvenanceStore, _, _}=_Payload}=CRDT) ->
    case orddict:size(ProvenanceStore) > 1 of
        false ->
            state_ps_singleton_orset_naive:is_strict_inflation(Threshold, CRDT);
        true ->
            false
    end;
threshold_met_strict(
    Threshold,
    {state_ps_group_by_orset_naive, {ProvenanceStore, _, _}=_Payload}=CRDT) ->
    {FoundDupGroup, _GroupSet} =
        orddict:fold(
            fun({Fst, _SndSet}, _Provenance, {AccFoundDupGroup, AccGroupSet}) ->
                case AccFoundDupGroup of
                    true ->
                        {true, ordsets:new()};
                    false ->
                        NewFoundDupGroup = ordsets:is_element(Fst, AccGroupSet),
                        NewGroupSet = ordsets:add_element(Fst, AccGroupSet),
                        {NewFoundDupGroup, NewGroupSet}
                end
            end,
            {false, ordsets:new()},
            ProvenanceStore),
    case FoundDupGroup of
        false ->
            state_ps_group_by_orset_naive:is_strict_inflation(Threshold, CRDT);
        true ->
            false
    end.

%% @private
map_internal(Function, {ProvenanceStore, SubsetEvents, AllEvents}=_POEORSet) ->
    MapProvenanceStore =
        orddict:fold(
            fun(Elem, Provenance, AccInMapProvenanceStore) ->
                orddict:update(
                    Function(Elem),
                    fun(OldProvenance) ->
                        state_ps_type:plus_provenance(OldProvenance, Provenance)
                    end,
                    Provenance,
                    AccInMapProvenanceStore)
            end, orddict:new(), ProvenanceStore),
    {MapProvenanceStore, SubsetEvents, AllEvents}.

%% @private
filter_internal(
    Function, {ProvenanceStore, SubsetEvents, AllEvents}=_POEORSet) ->
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
    {FilterProvenanceStore, SubsetEvents, AllEvents}.

%% @private
union_internal(POEORSetL, POEORSetR) ->
    state_ps_poe_orset:join(POEORSetL, POEORSetR).

%% @private
product_internal(
    {ProvenanceStoreL, SubsetEventsL, AllEventsL}=_POEORSetL,
    {ProvenanceStoreR, SubsetEventsR, AllEventsR}=_POEORSetR) ->
    ProductAllEvents = state_ps_type:join_all_events(AllEventsL, AllEventsR),
    ProductSubsetEvents =
        state_ps_type:join_subset_events(
            SubsetEventsL, AllEventsL, SubsetEventsR, AllEventsR),
    {CrossedProvenanceStore, NewEvents} =
        orddict:fold(
            fun(ElemL,
                ProvenanceL,
                {AccProductProvenanceStoreL, AccNewEventsL}) ->
                orddict:fold(
                    fun(ElemR,
                        ProvenanceR,
                        {AccProductProvenanceStoreLR, AccNewEventsLR}) ->
                        ProductElem = {ElemL, ElemR},
                        {ProductProvenance, ProductNewEvents} =
                            state_ps_type:cross_provenance(
                                ProvenanceL, ProvenanceR),
                        NewProductProvenanceStore =
                            orddict:store(
                                ProductElem,
                                ProductProvenance,
                                AccProductProvenanceStoreLR),
                        {NewProductProvenanceStore,
                            ordsets:union(AccNewEventsLR, ProductNewEvents)}
                    end,
                    {AccProductProvenanceStoreL, AccNewEventsL},
                    ProvenanceStoreR)
            end,
            {orddict:new(), ordsets:new()},
            ProvenanceStoreL),
    NewProductAllEvents =
        state_ps_type:event_set_max(
            state_ps_type:event_set_union(ProductAllEvents, NewEvents)),
    NewProductSubsetEvents =
        state_ps_type:event_set_max(
            state_ps_type:event_set_union(ProductSubsetEvents, NewEvents)),
    ProductProvenanceStore =
        prune_provenance_store(CrossedProvenanceStore, NewProductSubsetEvents),
    {ProductProvenanceStore, NewProductSubsetEvents, NewProductAllEvents}.

%% @private
prune_provenance_store(ProvenanceStore, Events) ->
    orddict:fold(
        fun(Elem, Provenance, AccPrunedProvenanceStore) ->
            NewProvenance =
                ordsets:fold(
                    fun(Dot, AccNewProvenance) ->
                        case ordsets:is_subset(Dot, Events) of
                            true ->
                                ordsets:add_element(Dot, AccNewProvenance);
                            false ->
                                AccNewProvenance
                        end
                    end,
                    ordsets:new(),
                    Provenance),
            case NewProvenance of
                [] ->
                    AccPrunedProvenanceStore;
                _ ->
                    orddict:store(Elem, NewProvenance, AccPrunedProvenanceStore)
            end
        end,
        orddict:new(),
        ProvenanceStore).

%% @private
length_internal(
    ObjectId,
    {state_ps_aworset_naive,
        {ProvenanceStore, SubsetEvents, AllEvents}=_Payload}) ->
    {Length, LengthProvenance, LengthNewEvents} =
        orddict:fold(
            fun(_Elem,
                Provenance,
                {AccLength, AccLengthProvenance, AccNewEvents}) ->
                {NewProvenance, NewEvents} =
                    case AccLengthProvenance of
                        [] ->
                            {Provenance, AccNewEvents};
                        _ ->
                            state_ps_type:cross_provenance(
                                AccLengthProvenance, Provenance)
                    end,
                {AccLength + 1,
                    NewProvenance,
                    ordsets:union(AccNewEvents, NewEvents)}
            end,
            {0, ordsets:new(), ordsets:new()},
            ProvenanceStore),
    {NewLengthProvenance, AddedEvents} =
        ordsets:fold(
            fun(Dot, {AccNewLengthProvenance, AccAddedEvents}) ->
                DotWithoutESet =
                    ordsets:fold(
                        fun({EventType, _EventInfo}=Event, AccDotWithoutESet) ->
                            case EventType of
                                state_ps_event_partial_order_event_set ->
                                    AccDotWithoutESet;
                                _ ->
                                    ordsets:add_element(
                                        Event, AccDotWithoutESet)
                            end
                        end,
                        ordsets:new(),
                        Dot),
                NewEvent =
                    {state_ps_event_partial_order_event_set,
                        {ObjectId, DotWithoutESet}},
                NewDot =
                    state_ps_type:event_set_max(
                        ordsets:add_element(NewEvent, Dot)),
                NewProvenance =
                    ordsets:add_element(NewDot, AccNewLengthProvenance),
                NewAddedEvents = ordsets:add_element(NewEvent, AccAddedEvents),
                {NewProvenance, NewAddedEvents}
            end,
            {ordsets:new(), ordsets:new()},
            LengthProvenance),
    NewProvenanceStore =
        case NewLengthProvenance of
            [] ->
                orddict:new();
            _ ->
                orddict:store(
                    Length, NewLengthProvenance, orddict:new())
        end,
    NewSubsetEvents =
        state_ps_type:event_set_max(
            state_ps_type:event_set_union(
                SubsetEvents,
                state_ps_type:event_set_union(AddedEvents, LengthNewEvents))),
    NewAllEvents =
        state_ps_type:event_set_max(
            state_ps_type:event_set_union(
                AllEvents,
                state_ps_type:event_set_union(AddedEvents, LengthNewEvents))),
    NewPayload = {NewProvenanceStore, NewSubsetEvents, NewAllEvents},
    {state_ps_size_t_naive, NewPayload}.

%% @private
singleton_internal(
    ObjectId,
    {_Type, {ProvenanceStore, SubsetEvents, AllEvents}=_Payload}) ->
    {SingletonElem, SingletonProvenance, SingletonNewEvents} =
        orddict:fold(
            fun(Elem,
                Provenance,
                {AccSingletonElem, AccSingletonProvenance, AccNewEvents}) ->
                %% @todo HACK!!!
                RealElem =
                    case Elem of
                        [H|T] ->
                            [H|T];
                        _ ->
                            [Elem]
                    end,
                NewElem = lists:append(AccSingletonElem, RealElem),
                {NewProvenance, NewEvents} =
                    case AccSingletonProvenance of
                        [] ->
                            {Provenance, AccNewEvents};
                        _ ->
                            state_ps_type:cross_provenance(
                                AccSingletonProvenance, Provenance)
                    end,
                {NewElem, NewProvenance, NewEvents}
            end,
            {[], ordsets:new(), ordsets:new()},
            ProvenanceStore),
    {NewSingletonProvenance, AddedEvents} =
        ordsets:fold(
            fun(Dot, {AccNewSingletonProvenance, AccAddedEvents}) ->
                DotWithoutESet =
                    ordsets:fold(
                        fun({EventType, _EventInfo}=Event, AccDotWithoutESet) ->
                            case EventType of
                                state_ps_event_partial_order_event_set ->
                                    AccDotWithoutESet;
                                _ ->
                                    ordsets:add_element(
                                        Event, AccDotWithoutESet)
                            end
                        end,
                        ordsets:new(),
                        Dot),
                NewEvent =
                    {state_ps_event_partial_order_event_set,
                        {ObjectId, DotWithoutESet}},
                NewDot =
                    state_ps_type:event_set_max(
                        ordsets:add_element(NewEvent, Dot)),
                NewProvenance =
                    ordsets:add_element(NewDot, AccNewSingletonProvenance),
                NewAddedEvents = ordsets:add_element(NewEvent, AccAddedEvents),
                {NewProvenance, NewAddedEvents}
            end,
            {ordsets:new(), ordsets:new()},
            SingletonProvenance),
    NewProvenanceStore =
        case NewSingletonProvenance of
            [] ->
                orddict:new();
            _ ->
                orddict:store(
                    SingletonElem, NewSingletonProvenance, orddict:new())
        end,
    NewSubsetEvents =
        state_ps_type:event_set_max(
            state_ps_type:event_set_union(
                SubsetEvents,
                state_ps_type:event_set_union(
                    AddedEvents, SingletonNewEvents))),
    NewAllEvents =
        state_ps_type:event_set_max(
            state_ps_type:event_set_union(
                AllEvents,
                state_ps_type:event_set_union(
                    AddedEvents, SingletonNewEvents))),
    NewPayload = {NewProvenanceStore, NewSubsetEvents, NewAllEvents},
    {state_ps_singleton_orset_naive, NewPayload}.

%% @private
unsingleton_internal(
    {state_ps_singleton_orset_naive,
        {ProvenanceStore, SubsetEvents, AllEvents}=_Payload}) ->
    NewProvenanceStore =
        case ProvenanceStore of
            [] ->
                [];
            [{ListElem, Provenance}] ->
                lists:foldl(
                    fun(Elem, AccNewProvenanceStore) ->
                        orddict:store([Elem], Provenance, AccNewProvenanceStore)
                    end,
                    orddict:new(),
                    ListElem)
        end,
    NewPayload = {NewProvenanceStore, SubsetEvents, AllEvents},
    {state_ps_aworset_naive, NewPayload}.

%% @private
group_by_first_internal(
    ObjectId,
    {state_ps_aworset_naive,
        {ProvenanceStore, SubsetEvents, AllEvents}=_Payload}) ->
    FstToSndSetAndCrossedProvenancesAndNewEvents =
        orddict:fold(
            fun({Fst, Snd}=_Elem, Provenance, AccTempGroupBy) ->
                orddict:update(
                    Fst,
                    fun({OldSndSet, OldCrossedProvenances, OldNewEvents}) ->
                        {NewCrossedProvenance, NewNewEvents} =
                            state_ps_type:cross_provenance(
                                OldCrossedProvenances, Provenance),
                        {ordsets:add_element(Snd, OldSndSet),
                            NewCrossedProvenance,
                            ordsets:union(OldNewEvents, NewNewEvents)}
                    end,
                    {ordsets:add_element(Snd, ordsets:new()),
                        Provenance,
                        ordsets:new()},
                    AccTempGroupBy)
            end,
            orddict:new(),
            ProvenanceStore),
    FstToSndSetAndNewCrossedProvenancesAndNewEventsWithAddedEvents =
        orddict:map(
            fun(_Fst, {SndSet, CrossedProvenances, NewEvents}) ->
                {NewCrossedProvenances, NewEventsWithAddedEvents} =
                    ordsets:fold(
                        fun(Dot, {AccNewSingletonProvenance, AccAddedEvents}) ->
                            DotWithoutESet =
                                ordsets:fold(
                                    fun({EventType, _EventInfo}=Event,
                                        AccDotWithoutESet) ->
                                        case EventType of
                                            state_ps_event_partial_order_event_set ->
                                                AccDotWithoutESet;
                                            _ ->
                                                ordsets:add_element(
                                                    Event, AccDotWithoutESet)
                                        end
                                    end,
                                    ordsets:new(),
                                    Dot),
                            NewEvent =
                                {state_ps_event_partial_order_event_set,
                                    {ObjectId, DotWithoutESet}},
                            NewDot = ordsets:add_element(NewEvent, Dot),
                            NewProvenance =
                                ordsets:add_element(
                                    NewDot, AccNewSingletonProvenance),
                            NewAddedEvents =
                                ordsets:add_element(NewEvent, AccAddedEvents),
                            {NewProvenance,
                                ordsets:union(NewEvents, NewAddedEvents)}
                        end,
                        {ordsets:new(), ordsets:new()},
                        CrossedProvenances),
                {SndSet, NewCrossedProvenances, NewEventsWithAddedEvents}
            end,
            FstToSndSetAndCrossedProvenancesAndNewEvents),
    {NewProvenanceStore, NewSubsetEvents, NewAllEvents} =
        orddict:fold(
            fun(Fst,
                {SndSet, SndCrossedProvenances, SndNewEventsWithAddedEvents},
                {AccNewProvenanceStore, AccNewSubsetEvents, AccNewAllEvents}) ->
                NewAccNewProvenanceStore =
                    case SndCrossedProvenances of
                        [] ->
                            AccNewProvenanceStore;
                        _ ->
                            orddict:store(
                                {Fst, SndSet},
                                SndCrossedProvenances,
                                AccNewProvenanceStore)
                    end,
                NewAccNewSubsetEvents =
                    state_ps_type:event_set_max(
                        state_ps_type:event_set_union(
                            AccNewSubsetEvents,
                            SndNewEventsWithAddedEvents)),
                NewAccNewAllEvents =
                    state_ps_type:event_set_max(
                        state_ps_type:event_set_union(
                            AccNewAllEvents,
                            SndNewEventsWithAddedEvents)),
                {NewAccNewProvenanceStore,
                    NewAccNewSubsetEvents,
                    NewAccNewAllEvents}
            end,
            {orddict:new(), SubsetEvents, AllEvents},
            FstToSndSetAndNewCrossedProvenancesAndNewEventsWithAddedEvents),
    {state_ps_group_by_orset_naive,
        {NewProvenanceStore, NewSubsetEvents, NewAllEvents}}.
