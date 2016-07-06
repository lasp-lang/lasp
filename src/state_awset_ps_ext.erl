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

-module(state_awset_ps_ext).
-author("Junghun Yoo <junghun.yoo@cs.ox.ac.uk>").

-export([intersect/2,
         map/2,
         union/2,
         product/2,
         filter/2]).

union(LValue, RValue) ->
    state_awset_ps:merge(LValue, RValue).

product({state_awset_ps, {{ElemDataStoreL, EventDataStoreL},
                          FilteredOutEventsL, AllEventsAnyL}=_ORSetL},
        {state_awset_ps, {{ElemDataStoreR, EventDataStoreR},
                          FilteredOutEventsR, AllEventsAnyR}=_ORSetR}) ->
    %% Remove the removed events of the other set first (optimisation)
    ValidEventsL = ordsets:union(
                     ordsets:from_list(orddict:fetch_keys(EventDataStoreL)),
                     FilteredOutEventsL),
    ValidEventsR = ordsets:union(
                     ordsets:from_list(orddict:fetch_keys(EventDataStoreR)),
                     FilteredOutEventsR),
    RemovedL = state_awset_ps:subtract_all_events(AllEventsAnyL, ValidEventsL),
    RemovedR = state_awset_ps:subtract_all_events(AllEventsAnyR, ValidEventsR),
    ValidEventsL1 = ordsets:subtract(ValidEventsL, RemovedR),
    ValidEventsR1 = ordsets:subtract(ValidEventsR, RemovedL),
    {ProductElemDataStore, ProductEventDataStore} =
        orddict:fold(
          fun(ElemL, ProvenanceL, AccProductDataStore0) ->
                  case state_awset_ps:subtract_removed(ProvenanceL, ValidEventsL1) of
                      [] ->
                          AccProductDataStore0;
                      RestProvenanceL ->
                          orddict:fold(
                            fun(ElemR, ProvenanceR, AccProductDataStore1) ->
                                    case state_awset_ps:subtract_removed(
                                           ProvenanceR, ValidEventsR1) of
                                        [] ->
                                            AccProductDataStore1;
                                        RestProvenanceR ->
                                            ProductElem = {ElemL, ElemR},
                                            ProductProvenance =
                                                state_awset_ps:cross_provenance(
                                                  RestProvenanceL, RestProvenanceR),
                                            ordsets:fold(
                                              fun(Dot, AccProductDataStore2) ->
                                                      state_awset_ps:add_elem_with_dot(
                                                        ProductElem,
                                                        Dot,
                                                        AccProductDataStore2)
                                              end,
                                              AccProductDataStore1,
                                              ProductProvenance)
                                    end
                            end, AccProductDataStore0, ElemDataStoreR)
                  end
          end, {orddict:new(), orddict:new()}, ElemDataStoreL),
    {state_awset_ps,
     {{ProductElemDataStore, ProductEventDataStore},
      ordsets:subtract(ordsets:union(FilteredOutEventsL, FilteredOutEventsR),
                       ordsets:from_list(orddict:fetch_keys(ProductEventDataStore))),
      join_all_events(AllEventsAnyL, AllEventsAnyR)}}.

intersect({state_awset_ps, {{ElemDataStoreL, EventDataStoreL},
                            FilteredOutEventsL, AllEventsAnyL}=_ORSetL},
          {state_awset_ps, {{ElemDataStoreR, EventDataStoreR},
                            FilteredOutEventsR, AllEventsAnyR}=_ORSetR}) ->
    %% Remove the removed events of the other set first (optimisation)
    ValidEventsL = ordsets:union(
                     ordsets:from_list(orddict:fetch_keys(EventDataStoreL)),
                     FilteredOutEventsL),
    ValidEventsR = ordsets:union(
                     ordsets:from_list(orddict:fetch_keys(EventDataStoreR)),
                     FilteredOutEventsR),
    RemovedL = state_awset_ps:subtract_all_events(AllEventsAnyL, ValidEventsL),
    RemovedR = state_awset_ps:subtract_all_events(AllEventsAnyR, ValidEventsR),
    ValidEventsL1 = ordsets:subtract(ValidEventsL, RemovedR),
    ValidEventsR1 = ordsets:subtract(ValidEventsR, RemovedL),
    {IntersectElemDataStore, IntersectEventDataStore} =
        orddict:fold(
          fun(ElemL, ProvenanceL, AccIntersectDataStore0) ->
                  case state_awset_ps:subtract_removed(ProvenanceL, ValidEventsL1) of
                      [] ->
                          AccIntersectDataStore0;
                      RestProvenanceL ->
                          orddict:fold(
                            fun(ElemR, ProvenanceR, AccIntersectDataStore1) ->
                                    case state_awset_ps:subtract_removed(
                                           ProvenanceR, ValidEventsR1) of
                                        [] ->
                                            AccIntersectDataStore1;
                                        RestProvenanceR ->
                                            case is_equal_inter(ElemL, ElemR) of
                                                false ->
                                                    AccIntersectDataStore1;
                                                true ->
                                                    IntersectElem =
                                                        get_inter_elem(ElemL, ElemR),
                                                    IntersectProvenance =
                                                        state_awset_ps:cross_provenance(
                                                          RestProvenanceL,
                                                          RestProvenanceR),
                                                    ordsets:fold(
                                                      fun(Dot,
                                                          AccIntersectDataStore2) ->
                                                              state_awset_ps:add_elem_with_dot(
                                                                IntersectElem,
                                                                Dot,
                                                                AccIntersectDataStore2)
                                                      end,
                                                      AccIntersectDataStore1,
                                                      IntersectProvenance)
                                            end
                                    end
                            end, AccIntersectDataStore0, ElemDataStoreR)
                  end
          end, {orddict:new(), orddict:new()}, ElemDataStoreL),
    IntersectAllEvents = join_all_events(AllEventsAnyL, AllEventsAnyR),
    {state_awset_ps,
     {{IntersectElemDataStore, IntersectEventDataStore},
      state_awset_ps:subtract_all_events(
        IntersectAllEvents,
        ordsets:from_list(orddict:fetch_keys(IntersectEventDataStore))),
      IntersectAllEvents}}.

map(Function, {state_awset_ps, {{ElemDataStore, EventDataStore},
                                FilteredOutEvents, AllEvents}}) ->
    MapElemDataStore =
        orddict:fold(
          fun(Elem, Provenance, MapElemDataStore0) ->
                  orddict:update(Function(Elem),
                                 fun(OldProvenance) ->
                                         ordsets:union(OldProvenance, Provenance)
                                 end,
                                 Provenance,
                                 MapElemDataStore0)
          end, orddict:new(), ElemDataStore),
    MapEventDataStore =
        orddict:fold(
          fun(Event, Elems, MapEventDataStore0) ->
                  NewElems = ordsets:fold(
                               fun(Elem, NewElems0) ->
                                       ordsets:add_element(Function(Elem), NewElems0)
                               end, ordsets:new(), Elems),
                  orddict:store(Event, NewElems, MapEventDataStore0)
          end, orddict:new(), EventDataStore),
    {state_awset_ps, {{MapElemDataStore, MapEventDataStore},
                      FilteredOutEvents, AllEvents}}.

filter(Function, {state_awset_ps, {{ElemDataStore, EventDataStore},
                                   FilteredOutEvents, AllEvents}}) ->
    {FilterElemDataStore, FilterFilteredOutEvents} =
        orddict:fold(
          fun(Elem, Provenance, {FilterElemDataStore0, FilterFilteredOutEvents0}) ->
                  case Function(Elem) of
                    true ->
                        {orddict:store(Elem, Provenance, FilterElemDataStore0),
                         FilterFilteredOutEvents0};
                    false ->
                        FOEvents = state_awset_ps:get_events_from_provenance(
                                     Provenance),
                        {FilterElemDataStore0,
                         ordsets:union(FilterFilteredOutEvents0, FOEvents)}
                end
          end, {orddict:new(), FilteredOutEvents}, ElemDataStore),
    FilterEventDataStore =
        orddict:fold(
          fun(Event, Elems, FilterEventDataStore0) ->
                  NewElems = ordsets:fold(
                               fun(Elem, NewElems0) ->
                                       case Function(Elem) of
                                           true ->
                                               NewElems0 ++ [Elem];
                                           false ->
                                               NewElems0
                                       end
                               end, ordsets:new(), Elems),
                  case NewElems of
                      [] ->
                          FilterEventDataStore0;
                      _ ->
                          FilterEventDataStore0 ++ [{Event, NewElems}]
                  end
          end, orddict:new(), EventDataStore),
    {state_awset_ps, {{FilterElemDataStore, FilterEventDataStore},
                      FilterFilteredOutEvents,
                      AllEvents}}.

%% @private
join_all_events({vclock, AllEventsA}, {vclock, AllEventsB}) ->
    {JoinedAllEvents, RestB} =
        lists:foldl(
          fun({EventIdA, CounterA}, {JoinedAllEvents0, RestB0}) ->
                  case lists:keytake(EventIdA, 1, RestB0) of
                      false ->
                          {[{EventIdA, CounterA}|JoinedAllEvents0], RestB0};
                      {value, {_EventIdB, CounterB}, RestB1} ->
                          {[{EventIdA, max(CounterA, CounterB)}|JoinedAllEvents0],
                           RestB1}
                  end
          end, {[], AllEventsB}, AllEventsA),
    {vclock, JoinedAllEvents ++ RestB};
join_all_events({vclock, AllEventsA}, AllEventsB) ->
    SetAllEventsA = state_awset_ps:subtract_all_events(
                      {vclock, AllEventsA}, ordsets:new()),
    ordsets:union(SetAllEventsA, AllEventsB);
join_all_events(AllEventsA, {vclock, AllEventsB}) ->
    join_all_events({vclock, AllEventsB}, AllEventsA);
join_all_events(AllEventsA, AllEventsB) ->
    ordsets:union(AllEventsA, AllEventsB).

%% @private
is_equal_inter(Elem, Elem) ->
    true;
is_equal_inter(_ElemL, _ElemR) ->
    false.

%% @private
get_inter_elem(Elem, Elem) ->
    Elem.
