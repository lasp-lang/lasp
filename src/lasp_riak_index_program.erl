%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
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

-module(lasp_riak_index_program).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-behavior(lasp_program).

-export([init/1,
         process/5,
         execute/2,
         value/1,
         merge/1,
         sum/1]).

-record(state, {type,
                id,
                previous,
                index_name,
                index_value}).

-define(APP,  lasp).
-define(CORE, lasp_core).
-define(SET,  lasp_orset_gbtree).
-define(VIEW, lasp_riak_index_program).

%% @doc Initialize an or-set as an accumulator.
init(Store) ->
    %% These three terms are re-written by a parse_transform.
    Id = ?MODULE,
    DefaultIndexName = undefined,
    DefaultIndexValue = undefined,

    %% Here, we initialize the variables.
    NormalizedId = normalize_to_binary(Id),
    {ok, NormalizedId} = ?CORE:declare(NormalizedId, ?SET, Store),

    {ok, #state{id=NormalizedId,
                index_name=normalize_to_binary(DefaultIndexName),
                index_value=normalize_to_binary(DefaultIndexValue)}}.

%% @doc Notification from the system of an event.
process(Object, Reason, Idx,
        #state{index_name=IndexName, index_value=IndexValue}=State, Store) ->
    Key = riak_object:key(Object),
    VClock = riak_object:vclock(Object),
    Metadata = riak_object:get_metadata(Object),
    IndexSpecs = extract_valid_specs(Object),
    case Reason of
        put ->
            %% Remove any existing entries for a given key.
            ok = remove_entries_for_key(Key, Idx, State, Store),

            %% Different behavior for total index vs. subset index.
            case IndexName of
                undefined ->
                    %% Indexes all values; add.
                    ok = add_entry(Key, VClock, Metadata, Idx, State, Store);
                IndexName ->
                    %% If the index appears in the objects index
                    %% specifications, then we add to index if it
                    %% matches by value.
                    case lists:keyfind(IndexName, 2, IndexSpecs) of
                        {add, IndexName, IndexValue} ->
                            %% Object has index with correct value.
                            ok = add_entry(Key, VClock, Metadata, Idx, State, Store),
                            ok;
                        {add, IndexName, _} ->
                            %% Object has index, but non matching value.
                            ok;
                        false ->
                            %% Object doesn't require indexing.
                            ok
                    end
            end,

            %% If this is the top-level index, create any required views
            %% off of this index.
            case ?MODULE of
                lasp_riak_index_program ->
                    ok = create_views(IndexSpecs);
                _ ->
                    ok
            end,

            ok;
        delete ->
            ok = remove_entries_for_key(Key, Idx, State, Store),
            ok;
        handoff ->
            ok
    end,
    {ok, State}.

%% @doc Return the result.
execute(#state{id=Id, previous=Previous}, Store) ->
    {ok, {_, _, Value}} = ?CORE:read(Id, Previous, Store),
    {ok, Value}.

%% @doc Return the result from a merged response
value(Merged) ->
    {ok, lists:usort([K || {K, _} <- ?SET:value(Merged)])}.

%% @doc Given a series of outputs, take each one and merge it.
merge(Outputs) ->
    Value = ?SET:new(),
    Merged = lists:foldl(fun(X, Acc) -> ?SET:merge(X, Acc) end, Value, Outputs),
    {ok, Merged}.

%% @doc Computing a sum accorss nodes is the same as as performing the
%%      merge of outputs between a replica, when dealing with the
%%      set.  For a set, it's safe to just perform the merge.
sum(Outputs) ->
    Value = ?SET:new(),
    Sum = lists:foldl(fun(X, Acc) -> ?SET:merge(X, Acc) end, Value, Outputs),
    {ok, Sum}.

%% Internal Functions

%% @doc For a given key, remove all metadata entries for that key.
remove_entries_for_key(Key, Idx, #state{id=Id, previous=Previous}, Store) ->
    {ok, {_, Type, Value}} = ?CORE:read(Id, Previous, Store),
    lists:foreach(fun(V) ->
                case V of
                    {Key, _} ->
                        {ok, _} = ?CORE:update(Id, {remove, V}, Idx, Store);
                    _ ->
                        ok
                end
        end, Type:value(Value)),
    ok.

%% @doc Add an entry to the index.
%%
%%      To ensure we can map between data types across replicas, we use
%%      the hashed vclock derived from the coordinator as the unique
%%      identifier in the index for the OR-Set.
add_entry(Key, VClock, Metadata, Idx, #state{id=Id}, Store) ->
    Hashed = crypto:hash(md5, term_to_binary(VClock)),
    {ok, _} = ?CORE:update(Id, {add_by_token, Hashed, {Key, Metadata}}, Idx, Store),
    ok.

%% @doc Extract index specifications from indexes; only select views
%%      which add information, given we don't want to destroy a
%%      pre-computed view, for now.
extract_valid_specs(Object) ->
    IndexSpecs0 = riak_object:index_specs(Object),
    lists:filter(fun({Type, _, _}) -> Type =:= add end, IndexSpecs0).

%% @doc Register all applicable views.
%%
%%      Launch a process to asynchronously register the view; if this
%%      fails, no big deal, it will be generated on the next write.
create_views(Views) ->
    lists:foreach(fun({_Type, Name, Value}) ->
                Module = list_to_atom(atom_to_list(?VIEW) ++ "-" ++
                                      binary_to_list(Name) ++ "-" ++
                                      binary_to_list(Value)),
                spawn_link(fun() ->
                                ok = lasp:register(
                                        ?VIEW,
                                        code:lib_dir(?APP, src) ++ "/" ++ atom_to_list(?VIEW) ++ ".erl",
                                        global,
                                        [{module, Module},
                                         {index_name, binary_to_list(Name)},
                                         {index_value, binary_to_list(Value)}])
                    end)
        end, Views).

%% @doc Normalize a parse_transform'd value into a binary.
normalize_to_binary(undefined) ->
    undefined;
normalize_to_binary(X) when is_atom(X) ->
    atom_to_binary(X, latin1);
normalize_to_binary(X) when is_list(X) ->
    list_to_binary(X).
