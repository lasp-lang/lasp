%% -------------------------------------------------------------------
%%
%% crdt_statem_eqc: Quickcheck statem test for riak_dt modules
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
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

-module(crdt_statem_eqc).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-record(state,{vnodes=[], mod_state, vnode_id=0, mod}).

-define(NUMTESTS, 1000).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

run(Module, Count) ->
    {atom_to_list(Module), {timeout, 120, [?_assert(prop_converge(Count, Module))]}}.

run_binary_rt(Module, Count) ->
    {atom_to_list(Module), {timeout, 120, [?_assert(prop_bin_roundtrip(Count, Module))]}}.

%% Initialize the state
initial_state() ->
    #state{}.

%% Command generator, S is the state
command(#state{vnodes=VNodes, mod=Mod}) ->
    oneof([{call, ?MODULE, create, [Mod]}] ++
           [{call, ?MODULE, update, [Mod, Mod:gen_op(), elements(VNodes)]} || length(VNodes) > 0] ++ %% If a vnode exists
           [{call, ?MODULE, merge, [Mod, elements(VNodes), elements(VNodes)]} || length(VNodes) > 0] ++
           [{call, ?MODULE, crdt_equals, [Mod, elements(VNodes), elements(VNodes)]} || length(VNodes) > 0]
).

%% Next state transformation, S is the current state
next_state(#state{vnodes=VNodes, mod=Mod, vnode_id=ID, mod_state=Expected0}=S,V,{call,?MODULE,create,_}) ->
    Expected = Mod:update_expected(ID, create, Expected0),
    S#state{vnodes=VNodes++[{ID, V}], vnode_id=ID+1, mod_state=Expected};
next_state(#state{vnodes=VNodes0, mod_state=Expected, mod=Mod}=S,V,{call,?MODULE, update, [Mod, Op, {ID, _C}]}) ->
    VNodes = lists:keyreplace(ID, 1, VNodes0, {ID, V}),
    S#state{vnodes=VNodes, mod_state=Mod:update_expected(ID, Op, Expected)};
next_state(#state{vnodes=VNodes0, mod_state=Expected0, mod=Mod}=S,V,{call,?MODULE, merge, [_Mod, {IDS, _C}=_Source, {ID, _C}=_Dest]}) ->
    VNodes = lists:keyreplace(ID, 1, VNodes0, {ID, V}),
    Expected = Mod:update_expected(ID, {merge, IDS}, Expected0),
    S#state{vnodes=VNodes, mod_state=Expected};
next_state(S, _V, _C) ->
    S.

%% Precondition, checked before command is added to the command sequence
precondition(S, {call,?MODULE, update, [_Mod, _Op, Vnode]}) ->
     #state{vnodes=Vnodes} = S,
     is_member(Vnode, Vnodes);
precondition(S, {call,?MODULE, Fun, [_Mod, Vnode1, Vnode2]}) when Fun /= create ->
     #state{vnodes=Vnodes} = S,
     is_member(Vnode1, Vnodes) and is_member(Vnode2, Vnodes);
precondition(_S, {call, _Mod, _Fun, _Args}) ->
    true.

is_member(Vnode, Vnodes) ->
    lists:member(Vnode, Vnodes).

%% Postcondition, checked after command has been evaluated
%% OBS: S is the state before next_state(S,_,<command>)
postcondition(_S,{call,?MODULE, crdt_equals, _},Res) ->
    Res == true;
postcondition(_S,{call,_,_,_},_Res) ->
    true.

prop_converge(NumTests, Mod) ->
    eqc:quickcheck(eqc:numtests(NumTests, ?QC_OUT(prop_converge(Mod)))).

prop_converge(Mod) ->
    ?FORALL(Cmds,commands(?MODULE, #state{mod=Mod, mod_state=Mod:init_state()}),
            begin
                {H,S,Res} = run_commands(?MODULE,Cmds),
                Merged = merge_crdts(Mod, S#state.vnodes),
                MergedVal = Mod:value(Merged),
                ExpectedValue = Mod:eqc_state_value(S#state.mod_state),
                ?WHENFAIL(
                   %% History: ~p\nState: ~p\ H,S,
                   io:format("History: ~p\nState: ~p~n", [H, S]),
                   conjunction([{res, equals(Res, ok)},
                                {total, equals(sort(Mod, MergedVal), sort(Mod, ExpectedValue))}]))
            end).

prop_bin_roundtrip(Count, Mod) ->
    eqc:quickcheck(eqc:numtests(Count, ?QC_OUT(prop_bin_roundtrip(Mod)))).

prop_bin_roundtrip(Mod) ->
    ?FORALL(CRDT, Mod:generate(),
            begin
                Bin = Mod:to_binary(CRDT),
                {ok, CRDT2} = Mod:from_binary(Bin),
                collect({range(byte_size(term_to_binary(CRDT))), range(byte_size(Bin))},
                        ?WHENFAIL(
                           begin
                               io:format("Gen ~p~n", [CRDT]),
                               io:format("Rountripped ~p~n", [CRDT2])
                           end,
                           Mod:equal(CRDT, CRDT2)))
            end).

bytes_smaller(Bin1, Bin2) ->
   trunc(((byte_size(Bin2) - byte_size(Bin1)) / byte_size(Bin1)) *  100).

range(0) ->
    0;
range(Value) ->
    N = Value div 10,
    {N * 10, (N +1) * 10}.

merge_crdts(Mod, []) ->
    Mod:new();
merge_crdts(Mod, [{_ID, Crdt}|Crdts]) ->
    lists:foldl(fun({_ID0, C}, Acc) ->
                        Mod:merge(C, Acc) end,
                Crdt,
                Crdts).

%% Commands
create(Mod) ->
    Mod:new().

update(Mod, Op, {ID, C}) ->
    %% Fix this for expected errors etc.
    case Mod:update(Op, ID, C) of
        {ok, C2} ->
            C2;
        _Error ->
            C
    end.

merge(Mod, {_IDS, CS}, {_IDD, CD}) ->
    Mod:merge(CS, CD).

crdt_equals(Mod, {_IDS, CS}, {_IDD, CD}) ->
    Mod:equal(Mod:merge(CS, CD),
              Mod:merge(CD, CS)).

%% Helpers
%% The orset CRDT returns a list, it has no guarantees about order
%% list equality expects lists in order
sort(Mod, L) when Mod == riak_dt_orset; Mod == riak_dt_gset;
                  Mod == riak_dt_orswot; Mod == riak_dt_map;
                  Mod == riak_dt_tsmap ->
    lists:sort(L);
sort(_, Other) ->
    Other.

-endif. % EQC
