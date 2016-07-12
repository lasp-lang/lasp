%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Christopher S. Meiklejohn.  All Rights Reserved.
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

-module(lasp_eqc).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-ifdef(TEST).
-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(TIMEOUT, 1200).

-define(TEST_TIME, 90).

-define(NUM_TESTS, 2).

-define(NODES, [rita, sue, bob]).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                io:format(user, Str, Args)
        end, P)).

%% State record.
-record(state, {status, variables, nodes}).

%% Generators.

type() ->
    elements([orset]).

%% Initial state.

initial_state() ->
    %% Stop the runner and re-initialize.
    lasp_support:stop_runner(),
    lasp_support:start_runner(),

    #state{status=init, nodes=[], variables=[]}.

%% Launch multiple nodes.

provision(Name) ->
    lasp_support:start_and_join_node(Name, [], ?MODULE).

provision_args(_S) ->
    [elements(?NODES)].

provision_pre(#state{nodes=Nodes, status=init}, [Name]) ->
    Members = [N || {N, _} <- Nodes],
    not lists:member(Name, Members);
provision_pre(#state{status=running}, [_Name]) ->
    false.

provision_next(#state{nodes=Nodes0}=S, Member, [Name]) ->
    Nodes = Nodes0 ++ [{Name, Member}],
    Status = case length(Nodes) =:= length(?NODES) of
        true ->
            running;
        false ->
            init
    end,
    S#state{status=Status, nodes=Nodes}.

%% Verify the node could be clustered.
provision_post(#state{nodes=_Nodes}=_S, [_Name], Member) ->
    RunnerNode = lasp_support:runner_node(),
    {ok, Members} = rpc:call(RunnerNode, partisan_peer_service, members, []),
    lists:member(Member, Members).

%% Declare variables.

declare(Id, Type) ->
    {ok, {{Id, Type}, _, _, _}} = lasp:declare(Id, Type),
    Id.

declare_args(_S) ->
    [elements([a, b, c]), type()].

declare_pre(#state{status=init}, [_Id, _Type]) ->
    false;
declare_pre(#state{status=running, variables=Variables}, [Id, _Type]) ->
    not lists:member(Id, Variables).

declare_next(#state{variables=Variables0}=S, _Res, [Id, _Type]) ->
    Variables = Variables0 ++ [Id],
    S#state{variables=Variables}.

%% Properties.

prop_sequential() ->
    ?SETUP(fun() ->
                setup(),
                fun teardown/0
         end,
        ?FORALL(Cmds, commands(?MODULE),
                begin
                    {H, S, Res} = run_commands(?MODULE, Cmds),
                    pretty_commands(?MODULE, Cmds, {H, S, Res},
                       aggregate(command_names(Cmds), Res == ok))
                end)).

setup() ->
    {ok, _Apps} = application:ensure_all_started(lager),
    ok.

teardown() ->
    ok.

sequential_test_() ->
    {timeout, ?TIMEOUT,
     fun() -> ?assert(eqc:quickcheck(?QC_OUT(eqc:testing_time(?TEST_TIME, prop_sequential())))) end}.

-endif.
-endif.
