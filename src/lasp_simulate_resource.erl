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

-module(lasp_simulate_resource).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-export([init/1,
         content_types_provided/2,
         to_json/2]).

-export([run/0,
         advertisement_counter_transmission_simulation/1]).

-include("lasp.hrl").
-include_lib("webmachine/include/webmachine.hrl").

-define(NUM_EVENTS, 200000).
-define(NUM_CLIENTS_PER_VM, 100).

-define(ORSET, lasp_orset).
-define(COUNTER, lasp_gcounter).

-spec init(list()) -> {ok, term()}.
init(_) ->
    {ok, undefined}.

content_types_provided(Req, Ctx) ->
    {[{"application/json", to_json}], Req, Ctx}.

to_json(ReqData, State) ->
    spawn_link(?MODULE, run, []),
    Encoded = jsx:encode(#{status => ok}),
    {Encoded, ReqData, State}.

%% @private
run() ->
    %% Get list of nodes from the peer service.
    {ok, Nodes} = lasp_peer_service:members(),

    %% Run the simulation.
    Profile = lasp_config:get(profile, false),
    case Profile of
        true ->
            lager:info("Applying and profiling function..."),
            eprof:profile([self(), whereis(lasp_sup)],
                          ?MODULE,
                          advertisement_counter_transmission_simulation,
                          [Nodes]),

            lager:info("Analyzing..."),
            eprof:analyze(total, [{sort, time}]);
        false ->
            advertisement_counter_transmission_simulation(Nodes)
    end,

    ok.

%% @private
output_file(Plot) ->
    string:join([plot_dir() ++  "/advertisement_counter_" ++ atom_to_list(Plot),
                atom_to_list(?ORSET),
                atom_to_list(?COUNTER),
                integer_to_list(?NUM_EVENTS),
                integer_to_list(?NUM_CLIENTS_PER_VM) ++ ".pdf"], "-").

%% @private
priv_dir() ->
    code:priv_dir(?APP).

%% @private
plot_dir() ->
    priv_dir() ++ "/plots".

%% @private
log_dir() ->
    priv_dir() ++ "/logs".

%% @private
log_dir(Log) ->
    log_dir() ++ "/" ++ Log.

%% @private
advertisement_counter_transmission_simulation(Nodes) ->

    DCOS = os:getenv("DCOS", "false"),
    {FastSync, SlowSync} = case DCOS of
        "false" ->
            {500, 1000};
        _ ->
            {30000, 60000}
    end,

    {ok, [DivergenceFilename1,
          ClientFilename1|_]} = lasp_simulation:run(lasp_advertisement_counter,
                                                    [Nodes,
                                                     false,
                                                     ?ORSET,
                                                     ?COUNTER,
                                                     ?NUM_EVENTS,
                                                     ?NUM_CLIENTS_PER_VM,
                                                     FastSync]),

    %% Run the simulation with the orset, gcounter, deltas enabled;
    %% 500ms sync.
    {ok, [_,
          ClientFilename2|_]} = lasp_simulation:run(lasp_advertisement_counter,
                                                    [Nodes,
                                                     true,
                                                     ?ORSET,
                                                     ?COUNTER,
                                                     ?NUM_EVENTS,
                                                     ?NUM_CLIENTS_PER_VM,
                                                     FastSync]),

    %% Run the simulation with the orset, gcounter, no deltas; 1s sync.
    {ok, [DivergenceFilename2
          |_]} = lasp_simulation:run(lasp_advertisement_counter,
                                    [Nodes,
                                     false,
                                     ?ORSET,
                                     ?COUNTER,
                                     ?NUM_EVENTS,
                                     ?NUM_CLIENTS_PER_VM,
                                     SlowSync]),

    generate_plot(divergence, DivergenceFilename1, DivergenceFilename2),

    generate_plot(transmission, ClientFilename1, ClientFilename2),

    ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% @private
%% @doc Generate plots.
generate_plot(Plot, Filename1, Filename2) ->
    Bin = case os:getenv("MESOS_TASK_ID", "false") of
        "false" ->
            "gnuplot";
        _ ->
            "/usr/bin/gnuplot"
    end,

    PlotString = atom_to_list(Plot),
    OutputFile = output_file(Plot),
    PlotFile = plot_dir() ++ "/advertisement_counter_" ++ PlotString ++ ".gnuplot",
    Command = Bin ++
        " -e \"inputfile1='" ++ log_dir(Filename1) ++
        "'; inputfile2='" ++ log_dir(Filename2) ++
        "'; outputname='" ++ OutputFile ++ "'\" " ++ PlotFile,
    Result = os:cmd(Command),
    lager:info("Generating " ++ PlotString ++ " plot: ~p; output: ~p",
               [Command, Result]).
