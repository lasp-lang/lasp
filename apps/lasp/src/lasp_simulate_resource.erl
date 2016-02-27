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

-export([run/1]).

-include("lasp.hrl").
-include_lib("webmachine/include/webmachine.hrl").

-define(NUM_EVENTS, 50000).
-define(NUM_CLIENTS, 100).
-define(SYNC_INTERVAL, 10000). %% 10 seconds.

-define(ORSET, lasp_orset).
-define(COUNTER, lasp_gcounter).

-spec init(list()) -> {ok, term()}.
init(_) ->
    {ok, undefined}.

content_types_provided(Req, Ctx) ->
    {[{"application/json", to_json}], Req, Ctx}.

to_json(ReqData, State) ->
    {ok, Nodes} = lasp_peer_service:members(),
    run(Nodes),
    Encoded = jsx:encode(#{status => ok}),
    {Encoded, ReqData, State}.

%% @private
run(Nodes) ->
    advertisement_counter_transmission_simulation(Nodes),
    ok.

%% @private
output_file() ->
    string:join([plot_dir() ++  "/advertisement_counter_transmission",
                atom_to_list(?ORSET),
                atom_to_list(?COUNTER),
                integer_to_list(?NUM_EVENTS),
                integer_to_list(?NUM_CLIENTS),
                integer_to_list(?SYNC_INTERVAL) ++ ".pdf"], "-").

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

    %% Run the simulation with the orset, gcounter, no deltas.
    {ok, [ClientFilename1|_]} = lasp_simulation:run(lasp_advertisement_counter,
                                                    [Nodes,
                                                     false,
                                                     ?ORSET,
                                                     ?COUNTER,
                                                     ?NUM_EVENTS,
                                                     ?NUM_CLIENTS,
                                                     ?SYNC_INTERVAL]),

    %% Run the simulation with the orset, gcounter, deltas enabled.
    {ok, [ClientFilename2|_]} = lasp_simulation:run(lasp_advertisement_counter,
                                                    [Nodes,
                                                     true,
                                                     ?ORSET,
                                                     ?COUNTER,
                                                     ?NUM_EVENTS,
                                                     ?NUM_CLIENTS,
                                                     ?SYNC_INTERVAL]),

    %% Plot both graphs.
    Bin = case os:getenv("MESOS_TASK_ID", "false") of
        "false" ->
            "gnuplot";
        _ ->
            "/usr/bin/gnuplot"
    end,

    OutputFile = output_file(),

    GnuPlot = plot_dir() ++ "/advertisement_counter_transmission.gnuplot",

    Command = Bin ++
        " -e \"inputfile1='" ++ log_dir(ClientFilename1) ++
        "'; inputfile2='" ++ log_dir(ClientFilename2) ++
        "'; outputname='" ++ OutputFile ++ "'\" " ++ GnuPlot,

    Result = os:cmd(Command),
    lager:info("Generating plot: ~p; output: ~p", [Command, Result]),

    ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

