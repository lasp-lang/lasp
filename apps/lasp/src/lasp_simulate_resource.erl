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

-include("lasp.hrl").
-include_lib("webmachine/include/webmachine.hrl").

-spec init(list()) -> {ok, term()}.
init(_) ->
    {ok, undefined}.

%% return the list of available content types for webmachine
content_types_provided(Req, Ctx) ->
    {[{"application/json", to_json}], Req, Ctx}.

to_json(ReqData, State) ->
    {ok, Nodes} = lasp_peer_service:members(),
    {ok, _} = lasp_simulation:run(lasp_advertisement_counter,
                                  [Nodes, lasp_orset, lasp_gcounter, 10000, 100, 10]),
    Encoded = jsx:encode(#{status => ok, nodes => Nodes}),
    PlotDir = code:priv_dir(?APP) ++ "/plots",
    LogDir = code:priv_dir(?APP) ++ "/logs",
    InputFile1 = LogDir ++ "/lasp_transmission_instrumentation-client-lasp_orset-lasp_gcounter-10000-100-10.csv",
    InputFile2 = LogDir ++ "/lasp_transmission_instrumentation-server-lasp_orset-lasp_gcounter-10000-100-10.csv",
    GnuPlot = PlotDir ++ "/advertisement_counter_transmission_orset_gcounter-10000-100-10.gnuplot",
    OutputFile = PlotDir ++ "/advertisement_counter_transmission_orset_gcounter-10000-100-10.pdf",
    Command = "gnuplot -e \"inputfile1='" ++ InputFile1 ++ "'; inputfile2='" ++ InputFile2 ++ "'; outputname='" ++ OutputFile ++ "'\" " ++ GnuPlot,
    Result = os:cmd(Command),
    lager:info("Generating plot: ~p; output: ~p", [Command, Result]),
    {Encoded, ReqData, State}.
