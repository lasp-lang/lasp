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

-module(lasp_marathon_simulations).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-export([stop/0,
         log_message_queue_size/1]).

stop() ->
    DCOS = os:getenv("DCOS", "false"),
    EvalTimestamp = lasp_config:get(evaluation_timestamp, 0),
    RunningApps = [
        "lasp-client-" ++ integer_to_list(EvalTimestamp),
        "lasp-server-" ++ integer_to_list(EvalTimestamp)
    ],

    lists:foreach(
        fun(AppName) ->
            delete_marathon_app(DCOS, AppName)
        end,
        RunningApps).

%% @private
delete_marathon_app(DCOS, AppName) ->
    Headers = [],
    Url = DCOS ++ "/marathon/v2/apps/" ++ AppName,
    case httpc:request(delete, {Url, Headers}, [], [{body_format, binary}]) of
        {ok, {{_, 200, _}, _, _Body}} ->
            ok;
        Other ->
            lager:info("Delete app ~p request failed: ~p", [AppName, Other])
    end.

log_message_queue_size(Method) ->
    {message_queue_len, MessageQueueLen} = process_info(self(), message_queue_len),
    lasp_logger:mailbox("MAILBOX " ++ Method ++ " message processed; messages remaining: ~p", [MessageQueueLen]).
