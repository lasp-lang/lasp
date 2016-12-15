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

-module(lasp_logger).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-export([extended/1,
         extended/2,
         mailbox/1,
         mailbox/2]).

extended(Message) ->
    extended(Message, []).

extended(Message, Args) ->
    case lasp_config:get(extended_logging, false) of
        true ->
            lager:info(Message, Args);
        _ ->
            ok
    end.

mailbox(Message) ->
    mailbox(Message, []).

mailbox(Message, Args) ->
    case lasp_config:get(mailbox_logging, true) of
        true ->
            lager:info(Message, Args);
        _ ->
            ok
    end.
