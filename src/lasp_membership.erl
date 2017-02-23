%% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 Christopher S. Meiklejohn.  All Rights Reserved.
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

-module(lasp_membership).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

%% API
-export([start_link/0,
         start_link/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% State record.
-record(state, {actor, membership_fun}).

-include("lasp.hrl").

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Same as start_link([]).
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    start_link([]).

%% @doc Start and link to calling process.
-spec start_link(list())-> {ok, pid()} | ignore | {error, term()}.
start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([]) -> {ok, #state{}}.
init([]) ->
    %% Distribution backend needs to assign actor identifier first.
    Actor = case lasp_config:get(actor, undefined) of
        undefined ->
            {stop, no_actor_identifier};
        A ->
            A
    end,

    %% Configure a membership callback to the peer service.
    MembershipFun = fun(S) ->
                            update_membership(S, Actor)
                    end,
    % partisan_peer_service:add_sup_callback(MembershipFun),

    {ok, #state{actor=Actor, membership_fun=MembershipFun}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.

handle_call(Msg, _From, State) ->
    _ = lager:warning("Unhandled call: ~p", [Msg]),
    {noreply, State}.

-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.

%% @private
handle_cast(Msg, State) ->
    _ = lager:warning("Unhandled cast: ~p", [Msg]),
    {noreply, State}.

%% @private
handle_info(Msg, State) ->
    _ = lager:warning("Unhandled info: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec terminate(term(), #state{}) -> term().
terminate(_Reason, _State) ->
    ok.

%% @private
-spec code_change(term() | {down, term()}, #state{}, term()) ->
    {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
update_membership(State, Actor) ->
    lager:info("Attempting to update membership; state: ~p", [State]),

    %% Declare variable if necessary, ensure variable is dynamic and not
    %% synchronized: use a LWW-register.
    {ok, _} = lasp:declare_dynamic(?MEMBERSHIP_ID, ?MEMBERSHIP_TYPE),
    lager:info("Declared dynamic membership: ~p", [?MEMBERSHIP_ID]),

    %% Decode the membership.
    Membership = partisan_peer_service:decode(State),
    lager:info("Decoded membership: ~p", [Membership]),

    %% Bind the new membership to the register.
    {ok, _} = lasp:update(?MEMBERSHIP_ID, {set, timestamp(), Membership}, Actor),
    lager:info("Updated membership: ~p", [?MEMBERSHIP_ID]),

    ok.

%% @private
timestamp() ->
    {Mega, Sec, _Micro} = erlang:timestamp(),
    Mega * 1000000 + Sec.
