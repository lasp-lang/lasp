-module(derflow_execute_coverage_fsm).
-author('Christopher Meiklejohn <cmeiklejohn@basho.com>').

-behaviour(riak_core_coverage_fsm).

-include_lib("derflow.hrl").

-export([execute/1,
         execute/2]).

-export([init/2,
         process_results/2,
         finish/2]).

-record(state, {from, module, results}).

%% ===================================================================
%% API functions
%% ===================================================================

execute(Module) ->
    execute(Module, 1).

execute(Module, NVal) ->
    ReqId = derflow:mk_reqid(),
    _ = derflow_execute_coverage_fsm_sup:start_child([{raw, ReqId, self()}, [?TIMEOUT, NVal, Module]]),
    {ok, ReqId}.

init(From={_, _, _}, [Timeout, NVal, Module]) ->
    lager:info("Execute coverage FSM started!"),
    Req = ?EXECUTE_REQUEST{module=Module},
    {Req, all, NVal, 1, derflow, derflow_vnode_master, Timeout,
     #state{from=From, module=Module}}.

process_results({error, Reason}, _State) ->
    lager:info("Error received: ~p", [Reason]),
    {error, Reason};
process_results({done, Result}, #state{results=Results}=State) ->
    {done, State#state{results=[Result|Results]}};
process_results(Message, State) ->
    lager:info("Unhandled result: ~p", [Message]),
    {ok, State}.

finish({error, Reason}=Error,
       StateData=#state{from={raw, ReqId, ClientPid}}) ->
    lager:info("Finish triggered with error: ~p", [Reason]),
    ClientPid ! {ReqId, Error},
    {stop, normal, StateData};
finish(clean,
       StateData=#state{from={raw, ReqId, ClientPid},
                        module=Module,
                        results=Results}) ->
    lager:info("Finish triggered with clean!"),
    Result = Module:merge(Results),
    lager:info("Merged result is: ~p, replying to: ~p",
               [Result, ClientPid]),
    ClientPid ! {ReqId, ok, Result},
    {stop, normal, StateData};
finish(Message, StateData) ->
    lager:info("Unhandled finish: ~p", [Message]),
    {stop, normal, StateData}.
