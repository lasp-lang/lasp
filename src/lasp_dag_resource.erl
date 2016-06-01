-module(lasp_dag_resource).

-export([init/1,
         content_types_provided/2,
         to_json/2]).

-include_lib("webmachine/include/webmachine.hrl").

init(_) ->
    {ok, undefined}.

content_types_provided(Req, Ctx) ->
    {[{"application/json", to_json}], Req, Ctx}.

to_json(ReqData, State) ->
    {ok, DotFile} = lasp_dependence_dag:to_dot(),
    Content = jsx:encode(#{dot_content => DotFile}),
    {Content, ReqData, State}.
