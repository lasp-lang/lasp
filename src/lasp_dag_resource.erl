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
    Status = case lasp_config:get(dag_enabled, false) of
        false -> #{present => false};
        true -> case lasp_dependence_dag:to_dot() of
            {ok, Content} -> #{present => true, dot_content => Content};
            _ -> #{present => false}
        end
    end,
    {jsx:encode(Status), ReqData, State}.
