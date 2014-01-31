-module(pulsedb_points_handler).
-behaviour(cowboy_http_handler).

-export([init/3, handle/2, terminate/3]).

-record(points_page, {
  secret
}).

init({tcp, http}, Req, Opts) ->
  Secret = case lists:keyfind(superuser, 1, Opts) of
    {superuser, Login, Pass} ->
      base64:encode(iolist_to_binary([Login, ":", Pass]));
    _ ->
      lager:info("unconfigured superuser access"),
      not_set
  end,
  {ok, Req, #points_page{secret=Secret}}.

terminate(_,_,_) ->
  ok.

%%%%%%
% PAGE
%%%%%%
handle(Req, #points_page{secret=Secret}=State) ->
  {ok, Reply} =
  case auth_data(Req) of
    {Secret, Req1} ->
      cowboy_req:reply(200, headers(html), get_page(), Req1);
    _ ->
      cowboy_req:reply(401, headers(auth), "", Req)
  end,
  {ok, Reply, State}.


get_page() ->
  Points = gen_tracker:list(pulsedb_pushers),
  Raw = ["<!DOCTYPE html>"
         "<html lang='en'>",
           template(head, []),
           "<body>",
             template(points, Points),
           "<body>",
         "</html>"],
  iolist_to_binary(Raw).


template(head, _) ->
  ["<head>"
     "<title>Connected Points</title>",
     styles(),
     scripts(),
   "</head>"];


template(points, Points) ->
  ["<div class='points'>",
    [template(point, Point) || Point <- Points],
   "</div>"];


template(point, {Name, Attrs}) ->
  ["<div class='point'>"
     "<h3>", Name, "</h3>",
     "<div class='attrs'>",
       [template(attr, Attr) || Attr <- Attrs],
     "</div>",
   "</div>"];


template(attr, {Name, Value}) ->
  ["<div class='attr'>",
     io_lib:format("~p: ~p", [Name, Value]),
   "</div>"].


styles() ->
  "<style>"
    "h3 {margin-bottom: 0.3ex;}"
    ".attrs {margin-left: 2em;}"
  "</style>".

scripts() ->
  "<script>"
    "setTimeout(function(){window.location.reload()}, 10000)"
  "</script>".

headers(html) -> [{<<"content-type">>, <<"text/html">>}];
headers(auth) -> [{<<"WWW-Authenticate">>, <<"Basic realm=\"pulsedb\"">>}].



auth_data(Req) ->
  {Auth, Req1} = cowboy_req:header(<<"authorization">>, Req),
  Secret = case Auth of
    undefined ->
      undefined;
    _ ->
      case binary:split(Auth, <<" ">>) of
        [<<"Basic">>, Encoded] -> Encoded;
        _                      -> undefined
      end
  end,
  {Secret, Req1}.
