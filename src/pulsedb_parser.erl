-module(pulsedb_parser).
-export([parse/1]).


parse(Str) when is_binary(Str) ->
  parse(binary_to_list(Str));

parse(Str) when is_list(Str) ->
  {ok, Tokens, _} = query_lexer:string(Str),
  {ok, Query} = query_parser:parse(Tokens),
  Query.

  
