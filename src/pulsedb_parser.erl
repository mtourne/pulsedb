-module(pulsedb_parser).
-export([parse/1]).


parse(Str) when is_binary(Str) ->
  parse(binary_to_list(Str));

parse(Str) when is_list(Str) ->
  case query_lexer:string(Str) of
    {ok, Tokens, _} ->
      case query_parser:parse(Tokens) of
        {ok, Query} -> Query;
        {error, _} = ParseError -> ParseError
      end;
    {error, _} = LexerError -> LexerError
  end.


  
