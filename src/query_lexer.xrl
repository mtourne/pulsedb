Definitions.
Ws = [\s\t\r\n]
Num = [0-9]
Id = [^;\{\}\n\t\r\,\=\:]
Sep = [\{\}\:\=\,]

Rules.
{Ws}+  : skip_token.
{Sep}  : {token, {list_to_atom(TokenChars), TokenLine}}.
{Num}+ : {token, {num, TokenLine, get_num(TokenChars)}}.
{Id}+  : {token, {id,  TokenLine, get_id(TokenChars)}}.

Erlang code.
get_num(TokenChars) ->
  list_to_binary(TokenChars).

get_id(TokenChars) ->
  list_to_binary(omit_quot(TokenChars)).

omit_quot([$"|Str]) -> lists:sublist(Str, length(Str)-1);
omit_quot(Str) -> Str.
  
