-module(pulsedb_netpush_auth).

-export([auth/2]).
-export([encrypt/2, decrypt/2]).
-export([make_api_key/2]).


ivec() ->
  <<0,1,2,3,4,5,6,7,7,6,5,4,3,2,1,0>>.


auth(Headers, Args) ->
  HexCipher = proplists:get_value(<<"pulsedb-api-key">>, Headers),
  case decrypt(HexCipher, Args) of
    {ok, <<"t", _/binary>>=AuthInfo} ->
      Tags = [ list_to_tuple(binary:split(T,<<"=">>)) || T <- binary:split(AuthInfo, <<":">>, [global]),  T =/= <<"t">> ],
      {ok, Tags};
    _ -> 
      {error, denied}
  end.


decrypt(undefined, _) ->
  {error, denied};

decrypt(HexCipher, Args) ->
  Secret = proplists:get_value(key, Args),
  if is_binary(Secret) ->
    Cipher = from_hex(HexCipher),
    C1 = crypto:stream_init(aes_ctr, Secret, ivec()),
    {_, Data} = crypto:stream_decrypt(C1, Cipher),
    {ok, Data};
  true ->
    lager:error("Unconfigured ~p, need to specify key in args", [?MODULE]),
    {error, unconfigured}
  end.

encrypt(Data, Args) ->
  Secret = proplists:get_value(key, Args),
  if is_binary(Secret) ->
    C1 = crypto:stream_init(aes_ctr, Secret, ivec()),
    {_, Cipher} = crypto:stream_encrypt(C1, Data),
    {ok, to_hex(Cipher)};
  true ->
    lager:error("Unconfigured ~p, need to specify key in args", [?MODULE]),
    {error, unconfigured}
  end.


make_api_key(<<Secret:16/binary, _/binary>>, Tags) when is_list(Tags) ->
  AuthInfo = iolist_to_binary(["t", [ [":", T,"=",V] || {T,V} <- Tags ]  ]),
  C1 = crypto:stream_init(aes_ctr, Secret, ivec()),
  {_, Cipher} = crypto:stream_encrypt(C1, AuthInfo),
  to_hex(Cipher).






to_hex(Bin) ->
  to_hex(Bin, <<>>).

to_hex(<<>>, Acc) ->  Acc;
to_hex(<<I, Bin/binary>>, Acc) ->
  C = iolist_to_binary(string:to_lower(lists:flatten(io_lib:format("~2.16.0B", [I])))),
  to_hex(Bin, <<Acc/binary, C/binary>>).




from_hex(Bin) ->
  from_hex(Bin, <<>>).

from_hex(<<>>, Acc) -> Acc;
from_hex(<<I:2/binary, Bin/binary>>, Acc) -> from_hex(Bin, <<Acc/binary, (erlang:binary_to_integer(I,16))>>).
