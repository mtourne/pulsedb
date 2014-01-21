-module(pulsedb_netpush_auth).

-export([auth/2]).
-export([make_api_key/2]).


ivec() ->
  <<0,1,2,3,4,5,6,7,7,6,5,4,3,2,1,0>>.


auth(Headers, Args) ->
  Secret = proplists:get_value(key, Args),
  case lists:keyfind(<<"pulsedb-api-key">>, 1, Headers) of
    {_, HexCipher} when byte_size(Secret) == 16 ->
      Cipher = from_hex(HexCipher),
      C1 = crypto:stream_init(aes_ctr, Secret, ivec()),
      {_, AuthInfo1} = crypto:stream_decrypt(C1, Cipher),
      <<"t:", AuthInfo/binary>> = AuthInfo1,
      Tags = [ list_to_tuple(binary:split(T,<<"=">>)) || T <- binary:split(AuthInfo, <<":">>, [global]) ],
      {ok, Tags};
    {_, _} when Secret == undefined ->
      lager:error("Unconfigured ~p, need to specify key in args", [?MODULE]),
      {error, denied};
    false ->
      {error, denied}
  end.


make_api_key(<<Secret:16/binary, _/binary>>, Tags) when is_list(Tags) ->
  AuthInfo = iolist_to_binary(["t", [ [":", T,"=",V] || {T,V} <- Tags ]  ]),
  C1 = crypto:stream_init(aes_ctr, Secret, ivec()),
  {_, Cipher} = crypto:stream_encrypt(C1, AuthInfo),
  to_hex(Cipher).






to_hex(Bin) ->
  to_hex(Bin, <<>>).

to_hex(<<>>, Acc) ->  Acc;
to_hex(<<I, Bin/binary>>, Acc) -> to_hex(Bin, <<Acc/binary, (erlang:integer_to_binary(I,16))/binary>>).




from_hex(Bin) ->
  from_hex(Bin, <<>>).

from_hex(<<>>, Acc) -> Acc;
from_hex(<<I:2/binary, Bin/binary>>, Acc) -> from_hex(Bin, <<Acc/binary, (erlang:binary_to_integer(I,16))>>).
