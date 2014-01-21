-module(pulsedb_query).
-export([parse/1,render/1]).
-export([tag/2, remove_tag/2, add_tag/2]).

parse(Query) ->
  pulsedb_parser:parse(Query).

render({Aggregator, Downsampler, Name, Tags}) ->
  Parts = [
    case Aggregator of 
      undefined -> <<>>;
      _ -> <<Aggregator/binary, ":">>
    end,
    case Downsampler of
      {N_,Fn} -> 
        N = integer_to_binary(N_),
        <<N/binary,"s-",Fn/binary,":">>;
      _ -> <<>>
    end,
    Name,
    tags_to_text(Tags)],
  iolist_to_binary(Parts).


tag(Tag, {_,_,_,Tags}) ->
  proplists:get_value(Tag, Tags).


add_tag(Tags, Query) when is_list(Tags) ->
  lists:foldl(fun add_tag/2, Query, Tags);
                                        
add_tag({Tag,_}=T, {A,D,N,Tags0}) ->
  Tags = lists:keystore(Tag, 1, Tags0, T),
  {A,D,N,Tags}.


remove_tag(Tags, Query) when is_list(Tags) ->
  lists:foldl(fun remove_tag/2, Query, Tags);  

remove_tag(Tag, {A,D,N,Tags0}) ->
  Tags = lists:keydelete(Tag, 1, Tags0),
  {A,D,N,Tags}.



tags_to_text([]) -> <<>>;
tags_to_text(Tags_) -> 
  Tags = tags_to_text0(Tags_),
  <<"{", Tags/binary, "}">>.

tags_to_text0([Tag0]) -> tag_to_text(Tag0);
tags_to_text0([Tag0|Rest0]) ->
  Tag = tag_to_text(Tag0),
  Rest = tags_to_text0(Rest0),
  <<Tag/binary, ",", Rest/binary>>.

tag_to_text({Tag, Value}) when is_atom(Tag) -> 
  tag_to_text({atom_to_binary(Tag, latin1), Value});
tag_to_text({Tag, Value}) when is_binary(Tag) ->
  iolist_to_binary([Tag, "=", value_to_text(Value)]).


value_to_text(Value) when is_number(Value) -> 
  integer_to_binary(Value);
value_to_text(Value) when is_atom(Value) -> 
  atom_to_binary(Value, latin1);
value_to_text(Value) -> 
  Value.