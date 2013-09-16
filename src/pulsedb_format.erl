-module(pulsedb_format).

-include("pulsedb.hrl").
-export([decode_config/1, encode_config/1]).
-export([decode_data/2, encode_data/2]).


-define(CONFIG_SOURCE, 1).
-define(DATA_TICKS, 2).


-spec decode_config(binary()) -> [source()].

decode_config(Bin) when is_binary(Bin) ->
  decode_config(Bin, 0).

decode_config(<<?CONFIG_SOURCE, Length:16, Source:Length/binary, Rest/binary>>, Index) ->
  <<L:16, Name:L/binary, Count, Config/binary>> = Source,
  Columns = [binary_to_atom(Column,latin1) || <<C, Column:C/binary>> <= Config],
  Count = length(Columns),
  [#source{source_id = Index, name = Name, columns = Columns}|decode_config(Rest, Index + 1)];

decode_config(<<>>, _) ->
  [].



-spec encode_config(source()|[source()]) -> binary().
encode_config(#source{name = Name, columns = Columns}) ->
  L = size(Name),
  Count = length(Columns),
  Config = [begin
    Col = atom_to_binary(Column,latin1),
    <<(size(Col)), Col/binary>>
  end || Column <- Columns],
  Conf = [<<L:16, Name:L/binary, Count>>,Config],
  Size = iolist_size(Conf),
  iolist_to_binary([<<?CONFIG_SOURCE, Size:16>>, Conf]);

encode_config([#source{}|_] = Sources) ->
  iolist_to_binary([encode_config(Source) || Source <- Sources]).








-spec encode_data(source(), [tick()]) -> iodata().
encode_data(#source{source_id = Id, columns = Columns}, Ticks0) ->
  Ticks1 = [
    T#tick{value = [proplists:get_value(Col,Proplist,0) || Col <- Columns]} || 
    #tick{value = Proplist} = T <-
    Ticks0
  ],
  [#tick{} = Tick|Ticks] = Ticks1,
  {Delta, _} = lists:mapfoldl(fun(Tick1,Tick0) ->
    Row = encode_delta_tick(Tick0, Tick1),
    {Row, Tick1}
  end, Tick, Ticks),
  Block = [leb128:encode(Id), encode_full_tick(Tick), Delta],
  [<<?DATA_TICKS, (iolist_size(Block)):16>>, Block].

encode_full_tick(#tick{utc = UTC, value = Values}) ->
  [<<UTC:32>>, [leb128:encode(Val) || Val <- Values]].

encode_delta_tick(#tick{utc = UTC0, value = Values0} = _Base, #tick{utc = UTC, value = Values}) ->
  [leb128:encode(UTC - UTC0), lists:zipwith(fun(Val0,Val) ->
    leb128:encode_signed(Val - Val0)
  end, Values0, Values)].






-spec decode_data([source()], binary()) -> [tick()].
decode_data(Sources, <<?DATA_TICKS, Length:16, Bin:Length/binary>>) ->
  {Id, Rows} = leb128:decode(Bin),
  #source{} = Source = lists:keyfind(Id, #source.source_id, Sources),
  {Tick, DeltaRows} = decode_full_tick(Source, Rows),
  {Ticks, <<>>} = decode_delta_ticks(Tick, DeltaRows),
  [Tick|Ticks].

decode_full_tick(#source{name = Name, columns = Columns}, <<UTC:32, Rows/binary>>) when is_binary(Rows) ->
  {Values, Rest} = lists:mapfoldl(fun(Column, Bin1) ->
    {Val, Bin2} = leb128:decode(Bin1),
    {{Column,Val}, Bin2}
  end, Rows, Columns),
  {#tick{name = Name, utc = UTC, value = Values}, Rest}.

decode_delta_ticks(#tick{} = _Base, <<>> = Bin) ->
  {[], Bin};

decode_delta_ticks(#tick{name = Name, utc = UTC0, value = Values0} = _Base, DeltaRows) ->
  {UTC, Bin1} = leb128:decode(DeltaRows),

  {Values, Bin4} = lists:mapfoldl(fun({Column,Value0}, Bin2) ->
    {Val, Bin3} = leb128:decode_signed(Bin2),
    {{Column,Value0 + Val}, Bin3}
  end, Bin1, Values0),

  Tick = #tick{name = Name, utc = UTC0 + UTC, value = Values},

  {Ticks, Rest} = decode_delta_ticks(Tick, Bin4),

  {[Tick|Ticks], Rest}.
















