%%% @doc pulsedb_format: module that codes and decodes 
%%% actual data to/from binary representation.
%%% Format version: 2
%%% Here "changed" flags for delta fields are aggregated at
%%% packet start to improve code/decode performance by
%%% byte-aligning LEB128 parts

-module(pulsedb_format).
-author({"Danil Zagoskin", 'z@gosk.in'}).

-include_lib("eunit/include/eunit.hrl").
-include("../include/pulsedb.hrl").

% -on_load(init_nif/0).

-export([encode_full_row/2, decode_full_row/1]).
-export([encode_delta_row/2, decode_delta_row/2]).
-export([format_header_value/2, parse_header_value/2]).

-export([decode_packet/2]).
-export([get_timestamp/1]).


init_nif() ->
  Path = filename:dirname(code:which(?MODULE)) ++ "/../priv",
  Load = erlang:load_nif(Path ++ "/pulsedb_format", 0),
  case Load of
    ok -> ok;
    {error, {Reason,Text}} -> io:format("Load pulsedb_format failed. ~p:~p~n", [Reason, Text])
  end,
  ok.

%% Utility: lists module does not have this
nested_foldl(Fun, Acc0, List) when is_list(List) ->
  lists:foldl(fun(E, Acc) -> nested_foldl(Fun, Acc, E) end, Acc0, List);
nested_foldl(Fun, Acc, Element) ->
  Fun(Element, Acc).


%% @doc Encode full MD packet with given timestamp and (nested) bid/ask list
-spec encode_full_row(Timestamp::integer(), Values::[Value::integer()]) -> iolist().
encode_full_row(Timestamp, Values) when is_integer(Timestamp) andalso is_list(Values) ->
  append_full_values(<<0:1, 0:1, (length(Values)):8, Timestamp:54/integer>>, Values).

append_full_values(Bin, []) -> Bin;
append_full_values(Bin, [Value|Values]) ->
  append_full_values(<<Bin/binary, Value:32/signed-integer>>, Values).




-spec decode_full_row(Buffer::binary()) ->
  {Timestamp::integer(), Values::[Value::integer()], ByteCount::integer()}.
decode_full_row(<<0:1, 0:1, Depth:8, Timestamp:54/integer, Tail/binary>>) ->
  Size = Depth*4,
  <<Vals:Size/binary, _/binary>> = Tail,
  Values = [V || <<V:32/signed-integer>> <= Vals],
  Depth == length(Values) orelse error({invalid_count_of_values,Depth,length(Values)}),
  {Timestamp, Values, 8+Size}.



%% @doc Encode delta MD packet with given timestamp delta and (nested) bid/ask delta list
-spec encode_delta_row(TimeDelta::integer(), ValueDelta::[Delta::integer()]) -> iolist().
encode_delta_row(TimeDelta, ValueDelta) when TimeDelta >= 0 ->
  % Bit mask length is 4*Depth, so wee can align it to 4 bits, leaving extra space for future
  Header = <<0:1, 1:1>>,
  TimeBin = leb128:encode(TimeDelta),

  {HBitMask, DataBin} = nested_foldl(fun(Value, {_Bitmask, _DataBin} = AccIn) ->
        add_delta_field(Value, AccIn)
    end, {Header, <<>>}, ValueDelta),
  HBitMaskPadded = pad_to_octets(HBitMask),
  <<HBitMaskPadded/binary, TimeBin/binary, DataBin/binary>>.


%% Utility: append bit to bitmask and (if not zero) value to data accumulator
add_delta_field(0, {BitMask, DataBin}) ->
  % Zero value. Append 0 to bitmask
  {<<BitMask/bitstring, 0:1>>, DataBin};

add_delta_field(Value, {BitMask, DataBin}) ->
  % non-zero value. Append 1 to bitmask and binary value to data
  ValueBin = leb128:encode_signed(Value),
  {<<BitMask/bitstring, 1:1>>, <<DataBin/binary, ValueBin/binary>>}.

pad_to_octets(BS) ->
  PadSize = erlang:byte_size(BS)*8 - erlang:bit_size(BS),
  <<BS/bitstring, 0:PadSize>>.


-spec decode_delta_row(Buffer::binary(), Depth::integer()) ->
  {TimeDelta::integer(), Values::[Value::integer()], ByteCount::integer()}.
decode_delta_row(<<0:1, 1:1, _/bitstring>> = Bin, Depth) ->
  BMPadSize = (8 - ((Depth + 2) rem 8)) rem 8,
  <<0:1, 1:1, BitMask:Depth/bitstring, _:BMPadSize/bitstring, DataTail/binary>> = Bin,

  {TimeDelta, Values_Tail} = leb128:decode(DataTail),
  {Values, Tail} = decode_deltas(Values_Tail, BitMask),
  ByteCount = erlang:byte_size(Bin) - erlang:byte_size(Tail),
  {TimeDelta, Values, ByteCount}.

decode_deltas(DataBin, BitMask) ->
  {RevPVs, Tail} = lists:foldl(fun(F, {Acc, PVData}) ->
        {P, Data} = get_delta_field(F, PVData),
        {[P | Acc], Data}
    end, {[], DataBin}, [F || <<F:1>> <= BitMask]),
  {lists:reverse(RevPVs), Tail}.

get_delta_field(0, Data) ->
  {0, Data};
get_delta_field(1, Data) ->
  leb128:decode_signed(Data).




%% @doc Main decoding function: takes binary and depth, returns packet type, body and size
-spec decode_packet(Bin::binary(), PrevRow::term()) ->
  {ok, Packet::term(), Size::integer()} | {error, Reason::term()}.
decode_packet(Bin, PrevRow) ->
  try
    do_decode_packet(Bin, PrevRow)
  catch
    Type:Message ->
      {error, {Type, Message, erlang:get_stacktrace()}}
  end.

do_decode_packet(Bin, PrevRow) ->
  do_decode_packet_erl(Bin, PrevRow).

do_decode_packet_erl(<<0:1, 0:1, _/bitstring>> = Bin, _PrevRow) ->
  {Timestamp, Values, Size} = decode_full_row(Bin),
  {ok, {row, Timestamp, Values}, Size};

do_decode_packet_erl(<<0:1, 1:1, _/bitstring>> = Bin, {row,_,PrevValues}=PrevRow) ->
  {TimeDelta, Values, Size} = decode_delta_row(Bin, length(PrevValues)),
  Result = apply_delta(PrevRow, {row, TimeDelta, Values}),
  {ok, Result, Size}.



%% Utility: apply delta md to previous md (actually, just sum field-by-field)
apply_delta({row, TS1, Values1}, {row, TS2, Values2}) ->
  {row, TS1 + TS2, lists:zipwith(fun(V1,V2) -> V1+V2 end,Values1, Values2)}.

% %% Utility: get delta md where first argument is old value, second is new one
% compute_delta({row, TS1, Values1}, {row, TS2, Values2}) ->
%   {row, TS2 - TS1, lists:zipwith(fun(V1,V2) -> V2-V1 end,Values1, Values2)}.


get_timestamp(<<0:1, 0:1, _:8, Timestamp:54/integer, _/binary>>) ->
  Timestamp.


%% @doc serialize header value, used when writing header
format_header_value(date, {Y, M, D}) ->
  io_lib:format("~4..0B-~2..0B-~2..0B", [Y, M, D]);

format_header_value(stock, Stock) ->
  erlang:atom_to_list(Stock);

format_header_value(columns, Columns) ->
  bin_join(Columns,",");

format_header_value(_, Value) ->
  io_lib:print(Value).

bin_join([A], _) -> [A];
bin_join([H|T], S) -> [H,S|bin_join(T,S)];
bin_join([], _) -> [].


%% @doc deserialize header value, used when parsing header
parse_header_value(depth, Value) ->
  erlang:list_to_integer(Value);

parse_header_value(scale, Value) ->
  erlang:list_to_integer(Value);

parse_header_value(chunk_size, Value) ->
  erlang:list_to_integer(Value);

parse_header_value(version, Value) ->
  erlang:list_to_integer(Value);

parse_header_value(columns, Value) ->
  [list_to_binary(S) || S <- string:tokens(Value, ",")];

parse_header_value(have_candle, "true") ->
  true;

parse_header_value(have_candle, "false") ->
  false;

parse_header_value(date, DateStr) ->
  [YS, MS, DS] = string:tokens(DateStr, "/-."),
  { erlang:list_to_integer(YS),
    erlang:list_to_integer(MS),
    erlang:list_to_integer(DS)};

parse_header_value(stock, StockStr) ->
  erlang:list_to_atom(StockStr);

parse_header_value(_, Value) ->
  Value.
