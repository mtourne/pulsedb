Nonterminals
query tags tag aggregator downsampler metric.

Terminals ':' ',' '{' '}' '=' id num.

Rootsymbol query.

query -> aggregator ':' downsampler ':' metric '{' tags : {'$1', '$3', '$5', '$7'}.
query -> aggregator ':' downsampler ':' metric          : {'$1', '$3', '$5', []}.
query -> aggregator ':'                 metric '{' tags : {'$1', undefined, '$3', '$5'}.
query -> aggregator ':'                 metric          : {'$1', undefined, '$3', []}.
query ->                                metric '{' tags : {undefined, undefined, '$1', '$3'}.
query ->                                metric          : {undefined, undefined, '$1', []}.

tags -> tag ',' tags : ['$1'|'$3'].
tags -> tag '}'      : ['$1'].
tags -> '}'          : [].

tag         -> id '=' id : make_tag('$1', '$3').
tag         -> id '=' num : make_tag('$1', '$3').

aggregator  -> id : make_aggregator('$1').
downsampler -> id : make_downsampler('$1').
metric      -> id : make_metric('$1').



Erlang code.

make_aggregator({_,_,A}) -> A;
make_aggregator(A) ->  io:format("A ~p", [A]).

make_downsampler({_,_,Ds}) -> 
  {N,Rest1} = parse_ds_number(Ds, []),
  {I, D} = parse_ds_interval(Rest1, <<>>),
  {ds_mult(I) * N, D}.


parse_ds_number(<<N_,Rest/binary>>, N0) when N_ >= $0, N_ =< $9 ->
  parse_ds_number(Rest, [N_-$0|N0]);
parse_ds_number(Rest, N0) ->
  N = lists:foldr(fun (X, Acc) -> X + Acc*10 end, 0, N0),
  {N, Rest}.

parse_ds_interval(<<I_,Rest/binary>>, I0) when I_ =/= $- -> 
  parse_ds_interval(Rest, <<I0/binary, I_>>);
parse_ds_interval(<<$-, Rest/binary>>, I) -> 
  {I, Rest}.

ds_mult(<<"s">>) -> 1;
ds_mult(<<"m">>) -> 60;
ds_mult(<<"h">>) -> 3600;
ds_mult(<<"d">>) -> 86400.

make_metric({_,_,M}) -> M.  

make_tag({_,_,<<"from">>}, {_,_,V}) -> {from, V};
make_tag({_,_,<<"to">>},   {_,_,V}) -> {to, V};
make_tag({_,_,K},          {_,_,V}) -> {K, V}.
