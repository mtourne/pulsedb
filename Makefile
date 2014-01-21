
all:
	ERL_LIBS=.. erl -make

app:
	@./rebar compile

clean:
	@./rebar clean
	@rm -f erl_crash.dump

run:
	ERL_LIBS=..:deps erl -name pulsedb@127.0.0.1 -pa ebin -s pulsedb_launcher

ct: test

test:
	mkdir -p logs
	ct_run -pa ebin -logdir logs/ -dir test/

.PHONY: test

PLT_NAME=.stockdb_dialyzer.plt

$(PLT_NAME):
	@ERL_LIBS=deps dialyzer --build_plt --output_plt $@ \
		--apps kernel stdlib sasl crypto || true

dialyze: $(PLT_NAME)
	@dialyzer ebin  --plt $(PLT_NAME) --no_native \
		-Werror_handling -Wunderspecs
