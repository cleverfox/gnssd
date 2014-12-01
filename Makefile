INCLUDE=-pa deps/*/ebin \
	-pa ebin

all:
	./rebar compile
run:
	rlwrap --always-readline erl $(INCLUDE) \
	-kernel inetrc '"./erl_inetrc"' \
	-boot start_sasl -name gnssd -config gnssd.config -s lager start -s gnssd start -s sync go
deploy:
	erl -detached -noshell -noinput $(INCLUDE) \
	-kernel inetrc '"./erl_inetrc"' \
	-boot start_sasl -name gnssd -config gnssd.config -s lager start -s gnssd start -s sync go
attach:
	rlwrap --always-readline erl -name obsrdrcon -remsh gnssd@`hostname`
stop:
	echo 'halt().' | erl -name obsrdrcon -remsh gnssd@`hostname`
