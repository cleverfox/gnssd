INCLUDE=-pa deps/*/ebin \
	-pa ebin

all:
	./rebar compile
run:
	erl $(INCLUDE) \
	-kernel inetrc '"./erl_inetrc"' -connect_all false \
	-boot start_sasl -name gnssd -config gnssd.config -s lager start -s gnssd start -s gnss_srv start -s sync go
deploy:
	erl -detached -noshell -noinput $(INCLUDE) \
	-kernel inetrc '"./erl_inetrc"' -connect_all false \
	-boot start_sasl -name gnssd -config gnssd.config \
	-s lager start \
	-s gnssd start \
	-s gnss_aggr start \
	-s gnss_srv start \
	-s gnss_gen start \
	-s sync go


attach:
	erl -name con`jot -r 1 0 100` -hidden -remsh gnssd@`hostname`
	#rlwrap --always-readline 
stop:
	echo 'halt().' | erl -name obsrdrcon -remsh gnssd@`hostname`
