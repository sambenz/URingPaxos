THRIFT=thrift
FLAGS=-gen java -out ../../src/java/

all: dmap

dmap: dmap.thrift 
	$(THRIFT) $(FLAGS) dmap.thrift

