THRIFT=thrift
FLAGS=-gen java -out ../../src/java/

all: paxos

paxos : paxos.thrift 
	$(THRIFT) $(FLAGS) paxos.thrift

