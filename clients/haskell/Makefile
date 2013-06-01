THRIFT=ghc
FLAGS=--make gen-hs/*.hs

all: proposer learner

proposer : proposer.hs 
	$(THRIFT) $(FLAGS) proposer.hs

learner : learner.hs 
	$(THRIFT) $(FLAGS) learner.hs

