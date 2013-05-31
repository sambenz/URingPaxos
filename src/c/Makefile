CC = gcc
CFLAGS += -shared -fPIC -g -O2 -W -Wall -Wno-unused-parameter -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux 
TARGET = libpaxos.so

all: $(TARGET)

header: ../../target/classes/ch/usi/da/paxos/storage/CyclicArray.class
	javah -jni -classpath ../../target/classes/ ch.usi.da.paxos.storage.CyclicArray

%.o: %.c
	$(CC) $(CFLAGS) -o $@ -c $<

$(TARGET): paxos.c
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)	

clean:
	find . -name "*~" -exec rm -f {} ';'
	find . -name "*.o" -exec rm -f {} ';'
	rm -f $(TARGET)

