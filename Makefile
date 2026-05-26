CC=gcc
CFLAGS=-Wall -g -I/usr/include/modbus
LDFLAGS=-lmodbus -lpaho-mqtt3c -lsqlite3 -lpthread

all:gateway

gateway:gateway.c
	$(CC) $(CFLAGS) -o gateway gateway.c $(LDFLAGS)

clean:
	rm -f gateway gateway.db
.PHONY: all clean
