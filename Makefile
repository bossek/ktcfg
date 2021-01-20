CFLAGS=-Wall -Wextra -std=c11 -pedantic
LIBS=-lrdkafka

.PHONY: all
all: ktcfg

ktcfg: ktcfg.c
	$(CC) $(CFLAGS) -o ktcfg ktcfg.c $(LIBS)
