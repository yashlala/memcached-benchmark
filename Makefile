mclient: mclient.c
	gcc mclient.c -pthread -lmemcached -lm -o mclient
