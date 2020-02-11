#Makefile creado por Sergio Mejia, Santiago Palacios y Julian Parada
#Objetivo: Compilar analogp.c analogh.c

all: analogp analogh analogh2

analogh2: analogh2.o
	gcc -o analogh2 analogh2.o -lpthread -ansi 
analogh: analogh.o
	gcc -o analogh analogh.o -lpthread -ansi
analogh2.o: analogh2.c result.h log.h parameters2.h
	gcc -c analogh2.c
analogh.o: analogh.c result.h log.h parameters.h
	gcc -c analogh.c
analogp: analogp.o
	gcc -o analogp analogp.o -ansi
analogp.o: analogp.c result.h log.h
	gcc -c analogp.c
clean:
	rm *.o analogh analogp
