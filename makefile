all:  tinyGoogleServer helper client mapreduce

tinyGoogleServer: tinyGoogleServer.o network_function.o
	gcc tinyGoogleServer.o network_function.o -o tinyGoogleServer -lpthread 
	
tinyGoogleServer.o: tinyGoogleServer.c
	gcc -c tinyGoogleServer.c

helper: helper.o network_function.o
	gcc helper.o network_function.o -o helper -lpthread 
	
helper.o: helper.c
	gcc -c helper.c

client: client.o network_function.o
	gcc client.o network_function.o -o client -lm
	
client.o: client.c
	gcc -c client.c
	
network_function.o: network_function.c
	gcc -c network_function.c

mapreduce: mapreduce.c 
	gcc -o mapreduce mapreduce.c
clean:
	rm -rf *o tinyGoogleServer helper client intermediate_file/*/ inverted_index/*/ file*.*