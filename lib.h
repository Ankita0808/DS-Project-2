#include <stdio.h>
#include <stdint.h> 
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>


#define NAMENODE_FILENAME "//afs/cs.pitt.edu/usr0/ankita/public/CS2510Prj2anm249/namenode_ip"
#define SERVER_FILENAME "//afs/cs.pitt.edu/usr0/ankita/public/CS2510Prj2anm249/server_ip"
#define SERVER_FILENAME2 "//afs/cs.pitt.edu/usr0/ankita/public/CS2510Prj2anm249/server_ip2"
#define TMP_FILENAME "//afs/cs.pitt.edu/usr0/ankita/public/CS2510Prj2anm249/intermediate_file/"
#define INVERTED_INDEX "//afs/cs.pitt.edu/usr0/ankita/public/CS2510Prj2anm249/inverted_index/"

#define MAX_BUF_LEN 3000 // maximum message length to be sent on network
#define MAX_MES_LEN 1500 // maximum buffer size
#define THREAD_NUMBER 8 // number of threads to work on NameNode and Server
#define MAX_URL_LEN 100 // maximum message length to be sent on network
#define NAMENODE_TABLE_SIZE 1000
#define HELPER_REGISTER_TIME_OUT 5
#define MAX_HELPER 1000
#define MAX_WORK 1000
#define MAX_QUERY_LEN 100
#define TIMEOUT 10
#define MAX_WORD_LEN 20
#define MAX_LINE_LEN 5000
#define HASH_LENGTH 5000
#define MAX_PRIME_LESS_THAN_HASH_LEN 4999