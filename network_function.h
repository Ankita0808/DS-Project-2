#include <ctype.h> 
#include <stdarg.h> 
#include <inttypes.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <sys/wait.h>
#include <signal.h>
#include <time.h>
#include <pthread.h>
#include <dirent.h>
#include "lib.h"

// this is for header.h
#define pack754_32(f) (pack754((f), 32, 8)) 
#define pack754_64(f) (pack754((f), 64, 11)) 
#define unpack754_32(i) (unpack754((i), 32, 8)) 
#define unpack754_64(i) (unpack754((i), 64, 11))

// for function inside
#define UDP_BURST_PACKET 50 // how many UDP packets are sent at once
#define UDP_BURST_WAIT_TIME  10000000// how many nano second wait between 2 UDP burst
#define TIMEOUT1 20 // use by checkData (wait reply from the receiver to sender)
#define TIMEOUT2 2 // use by recvfrom  (wait if sender continues to send data or not), must be smaller than TIMEOUT1
#define RESEND_NUMBER 3 // for UDP asking to resend lost packets
#define TEST_UDP_LOST_PACKET_NUMBER 0 // lost packets percentage; for test only; default is 0


typedef float float32_t;
typedef double float64_t;
typedef unsigned long long timestamp_t;

/********************************************************/
int createTCP_server(char *addrstr, char *port);
int connectTCP_server(char *Server_ip, char *port);
int createUDP_server(char *addrstr, char *port);
int connectUDP_server(char *Server_ip, char *port, struct sockaddr_storage *their_addr, socklen_t *addr_len);
void *get_in_addr(struct sockaddr *sa);
void sleep_burst();
timestamp_t get_timestamp();
/********************************************************/
//pack: source from beej document
uint64_t pack754(long double f, unsigned bits, unsigned expbits);
long double unpack754(uint64_t i, unsigned bits, unsigned expbits);
void packi16(unsigned char *buf, unsigned int i);
void packi32(unsigned char *buf, unsigned long i);
unsigned int unpacki16(unsigned char *buf);
unsigned long unpacki32(unsigned char *buf);
int32_t pack(unsigned char *buf, char *format, ...);
void unpack(unsigned char *buf, char *format, ...);
//
int array_marshal (char **buf, float *array, int array_size);
int array_demarshal(char *buf, float *a);
int pack_string_array(char *buf, char *string[], int num, int size);
int unpack_string_array(char *buf, char ***string, int size);
/********************************************************/


