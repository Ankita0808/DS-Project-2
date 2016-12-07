#include "network_function.h"

/********************************* UDP TCP socket function ***************************************************/
int createTCP_server(char *addrstr, char *port)
{
	int sockfd, len;  // listen on sock_fd, new connection on new_fd    
	struct addrinfo hints, *servinfo, *p;
	struct sockaddr_in local;
	void *ptr;
	char hostname[1024];    
	int rv, yes=1;;
	
	hostname[1023] = '\0';
	gethostname(hostname, 1023);
	
    memset(&hints, 0, sizeof hints);    
	hints.ai_family = AF_UNSPEC;    
	hints.ai_socktype = SOCK_STREAM;    
	hints.ai_flags = AI_PASSIVE; // use my IP
    if ((rv = getaddrinfo(hostname, NULL, &hints, &servinfo)) != 0) 
		{        
			fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));        
			return 0;    
		}
		
    // loop through all the results and bind to the first we can    
	for(p = servinfo; p != NULL; p = p->ai_next) 
	{        
		if ((sockfd = socket(p->ai_family, p->ai_socktype,p->ai_protocol)) == -1) 
			{            
				perror("server: socket");            
				continue;        
			}
			
		if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1) 
			{            
				perror("setsockopt: reuse address");
				continue;
			}
			
		if (bind(sockfd, p->ai_addr, p->ai_addrlen) == -1)
		{
			close(sockfd);
			perror("server: bind");
		continue;
		}
		break;
	}
	
    if (p == NULL)
	{
	fprintf(stderr, "server: failed to bind\n");
	return 0;
    }
	
	/*get ip address and port*/
	//get ip
	switch (p->ai_family)
	{
		case AF_INET:
			ptr = &((struct sockaddr_in *) p->ai_addr)->sin_addr;
			break;
		case AF_INET6:
			ptr = &((struct sockaddr_in6 *) p->ai_addr)->sin6_addr;
			break;	
	}
	inet_ntop (p->ai_family, ptr, addrstr, 100);
	
	// get port number
	len = sizeof(local);
	getsockname(sockfd,(struct sockaddr *)&local,&len);
	sprintf(port, "%d", htons(local.sin_port));
	/* done get ip and port */
	
    freeaddrinfo(servinfo); // all done with this structure
	
	//listen to with maximum number of connection
    if (listen(sockfd, 50) == -1)
	{
	perror("listen");
	return 0;
    }
	
	return sockfd;
}

int connectTCP_server(char *Server_ip, char *port)
{
	int sockfd, status;
	struct addrinfo hints, *servinfo, *p;
	 
	memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
		
	if ((status = getaddrinfo(Server_ip, port, &hints, &servinfo)) != 0)
	{
		fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(status));
		return 0;
    }
   
   // loop through all the results and connect to the first we can
    for(p = servinfo; p != NULL; p = p->ai_next)
	{
		if ((sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1)
		{
			perror("client: socket");
			continue;
		}
        
		if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1)
		{
			close(sockfd);
			perror("TCP client: connect");
			continue;
		}
        
		break;
    }
    
	if (p == NULL)
	{
		fprintf(stderr, "client: failed to connect\n");
		return 0;
    }
	
    freeaddrinfo(servinfo); // all done with this structure
	
	return sockfd;
}

int createUDP_server(char *addrstr, char *port)
{
	int sockfd, len;  // listen on sock_fd, new connection on new_fd    
	struct addrinfo hints, *servinfo, *p;
	struct sockaddr_in local;
	void *ptr;
	char hostname[1024];    
	int rv, yes=1;;
	
	hostname[1023] = '\0';
	gethostname(hostname, 1023);
	
    memset(&hints, 0, sizeof hints);    
	hints.ai_family = AF_UNSPEC;    
	hints.ai_socktype = SOCK_DGRAM;    
	hints.ai_flags = AI_PASSIVE; // use my IP
    if ((rv = getaddrinfo(hostname, NULL, &hints, &servinfo)) != 0) 
		{        
			fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));        
			return 0;    
		}
		
    // loop through all the results and bind to the first we can    
	for(p = servinfo; p != NULL; p = p->ai_next) 
	{        
		if ((sockfd = socket(p->ai_family, p->ai_socktype,p->ai_protocol)) == -1) 
			{            
				perror("server: socket");            
				continue;        
			}
			
		if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1) 
			{            
				perror("setsockopt");
				continue;
			}
		
		if (bind(sockfd, p->ai_addr, p->ai_addrlen) == -1)
		{
			close(sockfd);
			perror("server: bind");
		continue;
		}
		break;
	}
	
    if (p == NULL)
	{
	fprintf(stderr, "server: failed to bind\n");
	return 0;
    }
	
	/*get ip address and port*/
	//get ip
	switch (p->ai_family)
	{
		case AF_INET:
			ptr = &((struct sockaddr_in *) p->ai_addr)->sin_addr;
			break;
		case AF_INET6:
			ptr = &((struct sockaddr_in6 *) p->ai_addr)->sin6_addr;
			break;	
	}
	inet_ntop (p->ai_family, ptr, addrstr, 100);
	
	// get port number
	len = sizeof(local);
	getsockname(sockfd,(struct sockaddr *)&local,&len);
	sprintf(port, "%d", htons(local.sin_port));
	/* done get ip and port */
	
    freeaddrinfo(servinfo); // all done with this structure
   
	return sockfd;
}
/*
*	connect to UDP server, store server address back for later usage: their_addr
*/
int connectUDP_server(char *Server_ip, char *port, struct sockaddr_storage *their_addr, socklen_t *addr_len)
{
	int sockfd, status;
	struct addrinfo hints, *servinfo, *p;
	 
	memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_DGRAM;
		
  	if ((status = getaddrinfo(Server_ip, port, &hints, &servinfo)) != 0)
	{
		fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(status));
		return 0;
    }
   
   // loop through all the results and connect to the first we can
    for(p = servinfo; p != NULL; p = p->ai_next)
	{
		if ((sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1)
		{
			perror("client: socket");
			continue;
		}
			// if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1)
			// {			
				// close(sockfd);
				// perror("client: connect");
				// continue;
			// }
		break;
    }
    
	if (p == NULL)
	{
		fprintf(stderr, "client: failed to connect\n");
		return 0;
    }
	memcpy((struct sockaddr *)their_addr, p->ai_addr, p->ai_addrlen);
	memcpy(addr_len, &(p->ai_addrlen) , sizeof (p->ai_addrlen));
    freeaddrinfo(servinfo); // all done with this structure
	
	return sockfd;
}

void *get_in_addr(struct sockaddr *sa)
{
	if (sa->sa_family == AF_INET) {
		return &(((struct sockaddr_in*)sa)->sin_addr);
	}
	return &(((struct sockaddr_in6*)sa)->sin6_addr);
}
/********************************* END UDP TCP socket function ************************************************/

/*****************************************TIME FUNCTION ****************************/
void sleep_burst()
{
	struct timespec tim1, tim2;
	tim1.tv_sec = 0;
	tim1.tv_nsec = UDP_BURST_WAIT_TIME;
	
	nanosleep(&tim1, &tim2);
}

timestamp_t get_timestamp()
{
	struct timeval now;
	gettimeofday(&now, NULL);
	return now.tv_usec + (timestamp_t)now.tv_sec * 1000000;
}

/**************************************** ENDTIME FUNCTION ****************************/



/********************************* PACK **************************************************************/

/* float pack by IEEE754 format */
uint64_t pack754(long double f, unsigned bits, unsigned expbits) 
{    long double fnorm;    
	int shift;    
	long long sign, exp, significand;    
	unsigned significandbits = bits - expbits - 1; // -1 for sign bit
    if (f == 0.0) return 0; // get this special case out of the way
    // check sign and begin normalization    
	if (f < 0) { sign = 1; fnorm = -f; }    else { sign = 0; fnorm = f; }
    // get the normalized form of f and track the exponent    
	shift = 0;    
	while(fnorm >= 2.0) { fnorm /= 2.0; shift++; }    
	while(fnorm < 1.0) { fnorm *= 2.0; shift--; }    
	fnorm = fnorm - 1.0;
    // calculate the binary form (non-float) of the significand data    
	significand = fnorm * ((1LL<<significandbits) + 0.5f);
    // get the biased exponent    
	exp = shift + ((1<<(expbits-1)) - 1); // shift + bias
    // return the final answer    
	return (sign<<(bits-1)) | (exp<<(bits-expbits-1)) | significand; 
}
long double unpack754(uint64_t i, unsigned bits, unsigned expbits) 
{    long double result;    
	long long shift;    
	unsigned bias;    
	unsigned significandbits = bits - expbits - 1; // -1 for sign bit
    if (i == 0) return 0.0;
    // pull the significand
  result = (i&((1LL<<significandbits)-1)); // mask    
  result /= (1LL<<significandbits); // convert back to float    
  result += 1.0f; // add the one back on
    // deal with the exponent    
	bias = (1<<(expbits-1)) - 1;    
	shift = ((i>>significandbits)&((1LL<<expbits)-1)) - bias;    
	while(shift > 0) { result *= 2.0; shift--; }    
	while(shift < 0) { result /= 2.0; shift++; }
    // sign it    
	result *= (i>>(bits-1))&1? -1.0: 1.0;
    return result; } 


/*
** packi16() -- store a 16-bit int into a char buffer (like htons())
*/
void packi16(unsigned char *buf, unsigned int i)
{
*buf++ = i>>8; *buf++ = i;
}
/*
** packi32() -- store a 32-bit int into a char buffer (like htonl())
*/
void packi32(unsigned char *buf, unsigned long i)
{
*buf++ = i>>24; *buf++ = i>>16;
*buf++ = i>>8; *buf++ = i;
}
/*
** unpacki16() -- unpack a 16-bit int from a char buffer (like ntohs())
*/
unsigned int unpacki16(unsigned char *buf)
{
return (buf[0]<<8) | buf[1];
}
/*
** unpacki32() -- unpack a 32-bit int from a char buffer (like ntohl())
*/
unsigned long unpacki32(unsigned char *buf)
{
return (buf[0]<<24) | (buf[1]<<16) | (buf[2]<<8) | buf[3];
}
/*
** pack() -- store data dictated by the format string in the buffer
**
** h - 16-bit l - 32-bit
** c - 8-bit char f - float, 32-bit
** s - string (16-bit length is automatically prepended)
*/
int32_t pack(unsigned char *buf, char *format, ...)
{
va_list ap;
int16_t h;
int32_t l;
int8_t c;
float32_t f;
char *s;
int32_t size = 0, len, nextstrlen=0;
va_start(ap, format);
for(; *format != '\0'; format++) {
switch(*format) {
case 'h': // 16-bit
size += 2;
h = (int16_t)va_arg(ap, int); // promoted
packi16(buf, h);
buf += 2;
break;
case 'l': // 32-bit
size += 4;
l = va_arg(ap, int32_t);
packi32(buf, l);
buf += 4;
break;
case 'c': // 8-bit
size += 1;
c = (int8_t)va_arg(ap, int); // promoted
*buf++ = (c>>0)&0xff;
break;
case 'f': // float
size += 4;
f = (float32_t)va_arg(ap, double); // promoted
l = pack754_32(f); // convert to IEEE 754
packi32(buf, l);
buf += 4;
break;
case 's': // string
s = va_arg(ap, char*);
if (nextstrlen > 0) {len = nextstrlen;} else {len = strlen(s);}
size += len + 2;
packi16(buf, len);
buf += 2;
memcpy(buf, s, len);
buf += len;
break;
default:
if (isdigit(*format)) { // track max str len
nextstrlen = nextstrlen * 10 + (*format-'0');
}
}
if (!isdigit(*format)) nextstrlen = 0;
}
va_end(ap);
return size;
}
/*
** unpack() -- unpack data dictated by the format string into the buffer
*/
void unpack(unsigned char *buf, char *format, ...)
{
va_list ap;
int16_t *h;
int32_t *l;
int32_t pf;
int8_t *c;
float32_t *f;
char *s;
int32_t len, count, maxstrlen=0;
va_start(ap, format);
for(; *format != '\0'; format++) {
switch(*format) {
case 'h': // 16-bit
h = va_arg(ap, int16_t*);
*h = unpacki16(buf);
buf += 2;
break;
case 'l': // 32-bit
l = va_arg(ap, int32_t*);
*l = unpacki32(buf);
buf += 4;
break;
case 'c': // 8-bit
c = va_arg(ap, int8_t*);
*c = *buf++;
break;
case 'f': // float
f = va_arg(ap, float32_t*);
pf = unpacki32(buf);
buf += 4;
*f = unpack754_32(pf);
break;
case 's': // string
s = va_arg(ap, char*);
len = unpacki16(buf);
buf += 2;
if (maxstrlen > 0 && len > maxstrlen) count = maxstrlen - 1;
else count = len;
memcpy(s, buf, count);
s[count] = '\0';
buf += len;
break;
default:
if (isdigit(*format)) { // track max str len
maxstrlen = maxstrlen * 10 + (*format-'0');
}
}
if (!isdigit(*format)) maxstrlen = 0;
}
va_end(ap);
}

// pack array to a buffer
int array_marshal(char **buf, float *array, int array_size)
{
	int i, counter = 0, buf_counter = 1, buf_size= 0;
	int16_t c, d;
	
	for (i=0; i < array_size; i++)
			{
					counter ++;
					
					if (counter > (MAX_MES_LEN-4)/4) {
						pack (buf[buf_counter-1],"h", (int16_t)(buf_counter-1));
						pack (buf[buf_counter-1]+2,"h", (int16_t)buf_size);
						counter = 1;
						buf_counter++;
						buf_size = 0;
					}
					buf_size += pack(buf[buf_counter-1]+4+4*(counter-1), "f", (float32_t)(*(array+i)));
			}
			
	// package buf_size and buf_counter to last buffer
		pack (buf[buf_counter-1],"h", (int16_t)(buf_counter-1));
		pack (buf[buf_counter-1]+2,"h", (int16_t) buf_size);
	
	return buf_counter;
}

int array_demarshal(char *buf, float *a)
{
int i,j,k,counter = 0, row, column, number;
int16_t buf_size, buf_counter;


unpack(buf,"hh", &buf_counter, &buf_size);

number = buf_counter * (MAX_MES_LEN-4) / 4;

	for (k=0; k < buf_size/4; k++) {
				   unpack (buf+k*4+4,"f", (a+number+k));
				   }
				   
return buf_counter;
}

//num: string array size, size: string size
int pack_string_array(char *buf, char *string[], int num, int size) 
{
	int i;
	memset(buf, 0, sizeof(buf));
	pack(buf,"h",num);
	char format[10];
	
	buf +=2;
	for (i=0; i<num; i++)
	{
		sprintf(format,"%d",size);
		strcat(format,"s");
		
		pack(buf,format,string[i]);
		buf+= size;
	}
	return (2+size*num);
}

int unpack_string_array(char *buf, char ***string, int size)
{
	int i;
	int16_t num;
	unpack(buf, "h", &num);
	*string = (char **)calloc(num, (sizeof(char *)));	
	for (i=0; i< num; i++)
		(*string)[i]=(char *)malloc(size*sizeof(char));
	
	buf +=2;
	for (i=0; i<num; i++)
	{
		unpack(buf,"s", (*string)[i]);
		buf+= size;
	}
	return num;
}

/********************************* END PACK **********************************************************/