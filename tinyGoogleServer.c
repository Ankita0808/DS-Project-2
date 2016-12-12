/*
*	Tiny Google Server
*	This Server includes 2 main functions: 
*		Namenode thread: using 2 threads.
*		Client Listen Thread:
*			IndexMaster and Search Query Master
*
*/

#include "network_function.h"
#include <time.h>
#include <math.h>


/**************** REGISTER FUNCTION **********************/
int readNameNodeIP (char *server_ip, char *server_port, int redundancy)
{
	// read IP address and port from file define in header.h
	FILE *ptr_file;
	char buf[100];
	// read the global file
	if (redundancy==0)
		ptr_file =fopen(NAMENODE_FILENAME, "r");
	else if (redundancy==1)
		ptr_file =fopen(NAMENODE_FILENAME2, "r");
	if (!ptr_file)
	{
		return 0;
		//printf("no file open");
	}
	if(fgets(buf,1000, ptr_file)!=NULL)
	{
		// ip address
		strcpy(server_ip, strtok(buf, ","));

		// port number
		strcpy(server_port, strtok(NULL, ","));
	}
	
	// if(server_ip && server_port)
		// printf("Namenode ip and port is %s %s\n", server_ip, server_port);
		
	fclose(ptr_file);
	
	return 1;
}

int readServerIP (char *server_ip, char *server_port, int redundancy)
{
	// read IP address and port from file define in header.h
	FILE *ptr_file;
	char buf[100];
	// read the global file
	if (redundancy==0)
		ptr_file =fopen(SERVER_FILENAME, "r");
	else if (redundancy==1)
		ptr_file =fopen(SERVER_FILENAME2, "r");
	else
	{
		//printf("This server currently only has a redundancy of 2 (coding error)");
		return 0;
	}
	if (!ptr_file)
	{
		//printf("no file open");
		return 0;
	}
	if(fgets(buf,1000, ptr_file)!=NULL)
	{
		// ip address
		strcpy(server_ip, strtok(buf, ","));

		// port number
		strcpy(server_port, strtok(NULL, ","));
	}
	// if(server_ip && server_port)
		// printf("Server ip and port is %s %s\n", server_ip, server_port);
		
	fclose(ptr_file);
	
	return 1;
}


/****************** THREAD VARIABLE PART **************/
struct thread_info 
{
	int connfd;
	int thread_id;
};

// for namenode thread (for Helper communicate)
pthread_t namenode_threads[THREAD_NUMBER];
int namenode_thread_available[THREAD_NUMBER];
pthread_mutex_t namenode_running_mutex;

// for TinyServer thread (for Client communicate)
pthread_t server_threads[THREAD_NUMBER];
int server_thread_available[THREAD_NUMBER];
pthread_mutex_t server_running_mutex;

//For copy-blocking
//pthread_t copy_thread;
int copy_needed_thread_safe;
int copy_in_progress;
//pthread_mutex_t copy_mutex;

/****************** END THREAD VARIABLE PART **************/

/****************** INDEX VARIABLE PART ***********/
struct work_table {
	char URL[MAX_URL_LEN];
	int state; //0: not done; 1: doing; 2: finished
	long int offset;
	char mapFile[MAX_URL_LEN];
};

struct work_table_query {
	char letter;
	char *string[MAX_WORD_LEN];
	int string_array_size;
	char **doc;
	int16_t doc_num;
	int state; //0: not done; 1: doing; 2: finished
};

struct helper_table{
	char ip[100];
	char port[100];
	int state; //0: free; 1: working
};

/****************** END INDEX VARIABLE PART ***********/


/****************** NAME NODE VARIABLE PART ***********/

struct namenode_table
{
	char helper_ip[100];
	char helper_port[100];
	int time;
	int avail;  // Helper is available or not: 0: avail; 1: not avail
	int16_t type; //Helper is either a index helper (0) or a search helper (1)
};

struct namenode_table namenode_table[NAMENODE_TABLE_SIZE];
int helper_number;    // total number of helper available
//namenode thread
pthread_t threads[THREAD_NUMBER];
int thread_available[THREAD_NUMBER];
pthread_mutex_t namenode_running_mutex;

/****************** END NAME NODE VARIABLE PART ***********/


/****************** BEGIN NAMENODE FUNCTION *******/

/*
* Namenode: similar to portmapper
*	Function namenode()
*	A thread: register_Namenode() to open socket -> write to file -> 
*		wait to receive register message from Helper and update Namenode Table
*	A thread: update_Namenode() periodically delete unregister Helper
*/

// write Tiny Google Server Namenode ip address & port number to FILE
int write_namenode_ip(char *addrstr, char *port, int redundancy)
{
	FILE *ptr_file;

	if (redundancy==0)
		ptr_file =fopen(NAMENODE_FILENAME, "w");
	else if (redundancy==1)
		ptr_file =fopen(NAMENODE_FILENAME2, "w");

	if (!ptr_file)
	{
		perror("server: open file to write");
		return 0;
	}
	fprintf(ptr_file,"%s,%s", addrstr, port);
	fclose(ptr_file);
}

//receiving register message from Helper
void *register_Namenode(void *arg)
{
	int16_t register_type, program_version, helper_type;
	char buf[MAX_BUF_LEN];
	char program_name[20], procedure_name[20];
	int i, j, resetTimeout, newServer, packet_size;
	char server_ip[100], server_port[100];		
	struct thread_info *info;
	int task = 0, same_transid =0, connfd, thread_id, numbytes; 
	
	info = (struct thread_info*) arg;
	
	connfd = info->connfd;
	thread_id = info->thread_id;
	
	//printf("connfd is %d\n", connfd);

	if (recv(connfd,buf,MAX_BUF_LEN,0) == -1)
	{
		//perror ("receive error");
		close(connfd);
		pthread_mutex_lock (&namenode_running_mutex);
		namenode_thread_available[thread_id] = 0;
		pthread_mutex_unlock (&namenode_running_mutex);
		pthread_exit(NULL);
	}
			
	unpack(buf,"hss", &register_type, server_ip, server_port);
	//read the message and reply						
	switch (register_type)
	{
		case 1: //helper register
		case 2:
				
			//printf("NameNode Receive from helper: ip: %s port: %s\n", server_ip, server_port);
			
			newServer = 0;
			
			for (i=0; i< helper_number; i++)
			{
					newServer = 0;
					
					// check if helper resend the register packet; 
					if (strcmp(namenode_table[i].helper_ip, server_ip) == 0 &&
							strcmp(namenode_table[i].helper_port, server_port) == 0)
						{
							//printf("Helper %s %s resending register packet \n", server_ip, server_port);
							newServer = 1;
							pthread_mutex_lock (&namenode_running_mutex);
							namenode_table[i].time =2;
							pthread_mutex_unlock (&namenode_running_mutex);
							break;
						}
			}	
			
			//New Helper Register
			if (newServer == 0)
			{
				
				pthread_mutex_lock (&namenode_running_mutex);
				helper_number++;
				if (register_type==2){
					namenode_table[helper_number-1].type = SEARCH_TYPE;
					printf("New Helper: %s %s %hu\n",server_ip, server_port, SEARCH_TYPE);
				}
				else {
					namenode_table[helper_number-1].type = INDEX_TYPE;
					printf("New Helper: %s %s %hu\n",server_ip, server_port, INDEX_TYPE);
				}
				strcpy(namenode_table[helper_number-1].helper_ip,server_ip);
				strcpy(namenode_table[helper_number-1].helper_port,server_port);												
				namenode_table[helper_number-1].time = 2;
				namenode_table[helper_number-1].avail = 0;

				pthread_mutex_unlock (&namenode_running_mutex);
			}
			
			break;
					
		default:
		// this is wrong message
		// reply wrong message
		break;
	}
	close(connfd);
	pthread_mutex_lock (&namenode_running_mutex);
	namenode_thread_available[thread_id] = 0;
	pthread_mutex_unlock (&namenode_running_mutex);
	pthread_exit(NULL);
}


// Thread work to check TIMEOUT of HELPER
void *update_Namenode(void *arg)
{
	int i,j;
	char ip[100], port[100];
	
	while (1)
	{
		for (i=0; i < helper_number; i++)
		{
			namenode_table[i].time--;
				if (namenode_table[i].time == 0)
				{
					for (j=i; j < helper_number-1; j++)
					{
						pthread_mutex_lock (&namenode_running_mutex);
							strcpy(namenode_table[j].helper_ip, namenode_table[j+1].helper_ip);
							strcpy(namenode_table[j].helper_port, namenode_table[j+1].helper_port);
							namenode_table[j].avail = namenode_table[j+1].avail;
							namenode_table[j].time = namenode_table[j+1].time;
							namenode_table[j].type = namenode_table[j+1].type;
						pthread_mutex_unlock (&namenode_running_mutex);
					}
					helper_number--;
					printf("One helper is dropped\n");
				}
		}
		sleep(HELPER_REGISTER_TIME_OUT);
	}
	pthread_exit(NULL);
}

void run_namenode(int sockfd)
{
	int connfd, packetsize, numbytes, namenode_thread_counter;
	struct thread_info *t;
	int i;
	pthread_t checkTimeout_thread;
	
	// Prevent SIGPIPE
	signal (SIGPIPE, SIG_IGN);
	// Thread stuff
	pthread_attr_t attr;
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
	pthread_mutex_init (&server_running_mutex, NULL);
	
	for (i = 0; i< THREAD_NUMBER; i++)
		namenode_thread_available[THREAD_NUMBER] = 0; //initialize all thread as available
	
	// update_Namenode() periodically delete unregister Helper
	if (pthread_create(&checkTimeout_thread, NULL, update_Namenode, NULL) != 0) 
	{
		printf("Failed to create namenode thread to check timeout\n");
	}	
	t = (struct thread_info *) malloc(sizeof(struct thread_info));	
	
	while(1) 
	{
		connfd = accept(sockfd,NULL, NULL);/*Accept new request for a connection*/
		//printf("connfd is %d\n", connfd);
		//printf("\n\nnew client connects\n");
		if (connfd == -1)
		{
			perror("Namenode server: accept");
		}
			
		while (1)
		{
			namenode_thread_counter++;
			if (namenode_thread_counter > THREAD_NUMBER-1)
			{
				namenode_thread_counter = 0;
				sleep_burst();
			}
			if (namenode_thread_available[THREAD_NUMBER] != 1) break;
		}
		
		t->connfd = connfd;
		t->thread_id = namenode_thread_counter;
					
		if (pthread_create(&namenode_threads[namenode_thread_counter], &attr, register_Namenode, (void *)t) != 0) 
		{
			printf("Failed to create Namenode thread %d", namenode_thread_counter);
		}
		else 
		{
			pthread_mutex_lock (&namenode_running_mutex);
			namenode_thread_available[namenode_thread_counter] = 1;
			pthread_mutex_unlock (&namenode_running_mutex);
			
		//	printf("Namenode Thread %d is running \n", namenode_thread_counter );	
		}
		sleep_burst();
   } 
   free(t);
   close(sockfd);
   pthread_attr_destroy(&attr);
   pthread_exit(NULL);
}

void *nameNode()
{
	int sockfd;
	int sockfd2;
	char namenode_ip[100], namenode_port[1000];
	char server_ip[100], server_port[100];
	int redundancy=-1;
	// open TCP socket and bind
	sockfd = createTCP_server(namenode_ip, namenode_port);
	printf("namenode ip and port is: %s %s \n", namenode_ip, namenode_port);
	

	//TODO: 
	//1) Check if server 1 exists AND you can ping it
	if (readNameNodeIP (server_ip, server_port, 0)==1)
	{
		if ((sockfd2= connectTCP_server(server_ip, server_port)) == -1)
		{
			//You are now the first node, since it is not up
			redundancy=0;


		} else {
			close(sockfd2);
		}
	} else {
		//Does not currently exist, so you are the first node
		redundancy=0;
	} 

	if (redundancy < 0 && readNameNodeIP (server_ip, server_port, 1)==1)
	{
		if ((sockfd2 = connectTCP_server(server_ip, server_port)) == -1)
		{
			//You are now the second node, since it is not up
			redundancy=1;

		} else{
			close(sockfd2);
		}
	} else {
		redundancy=1;
	}

	if (redundancy<0)
	{
		printf("Two name servers are already running! We only support 2. Exiting...");
		exit(1);
	}

	// write IP address and port to File
	write_namenode_ip(namenode_ip, namenode_port,redundancy);
	
	// run namenode loop
	run_namenode(sockfd);
	pthread_exit(NULL);
}

/****************** END NAMENODE FUNCTION *******/


/****************** BEGIN TinyGOogleServer FUNCTION *******/
/*
*	include: index master and query master
*	function tinyGoogleServer()
*	function server_threadwork: receive request and depends on request type -> 
*		call function index or query
*/

int check_current_server_alive()
{

}



int write_server_ip(char *addrstr, char *port, int redundancy)
{
	FILE *ptr_file;

	if (redundancy==0)
		ptr_file =fopen(SERVER_FILENAME, "w");
	else if (redundancy==1)
		ptr_file =fopen(SERVER_FILENAME2, "w");

	if (!ptr_file)
	{
		perror("server: open file to write");
		return 0;
	}
	fprintf(ptr_file,"%s,%s", addrstr, port);
	fclose(ptr_file);
}

int remove_directory(const char *path)
{
   DIR *d = opendir(path);
   size_t path_len = strlen(path);
   int r = -1;

   if (d)
   {
      struct dirent *p;

      r = 0;

      while (!r && (p=readdir(d)))
      {
          int r2 = -1;
          char *buf;
          size_t len;

          /* Skip the names "." and ".." as we don't want to recurse on them. */
          if (!strcmp(p->d_name, ".") || !strcmp(p->d_name, ".."))
          {
             continue;
          }

          len = path_len + strlen(p->d_name) + 2; 
          buf = malloc(len);

          if (buf)
          {
             struct stat statbuf;

             snprintf(buf, len, "%s/%s", path, p->d_name);

             if (!stat(buf, &statbuf))
             {
                if (S_ISDIR(statbuf.st_mode))
                {
                   r2  = remove_directory(buf);
                }
                else
                {
                   r2 = unlink(buf);
                }
             }

             free(buf);
          }

          r = r2;
      }

      closedir(d);
   }

   if (!r)
   {
      r = rmdir(path);
   }

   return r;
}

int  server_index(char URL[MAX_URL_LEN], long int segment_size)
{
	int i, j, work_total = 0, reduce_work, helper_counter = 0, work_finished = 0, work_assigned = 0;
	int sockfd, clientfdm, packetsize, numbytes, clientfd, connfd;
	int16_t work_id;
	char server_ip[100], server_port[100], helper_ip[100], helper_port[100];
	char  mapFile[MAX_URL_LEN], reduceFile[MAX_URL_LEN], buf[MAX_BUF_LEN], file_directory[MAX_URL_LEN];
	struct work_table work_table[MAX_WORK];
	struct helper_table helper_table[MAX_HELPER];
	
	//TODO: remove this to somewhere:
	char ii[MAX_URL_LEN]; //inverted index file directory
	strcpy(ii, INVERTED_INDEX);
	
	// Read URL to know how many segment to pass to helpers
	// receive Helper from namenode_table:
	struct dirent *entry;
	DIR *directory = opendir(URL);
	char filename[MAX_URL_LEN];
	FILE *fp;
	
	//change segment size from to KB to B
	segment_size = segment_size * 1024 ;
	
	printf("%ld \n", segment_size);
	
	if(directory == NULL)
	{
		printf("can not open directory %s\n", directory);
		return 0;
	}
	
	while (entry = readdir(directory))
	{
		if(strcmp(entry->d_name, ".") == 0)
			continue;
		if(strcmp(entry->d_name, "..") == 0)
			continue;
		
		strcpy(filename, URL);
		strcat(filename, "/");
		strcat(filename, entry->d_name);
		fp = fopen(filename, "r");
		if(fp==NULL)
		{
			printf("ERROR: Open file %s failed.\n", filename); 
			continue;
		}
		
		fseek( fp, SEEK_SET, SEEK_END ); 
		long int fsize=ftell(fp); 
		fclose(fp);
		long int offset = 0;
		while(fsize > 0)
		{
			//DEK to make sure we do not split at the word boundary,
			//look at last byte before the one you are responsible for
			if (offset == 0)
				work_table[work_total].offset = offset;
			else
				work_table[work_total].offset = offset-1; 

			strcpy(work_table[work_total].URL, filename);
			work_total++;
			fsize = fsize - segment_size;
			offset = offset + segment_size;
		}
	}
        
	closedir(directory);

		
	if (work_total == 0) return 0; //no work to do; directory empty
	
	// create tmp_DIR to store the map intermediate file
	strcpy(file_directory, TMP_FILENAME);
	char template[] = "fileXXXXXX";
	mkstemp(template);
	strcat(file_directory,template);
	mkdir(file_directory, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

	for (i=0 ; i< work_total; i++)
		work_table[i].state = 0;
	
	//divide map work
	//open socknet to listen (job tracker)
	if ((sockfd = createTCP_server(server_ip, server_port)) == 0)
		{
			return 0;
		}
	
	// map	
	while (1)
	{
		printf("Map Works done: %d/%d\n", work_finished, work_total);
		// check if all works have finished or not
		if (work_finished == work_total) // all work finished
		{
			// free all helper
			for (i=0; i< helper_counter; i++)
			{
				for (j=0; j< helper_number; j++)

					if (strcmp(helper_table[i].ip,namenode_table[j].helper_ip) == 0 
							&& strcmp(helper_table[i].port,namenode_table[j].helper_port) ==0 )
					{	
						pthread_mutex_lock (&namenode_running_mutex);
						namenode_table[j].avail = 0;
						pthread_mutex_unlock (&namenode_running_mutex);
					}	
			}	
		break;	
		}
		
		// check existing helper if they are dead or not
		for (i=0 ; i< helper_counter;i++)
		{
			int check = 0;
			for (j=0; j < helper_number; j++)
			{

				if (strcmp(helper_table[i].ip, namenode_table[j].helper_ip) ==0 && 
							strcmp(helper_table[i].port, namenode_table[j].helper_port) == 0)
				{
					check = 1;
					break;
				}
			}
			if (check == 0) // helper died; delete helper from helper table
			{
				for (j = i; j< helper_counter-1; j++)
				{
					strcpy(helper_table[j].ip,helper_table[j+1].ip);
					strcpy(helper_table[j].port,helper_table[j+1].port);
				}
				helper_counter--;
				printf("delete one helper from task\n");
			}
		}
		//get helper from namenode
		for (i=0; i< helper_number; i++)
		{
			if (helper_counter < work_total - work_finished)
			{
				if (namenode_table[i].type==SEARCH_TYPE) continue; //don't include search helpers

				if (namenode_table[i].avail == 0) //helper has no job now
				{	
					pthread_mutex_lock (&namenode_running_mutex);
					namenode_table[i].avail = 1;
					pthread_mutex_unlock (&namenode_running_mutex);
					helper_counter++;
					strcpy(helper_table[helper_counter-1].ip, namenode_table[i].helper_ip);
					strcpy(helper_table[helper_counter-1].port , namenode_table[i].helper_port);
					helper_table[helper_counter-1].state = 0;
				}
			}
		}
		//printf("Number of helper is %d\n", helper_counter);
		// assign work to free helper
		for (i=0; i < helper_counter; i++)
		{
			if (helper_table[i].state == 0)
			{
				for (j=0; j< work_total; j++)
					if (work_table[j].state == 0)
						{
							//TODO: assign work id j to helper id i
							// open socket to connect to namenode
							//printf("helper ip and port is: %s %s\n", helper_table[i].ip, helper_table[i].port);
							helper_table[i].state = 1; //this helper is busy now
							if ((clientfd = connectTCP_server(helper_table[i].ip, 
																		helper_table[i].port)) == 0)
							{
								perror("Connection to helper:");
								helper_table[i].state = 1;
								continue;
							}
														
							memset(buf, 0 , sizeof(buf));
							// pack the register data
							packetsize = pack(buf, "hhsllsss",(int16_t)1, // 1: map
											j, work_table[j].URL, work_table[j].offset, segment_size,
												server_ip, server_port, file_directory);
							// send
							
							if ((numbytes = send(clientfd, buf, packetsize,MSG_NOSIGNAL)) == -1)
							{
								perror("send task to helper");
								pthread_exit(NULL);
							}
							close(clientfd);
							//printf("asking helper here \n");
							//end assign work
							
							work_table[j].state = 1; // this work is assigned already
							work_assigned ++;
							break;
						}
			}
		}
		
		// if 80% of total work is done then assign doing works to other helpers, avoid slaggers
		if (work_finished > reduce_work*8/10 || work_assigned > work_total*8/10)
		{
			for (i=0; i < helper_counter; i++)
			{
				if (helper_table[i].state == 0)
				{
					for (j=0; j< work_total; j++)
						if (work_table[j].state == 1)
							{
								printf("Speculative computation for map work %d\n", j);
								helper_table[i].state = 1; //this helper is busy now
								if ((clientfd = connectTCP_server(helper_table[i].ip, 
																			helper_table[i].port)) == 0)
								{
									perror("Connection to helper:");
									continue;
								}
								memset(buf, 0 , sizeof(buf));
								// pack the register data
								packetsize = pack(buf, "hhsllsss",(int16_t)1, // 1: map
											j, work_table[j].URL, work_table[j].offset, segment_size,
												server_ip, server_port, file_directory);
								// send
								if ((numbytes = send(clientfd, buf, packetsize,MSG_NOSIGNAL)) == -1)
								{
									perror("send extra task to helper");
									pthread_exit(NULL);
								}
								close(clientfd);
								//printf("asking extra helper here \n");
								//end assign work
																
								work_table[j].state = 3; // this work is assigned to 2 helpers at the same time
								break;
							}
				}
			}
		}
		
		// wait reply from any helper
		
		// implement TIMEOUT here; TIMEOUT -> continue while loop (all helpers died case)
		fd_set readfs;
		struct timeval tv;
		tv.tv_sec = TIMEOUT;
		tv.tv_usec = 0;
		FD_ZERO (&readfs);
		FD_SET(sockfd, &readfs);
		
		int nready = select(sockfd+1,&readfs, NULL, NULL, &tv );
		switch (nready) 
		{
				case -1:
					perror("\nSELECT: unexpected error");
					return 0;
					break;
					
				case 0: //TIMEOUT but no helper reply
					continue; // no wait, continue the while loop to ask other helpers
					break;
				
				default: 
					if (FD_ISSET(sockfd, &readfs))
					{
						connfd = accept(sockfd,NULL, NULL);
						if (connfd == -1)
						{
							perror("helper: sending");
						}	
						// receive result from 1 map helper:
						if (recv(connfd,buf,MAX_BUF_LEN,0) == -1)
						{
							perror ("receive error");
						}
						unpack(buf,"hsss",&work_id, helper_ip, helper_port, mapFile);
						
						for (i=0; i< helper_counter; i++)
						{
							if (strcmp(helper_table[i].ip,helper_ip) == 0 
													&& strcmp(helper_table[i].port,helper_port) ==0 )
							helper_table[i].state = 0; // this helper is free now
						}
						if (work_table[work_id].state == 1 || work_table[work_id].state == 3) // finished one work
						{
							work_finished++;
							work_table[work_id].state = 2; // this work is finished
							strcpy(work_table[work_id].mapFile, mapFile );
						}
					}
		}		
		
		
	}
	
	printf("Finish map task\n");
	
	/************* reduce **************/
	reduce_work = 26; //from a->z
	work_finished = 0;
	helper_counter = 0;
	work_assigned = 0;
	for (i=0 ;i< reduce_work; i++)
		work_table[i].state = 0;
	
	printf("Doing reduce task\n");
	while (1)
	{
		printf("Reduce Works done: %d/%d\n", work_finished, reduce_work);
		
		if (work_finished == reduce_work) // all work finished
		{
			// free all helpers
			for (i=0; i< helper_counter; i++)
			{
				for (j=0; j< helper_number; j++)

					if (strcmp(helper_table[i].ip,namenode_table[j].helper_ip) == 0 
							&& strcmp(helper_table[i].port,namenode_table[j].helper_port) ==0 )
					{	
						pthread_mutex_lock (&namenode_running_mutex);
						namenode_table[j].avail = 0;
						pthread_mutex_unlock (&namenode_running_mutex);
					}	
			}	
		break;	
		}
		
		// check existing helper if they are dead or not
		for (i=0 ; i< helper_counter;i++)
		{
			int check = 0;
			for (j=0; j < helper_number; j++)
			{

				if (strcmp(helper_table[i].ip, namenode_table[j].helper_ip) ==0 && 
							strcmp(helper_table[i].port, namenode_table[j].helper_port) == 0)
				{
					check = 1;
					break;
				}
			}
			if (check == 0) // helper died; delete helper from helper table
			{
				for (j = i; j< helper_counter-1; j++)
				{
					strcpy(helper_table[j].ip,helper_table[j+1].ip);
					strcpy(helper_table[j].port,helper_table[j+1].port);
				}
				helper_counter--;
				printf("delete one helper from task\n");
			}
		}
		
		//get helper from namenode
		for (i=0; i< helper_number; i++)
		{
			if (helper_counter < reduce_work - work_finished)
			{
				if (namenode_table[i].type==SEARCH_TYPE) continue; //don't include inverted_index helpers

				if (namenode_table[i].avail == 0) //helper has no job now
				{	
					pthread_mutex_lock (&namenode_running_mutex);
					namenode_table[i].avail = 1;
					pthread_mutex_unlock (&namenode_running_mutex);
					helper_counter++;
					strcpy(helper_table[helper_counter-1].ip, namenode_table[i].helper_ip);
					strcpy(helper_table[helper_counter-1].port , namenode_table[i].helper_port);
					helper_table[helper_counter-1].state = 0;
				}
			}
		}
		//printf("Number of helper is: %d\n", helper_counter);
		// assign work to free helper
		for (i=0; i < helper_counter; i++)
		{
			if (helper_table[i].state == 0)
			{
				//printf("Helper %d is free \n", i);
				for (j=0; j< reduce_work; j++)
					if (work_table[j].state == 0)
						{
							//assign reduce work id j to helper id i
								
								helper_table[i].state = 1; //this helper is busy now
								
								if ((clientfd = connectTCP_server(helper_table[i].ip, 
																			helper_table[i].port)) == 0)
								{
									perror("Connection to helper:");
									continue;
								}
								
								
								memset(buf, 0 , sizeof(buf));
								// pack the register data
								packetsize = pack(buf, "hhssss",(int16_t)2, // 2: reduce
													j, ii, //previous II file
													server_ip, server_port, file_directory);
								// send
								if ((numbytes = send(clientfd, buf, packetsize,MSG_NOSIGNAL)) == -1)
								{
									perror("send to helper");
									pthread_exit(NULL);
								}
								close(clientfd);
								
								//end assign work
							
							
							work_table[j].state = 1; // this work is assigned already
							work_assigned ++;
							break;
						}
			}
		}
		
		// if 80% of total work is done then assign doing works to other helpers, avoid slaggers
		if (work_finished > reduce_work*8/10 || work_assigned > work_total*8/10)
		{
			for (i=0; i < helper_counter; i++)
			{
				if (helper_table[i].state == 0)
				{
					for (j=0; j< reduce_work; j++)
						if (work_table[j].state == 1)
							{
								printf("Speculative computation for reduce work %d\n", j);
								helper_table[i].state = 1; //this helper is busy now
								if ((clientfd = connectTCP_server(helper_table[i].ip, 
																			helper_table[i].port)) == 0)
								{
									perror("Connection to helper:");
									continue;
								}
								memset(buf, 0 , sizeof(buf));
								// pack the register data
								packetsize = pack(buf, "hhssss",(int16_t)2, // 2: reduce
													j, ii, //previous II file
													server_ip, server_port, file_directory);
								// send
								if ((numbytes = send(clientfd, buf, packetsize,MSG_NOSIGNAL)) == -1)
								{
									perror("send to helper");
									pthread_exit(NULL);
								}
								close(clientfd);
								
								//end assign work								
								work_table[j].state = 3; // this work is assigned to 2 helpers at the same time
								break;
							}
				}
			}
		}
		
		// implement TIMEOUT here; TIMEOUT -> continue
		connfd = accept(sockfd,NULL, NULL);
		if (connfd == -1)
		{
			perror("helper: sending");
		}	
		if (recv(connfd,buf,MAX_BUF_LEN,0) == -1)
		{
			perror ("receive error");
		}
		unpack(buf,"hss",&work_id, helper_ip, helper_port);
		//printf("work finished: %d has state: %d\n", work_id, work_table[work_id].state);
		for (i=0; i< helper_counter; i++)
		{
			if (strcmp(helper_table[i].ip,helper_ip) == 0 
									&& strcmp(helper_table[i].port,helper_port) ==0 )
			helper_table[i].state = 0; //helper is free now
		}
		if (work_table[work_id].state == 1 || work_table[work_id].state == 3) // finished one work
		{
			work_finished++;
			work_table[work_id].state = 2; // this work is finished
			//TODO: update II structures by reduceFile
		}		
	}
	
	printf("Finish reduce task, deleting Temp folder\n");
	remove_directory(file_directory);

	
	return 1;
}

int server_query(char *query_string, char ***doc, int *doc_number)
{
	int i, j, work_total = 0, reduce_work, helper_counter = 0, work_finished = 0, new_word, work_assigned = 0;
	int sockfd, packetsize, numbytes, clientfd, connfd;
	int16_t work_id;
	char server_ip[100], server_port[100], helper_ip[100], helper_port[100];
	char  mapFile[MAX_URL_LEN], reduceFile[MAX_URL_LEN], buf[MAX_BUF_LEN], file_directory[MAX_URL_LEN];
	struct work_table_query work_table[MAX_WORK];
	struct helper_table helper_table[MAX_HELPER];
	
	// seperate query string -> into words
	// Extract first letter from each word to find how many work this task has
	// in here fill up the work table with work_total and work_table.string
	char *token;
	//printf("query string is: %s\n", query_string);
	size_t len;
	token = strtok(query_string," \n");
	while (token)
	{
		//printf("token is: %s\n", token);
		char word[MAX_WORD_LEN], letter;
				
		strcpy(word, token);
		len = strlen(word);
		
		//change letter to lower case
		for (i=0; i< len; i++)
			if(word[i] >= 65 && word[i] <= 90)
				word[i] = word[i] + 32;	
		
		//check first letter
		letter = word[0];
			
		//printf("word and first letter is: %s %c\n", word, letter);
		new_word  = 1;
		
		for (i=0; i <work_total; i++)
		{
			// word start with old letter
			if (work_table[i].letter == letter)
			{
				work_table[i].string[work_table[i].string_array_size] = malloc(MAX_WORD_LEN*sizeof(char));
				strcpy(work_table[i].string[work_table[i].string_array_size], word);
				work_table[i].doc_num++;
				work_table[i].string_array_size ++;
				new_word = 0;
				break;
			}
		}
		
		// word start with a new letter
		if (new_word == 1)
		{
			work_table[work_total].letter = letter;
			work_table[work_total].string[0] = malloc(MAX_WORD_LEN*sizeof(char));
			strcpy(work_table[work_total].string[0], word);
			work_table[work_total].string_array_size = 1;
			work_table[work_total].doc_num = 1;
			work_total++;
		}
		token = strtok (NULL, " \n");
	}
	//printf("Done tokenize, number of work  %d\n", work_total);
	// assign each work to each helper (letter, word string)
	char ii[MAX_URL_LEN]; //inverted index file directory
	strcpy(ii, INVERTED_SEARCH_INDEX);
	
	if (work_total == 0) return 1; //no work to do; directory empty
	printf("Total work is: %d \n", work_total);
	for (i=0 ; i< work_total; i++)
		work_table[i].state = 0;
	
	//divide map work
	//open socknet to listen (job tracker)
	if ((sockfd = createTCP_server(server_ip, server_port)) == 0)
		{
			return 0;
		}
	
	//DEK: Somewhere in here it is being blocked by index commands
	while (1)
	{
		printf("Works done: %d/%d\n", work_finished, work_total);
		// check if all works have finished or not
		if (work_finished == work_total) // all work finished
		{
			// free all helpers
			for (i=0; i< helper_counter; i++)
			{
				for (j=0; j< helper_number; j++)
					{
						if (strcmp(helper_table[i].ip,namenode_table[j].helper_ip) == 0 
								&& strcmp(helper_table[i].port,namenode_table[j].helper_port) ==0 )
						{	
							pthread_mutex_lock (&namenode_running_mutex);
							namenode_table[j].avail = 0;
							pthread_mutex_unlock (&namenode_running_mutex);
							break;
						}	
					}
			}
		break;	
		}
		
		// check existing helper if they are dead or not
		for (i=0 ; i< helper_counter;i++)
		{
			int check = 0;
			for (j=0; j < helper_number; j++)
			{

				if (strcmp(helper_table[i].ip, namenode_table[j].helper_ip) ==0 && 
							strcmp(helper_table[i].port, namenode_table[j].helper_port) == 0)
				{
					check = 1;
					break;
				}
			}
			if (check == 0) // helper died; delete helper from helper table
			{
				for (j = i; j< helper_counter-1; j++)
				{
					strcpy(helper_table[j].ip,helper_table[j+1].ip);
					strcpy(helper_table[j].port,helper_table[j+1].port);
				}
				helper_counter--;
				printf("delete one helper from task\n");
			}
		}
		//get new helper from namenode
		for (i=0; i< helper_number; i++)
		{
			if (helper_counter < work_total - work_finished)
			{
				if (namenode_table[i].type==INDEX_TYPE) continue; //don't include index helpers
				if (namenode_table[i].avail == 0) //helper has no job now
				{	
					pthread_mutex_lock (&namenode_running_mutex);
					namenode_table[i].avail = 1;
					pthread_mutex_unlock (&namenode_running_mutex);
					helper_counter++;
					strcpy(helper_table[helper_counter-1].ip, namenode_table[i].helper_ip);
					strcpy(helper_table[helper_counter-1].port , namenode_table[i].helper_port);
					helper_table[helper_counter-1].state = 0;
				}
			}
		}
		// assign work to free helper  DEK_important
		for (i=0; i < helper_counter; i++)
		{

			if (helper_table[i].state == 0)
			{
				for (j=0; j< work_total; j++)
					if (work_table[j].state == 0)
						{
							//TODO: assign work id j to helper id i
							
							helper_table[i].state = 1; //this helper is busy now
							
							if ((clientfd = connectTCP_server(helper_table[i].ip, 
																		helper_table[i].port)) == 0)
							{
								perror("Connection to helper:");
								continue;
							}
							
							memset(buf, 0 , sizeof(buf));
							// pack the register data
							packetsize = pack(buf,"hhh100s100s100s", (int16_t)3,  // 3: query
												j, work_table[j].string_array_size, ii, server_ip, server_port); 
							packetsize +=pack_string_array(buf+6+300, work_table[j].string, 
																	work_table[j].string_array_size, MAX_WORD_LEN);
							// send
							if ((numbytes = send(clientfd, buf, packetsize,MSG_NOSIGNAL)) == -1)
							{
								perror("send task to helper");
								pthread_exit(NULL);
							}
							close(clientfd);
							//printf("asking helper here \n");
							//end assign work
							work_table[j].state = 1; // this work is assigned already
							work_assigned ++;
							break;
						}
			}
		}
		
		// if 80% of total work is done then assign doing works to other helpers, avoid slaggers
		if (work_finished >= work_total*9/10 && work_assigned == work_total)
		{
			for (i=0; i < helper_counter; i++)
			{
				if (helper_table[i].state == 0)
				{
					printf("Speculative computation for query work %d\n", j);
					for (j=0; j< work_total; j++)
						if (work_table[j].state == 1)
							{
								printf("Speculative computation for query work %d\n", j);
								//TODO: assign work id j to helper id i
								// open socket to connect to namenode
								
								helper_table[i].state = 1; //this helper is busy now
								
								if ((clientfd = connectTCP_server(helper_table[i].ip, 
																			helper_table[i].port)) == 0)
								{
									perror("Connection to helper:");
									continue;
								}
								memset(buf, 0 , sizeof(buf));
								// pack the register data
								packetsize = pack(buf,"hhh100s100s100s", (int16_t)3,  // 3: query
												j, work_table[j].string_array_size, ii, server_ip, server_port); 
								packetsize +=pack_string_array(buf+6+300, work_table[j].string, 
																	work_table[j].string_array_size, MAX_WORD_LEN);
								// send
								if ((numbytes = send(clientfd, buf, packetsize,MSG_NOSIGNAL)) == -1)
								{
									perror("send extra task to helper");
									pthread_exit(NULL);
								}
								close(clientfd);
								
								work_table[j].state = 3; // this work is assigned to 2 helpers at the same time
								//printf("asking extra helper here \n");
								//end assign work
								
								break;
							}
				}
			}
		}
		
		// wait reply from any helper
		
		// implement TIMEOUT here; TIMEOUT -> continue while loop (all helpers died case)
		fd_set readfs;
		struct timeval tv;
		tv.tv_sec = TIMEOUT;
		tv.tv_usec = 0;
		FD_ZERO (&readfs);
		FD_SET(sockfd, &readfs);
		
		int nready = select(sockfd+1,&readfs, NULL, NULL, &tv );
		switch (nready) 
		{
				case -1:
					perror("\nSELECT: unexpected error");
					return 0;
					break;
					
				case 0: //TIMEOUT but no helper reply
					continue; // no wait, continue the while loop to ask other helpers
					break;
				
				default: 
					if (FD_ISSET(sockfd, &readfs))
					{
						connfd = accept(sockfd,NULL, NULL);
						if (connfd == -1)
						{
							perror("helper: sending");
						}	
						// receive result from 1 map helper:
						if (recv(connfd,buf,MAX_BUF_LEN,0) == -1)
						{
							perror ("receive error");
						}
						unpack(buf,"h100s100s",&work_id, helper_ip, helper_port);
						
						
						for (i=0; i< helper_counter; i++)
						{
							if (strcmp(helper_table[i].ip,helper_ip) == 0 
													&& strcmp(helper_table[i].port,helper_port) ==0 )
							helper_table[i].state = 0; // this helper is free now
						}
						if (work_table[work_id].state == 1 || work_table[work_id].state == 3) // finished one work
						{
							work_finished++;
							work_table[work_id].state = 2; // this work is finished
							work_table[work_id].doc_num = unpack_string_array(buf+2+200, &work_table[work_id].doc, MAX_WORD_LEN);
							// printf("Number of Documents found is: %d\n", work_table[work_id].doc_num);
							// for (j=0; j< work_table[work_id].doc_num; j++)
								// printf("%d. %s\n", j, work_table[work_id].doc[j]);
						}
					}
		}		
	}
	
	// printf("Done query\n");
	// for (i=0; i<work_total; i++)
	// {
		// printf("Number of Documents found is: %d\n", work_table[i].doc_num);
		// for (j=0; j< work_table[i].doc_num; j++)
			// printf("%d. %s\n", j, work_table[i].doc[j]);
				
	// }
			
	int temp = 0, most_value =0, *docCheck, check, count=0, k;
	
	// select documents that have all the words only
	for (i=0; i<work_total; i++)
		if (work_table[i].doc_num > temp)
		{
			temp = work_table[i].doc_num;
			most_value = i;
		}
		
	docCheck = (int *)malloc(work_table[most_value].doc_num * sizeof (int));
	
	for (i=0; i< work_table[most_value].doc_num; i++)
		docCheck[i] = 1;
		
	for (i=0; i< work_table[most_value].doc_num; i++)
	{
		for (j=0; j< work_total; j++)
		{
			check = 0;
			for (k=0; k< work_table[j].doc_num; k++)
				if (strcmp(work_table[j].doc[k], work_table[most_value].doc[i]) == 0)
				{
					check = 1;
					break;
				}
			if (check == 0) 
			{	
				docCheck[i] = 0;
				continue;
			}	
		}			
	}
	
	for (i=0; i<work_table[most_value].doc_num; i++)
	{
		if (docCheck[i] == 1)
			count++;
	}
	
	*doc_number = count;
	
	*doc = (char **)calloc(count, (sizeof(char *)));
	for (i=0; i< count; i++)
	{
		(*doc)[i]=(char *)malloc(MAX_URL_LEN*sizeof(char));
	}
	
	count =0;
	for (i=0; i<work_table[most_value].doc_num; i++)
		if (docCheck[i] == 1)
		{
			strcpy((*doc)[count], work_table[most_value].doc[i]);
			count++;
		}
	
	// printf("Number of doc is: %d\n", count);
	// for (i=0; i< count; i++)
		// printf("%d. %s\n", i, (*doc)[i]);
	
	
	free(docCheck);
	for (i=0; i< work_total; i++)
	{
		free(work_table[i].doc);
		//free(work_table[i].string);
	}
	// receive result from each helper
	return 1;
}



void *server_thread_work(void *arg)
{

	char server_ip[100], server_port[100], serverUDP_ip[100], serverUDP_port[100] ;
	char procedure_name[20], buf[MAX_BUF_LEN], URL[MAX_URL_LEN], query_string[MAX_QUERY_LEN];	
	int i, connfd, UDPsock,  numbytes, packetsize, thread_id, request_type, doc_num;
	long int segment_size;
	struct thread_info *info;
	char **doc;
	timestamp_t begin, end;
	double 	time_run;
	
	info = (struct thread_info*) arg;
	
	connfd = info->connfd;
	thread_id = info->thread_id;
	
	if (recv(connfd,buf,MAX_BUF_LEN,0) == -1)
	{
		perror ("receive error");
	}
	printf("Receive New Request \n");
	unpack(buf,"h", &request_type);
	switch(request_type)
	{

		while(copy_in_progress==1);

		case(1): // indexing request
			


			//Get timer			
			begin = get_timestamp();
			
			unpack(buf,"hsl", &request_type, URL, &segment_size);
			if (server_index(URL, segment_size))
			{
				memset(buf, 0 , sizeof(buf));
				// 
				packetsize = pack(buf, "h", (int16_t)1);
				// send
				if ((numbytes = send(connfd, buf, packetsize,MSG_NOSIGNAL)) == -1)
				{
					perror("send to helper");
					pthread_mutex_lock (&server_running_mutex);
					server_thread_available[thread_id] = 0;
					pthread_mutex_unlock (&server_running_mutex);
					pthread_exit(NULL);
				}
				
			
			}
			else {
				memset(buf, 0 , sizeof(buf));
				// 
				packetsize = pack(buf, "h", (int16_t)0);
				// send
				if ((numbytes = send(connfd, buf, packetsize,MSG_NOSIGNAL)) == -1)
				{
					perror("send to helper");
					pthread_mutex_lock (&server_running_mutex);
					server_thread_available[thread_id] = 0;
					pthread_mutex_unlock (&server_running_mutex);
					pthread_exit(NULL);
				}
			}			
			
			end = get_timestamp();	
			time_run = end-begin;
			printf("Index Time is %f (ms)\n", time_run/1000);
			break;
		
		case(2): // query request

			begin = get_timestamp();
		
			unpack(buf,"hs", &request_type, query_string);
			printf("Query String: %s\n", query_string);
			server_query(query_string, &doc, &doc_num);
			// for (i=0; i<doc_num; i++)
				// printf("%s\n", doc[i]);
			memset(buf, 0 , sizeof(buf));
			packetsize = pack_string_array(buf, doc, doc_num, MAX_WORD_LEN);
			if ((numbytes = send(connfd, buf, packetsize,MSG_NOSIGNAL)) == -1)
			{
					perror("send to helper");
					pthread_mutex_lock (&server_running_mutex);
					server_thread_available[thread_id] = 0;
					pthread_mutex_unlock (&server_running_mutex);
					pthread_exit(NULL);
			}
			
			end = get_timestamp();
			time_run = end-begin;			
			printf("Query Time is %f (ms)\n", time_run/1000);
			break;
		
		default:
			//wrong message
			break;
	}

	
	close(connfd);

	pthread_mutex_lock (&server_running_mutex);
	server_thread_available[thread_id] = 0;
	pthread_mutex_unlock (&server_running_mutex);

	//print('Entering copy mutex...');
	//pthread_mutex_lock(&copy_mutex);
	if (request_type==1)
		copy_needed_thread_safe=1;


	int holdoff = 0;
	if (copy_needed_thread_safe)
	{
		for (i = 0; i< THREAD_NUMBER; i++)
		{
			if (server_thread_available[i] == 1)
			{
				holdoff = 1;
				break;
			}
		}
		if (holdoff == 0) //No one is doing anything: update the real index
		{
			copy_in_progress=1;	

			printf("Refreshing stored inverted index...\n");

			char cpy_cmd[200];
			char d1[200];
			char d2[200];
			strcpy(cpy_cmd,"cp -r ");
			strcpy(d1,INVERTED_INDEX);
			strcpy(d2,INVERTED_INDEX_COPY_TMP);
			strcat(cpy_cmd,d1);
			strcat(cpy_cmd," ");
			strcat(cpy_cmd, d2);

			printf("Command is %s\n",cpy_cmd);
			//int status_cpy_cmd = system(cpy_cmd);
			printf("Copying done, now moving...");

			remove_directory(INVERTED_SEARCH_INDEX);

			char mv_cmd[200];
			char m1[200];
			char m2[200];
			strcpy(mv_cmd,"mv ");
			strcpy(m1,INVERTED_INDEX_COPY_TMP);
			strcpy(m2,INVERTED_SEARCH_INDEX);
			strcat(mv_cmd,m1);
			strcat(mv_cmd," ");
			strcat(mv_cmd, m2);

			printf("Command is %s\n",mv_cmd);
			//int status_mv_cmd = system(mv_cmd);


			printf("Refreshing complete.\n");
			copy_in_progress=0;
			copy_needed_thread_safe=0;
		}
		
	}
	//pthread_mutex_unlock(&copy_mutex);

	pthread_exit(NULL);
}

int tinyGoogleServer()
{
	int connfd, sockfd, server_thread_counter = 0, i;
	struct sockaddr_storage client_addr;
	socklen_t addr_len;
	struct thread_info *t;
	pthread_attr_t attr;
	char server_ip[100], server_port[100];
	
	// Prevent SIGPIPE
	signal (SIGPIPE, SIG_IGN);
	// thread stuff initialize
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
	pthread_mutex_init (&server_running_mutex, NULL);	
	// initialize all server_threads as available
	for (i = 0; i< THREAD_NUMBER; i++)
		server_thread_available[THREAD_NUMBER] = 0;

	//initialize copy as not needed and not in progress
	copy_needed_thread_safe = 0;
	copy_in_progress = 0;
		
	t = (struct thread_info *) malloc(sizeof(struct thread_info));	
	
	addr_len = sizeof(client_addr);
	
	if ((sockfd = createTCP_server(server_ip, server_port)) == 0)
		{
			free(t);
			pthread_attr_destroy(&attr);	
			pthread_exit(NULL);
		}
		
	printf("Server IP and Port is %s %s\n", server_ip, server_port);

	int sockfd2;
	int redundancy=-1;
	char existing_sip[100], existing_port[100];
	if (readServerIP (existing_sip, existing_port, 0)==1)
	{
		if ((sockfd2 = connectTCP_server(existing_sip, existing_port)) == -1)
		{
			//You are now the first node, since it is not up
			redundancy=0;


		} else {
			close(sockfd2);
		}
	} else {
		//Does not currently exist, so you are the first node
		redundancy=0;
	} 

	if (redundancy < 0 && readServerIP (existing_sip, existing_port, 1)==1)
	{
		if ((sockfd2 = connectTCP_server(existing_sip, existing_port)) == -1)
		{
			//You are now the second node, since it is not up
			redundancy=1;

		} else{
			close(sockfd2);
		}
	} else {
		redundancy=1;
	}

	if (redundancy<0)
	{
		printf("Both servers are currently running! We only support 2. Exiting...");
		exit(1);
	}
	//TODO: 
	//1) Check if server 1 exists AND you can ping it

	//2) Check if server 2 exists AND you can ping it 

	//If no to both, exit


	write_server_ip(server_ip, server_port, redundancy );
	while (1)
	{
		// Accept new request for a connection
		connfd = accept(sockfd,NULL, NULL);
		//printf("new client connecting\n");
		if (connfd == -1)
		{
			perror("server: accept");
		}	
		// create thread here
		while (1)
		{
			server_thread_counter++;
			if (server_thread_counter > THREAD_NUMBER-1)
			{
					server_thread_counter = 0;
					sleep_burst();
			}
			if (server_thread_available[THREAD_NUMBER] == 0)
				break;
		}
		// TODO: if not work use (long)
		t->connfd = connfd;
		t->thread_id = server_thread_counter;
			
		/* create the new thread */
		if (pthread_create(&server_threads[server_thread_counter], &attr, server_thread_work, (void *)t) != 0) 
		{
			printf("Failed to create thread %d", server_thread_counter);
		}
		else 
		{
			pthread_mutex_lock (&server_running_mutex);	
			server_thread_available[server_thread_counter] = 1;
			pthread_mutex_unlock (&server_running_mutex);
			//printf("Thread %d is running \n", server_thread_counter );	
		}


	}
	free(t);
	pthread_attr_destroy(&attr);	
	pthread_exit(NULL);
}
/***************** END TinyGOolgleServer FUNCTION *********/

/***************** BEGIN MAIN FUNCTION *********/
int main()
{
	pthread_t namenode;
	
	// Prevent SIGPIPE
	signal (SIGPIPE, SIG_IGN);
	// Thread stuff
	pthread_attr_t attr;
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
	pthread_mutex_init (&server_running_mutex, NULL);
	//pthread_mutex_init (&copy_mutex, NULL);
		
	// update_Namenode() periodically delete unregister Helper
	if (pthread_create(&namenode, NULL, nameNode, NULL) != 0) 
	{
		printf("Failed to create namenode thread\n");
	}
	
	tinyGoogleServer();
	
	// code to send string through network
	// char *f[100];
	// char **f2;
	// char buf[MAX_BUF_LEN];
	// f[0] = "aaa";
	// f[1] = "bbbasdf";
	
	// int i;
	// int16_t num;
	
	// pack(buf, "h100s100s",(int16_t)3, "172.16.100.100", "4420");
	// pack_string_array(buf + 202, f, 2, 100);
	// unpack_string_array(buf +202, &f2, 100);
	// printf("result is: %s %s\n",f2[0],f2[1]);
	return 0;
}
/***************** END MAIN FUNCTION *********/
