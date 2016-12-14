#include "network_function.h"
#include <math.h>

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
		printf("This server currently only has a redundancy of 2 (coding error)");
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

int main()
{
	int sockfd, packetsize, numbytes, doc_num, i, c;
	char URL[MAX_URL_LEN], buf[MAX_BUF_LEN], server_ip[100], server_port[100];
	long int segment_size;
	int choice=1;
	const int max_redundancy= 2;
	int num_redundancy_tries = 0;
	int current_redundant_server=0;
	int16_t reply;
	char query_string[MAX_QUERY_LEN], **doc;
	
	//for evaluation
	int count = 0;
	int times = 100;
	timestamp_t begin, end, average[times], standard, real_average;
	
	
	int fullfilled_choice = 1;
	while (choice == 1 || choice == 2)
	{

		if (num_redundancy_tries==max_redundancy)
		{
			perror("ERROR: All name servers are offline!");
			perror("Exiting...");
			exit(1);
		}

		if (fullfilled_choice == 1)
		{
			printf("Press:\n1 to index\n2 to query\n0 to exit\n");
			scanf("%d", &choice);
		}
		if (choice == 1)
		{
			if (fullfilled_choice == 1) //If the client is not waiting on a previous request
			{
				printf("Input Index Directory:");
				while ((c = getchar()) != '\n' && c != EOF);
				scanf("%s", URL);
				printf("Input Segment Size (KB):");
				scanf("%ld", &segment_size);
				while ((c = getchar()) != '\n' && c != EOF);
			}	
			
			//Read the name server IP from the shared filespace. 
			if (readServerIP(server_ip, server_port, current_redundant_server)==0)
			{
				num_redundancy_tries++;
				printf("DEBUG: name server number %d was not initialized (can't read its file)",current_redundant_server);
				fullfilled_choice = 0; //Do not ask user again
				current_redundant_server=(current_redundant_server+1)%max_redundancy;
				continue;
			}
			
			// open socket to connect to server
			if ((sockfd = connectTCP_server(server_ip, server_port)) == 0)
			{
				//perror("Connection to server:");
				printf("DEBUG: name server number %d is offline, switching to next redundant server",current_redundant_server);
				current_redundant_server=(current_redundant_server+1)%max_redundancy;
				fullfilled_choice=0;
				continue;
			}
			
			//Connected fine to one of the servers, so give the other one a chance to come back online if it was down
			//In other words, no longer assume it is down if it was previously
			num_redundancy_tries=0;
			fullfilled_choice=1;

			memset(buf, 0, sizeof(buf));
		
			// pack the register data
			packetsize = pack(buf, "hsl",(int16_t)1, URL, segment_size); // 1: indexing request
				
			// send
			if ((numbytes = send(sockfd, buf, packetsize,MSG_NOSIGNAL)) == -1)
			{
				perror("send server");
				continue;
			}
			printf("Waiting for reply\n...");
			
			if ((numbytes = recv(sockfd, buf, MAX_BUF_LEN, 0)) == -1)
			{
				perror("receive from server");
				continue;
			}
			
			unpack(buf,"h", &reply);
			if (reply == 0)
				printf("Index did not succeed: wrong directory path\n");
			else if (reply == 5)
				printf("Index succeeded\n");
			else
				printf("Indexing failed, lost connection to the server\n");
			// receive result
			// if (recv)
			
			close(sockfd);
		}
		
		if (choice == 2)
		{
			
			// while (count < times)
			// {
			// begin = get_timestamp();
			// count++;
			
			if (fullfilled_choice==1) //If the client is not waiting on a previous request
			{
				printf("Input Query:\n");
				query_string[0] = '\0'; /* Ensure empty line if no input delivered */
				query_string[sizeof(query_string)-1] = ~'\0';  /* Ensure no false-null at end of buffer */
				
				//flush the stdin
				
				while ((c = getchar()) != '\n' && c != EOF);
				
				fgets(query_string, sizeof(query_string), stdin);
				
				printf("\nQuery is: %s\n", query_string);
			}
			
			if (readServerIP(server_ip, server_port,current_redundant_server)==0)
			{
				num_redundancy_tries++;
				printf("DEBUG: name server number %d was not initialized (can't read its file)",current_redundant_server);
				current_redundant_server=(current_redundant_server+1)%max_redundancy;
				fullfilled_choice=0;
				continue;
			}
			
			// open socket to connect to namenode
			if ((sockfd = connectTCP_server(server_ip, server_port)) == 0)
			{
				//perror("Connection to namenode:");				
				printf("DEBUG: name server number %d is offline, switching to next redundant server",current_redundant_server);
				current_redundant_server=(current_redundant_server+1)%max_redundancy;
				fullfilled_choice=0;
				continue;
			}

			//Connected fine to one of the servers, so give the other one a chance to come back online if it was down
			//In other words, no longer assume it is down if it was previously
			num_redundancy_tries=0;
			fullfilled_choice=1;
			
			memset(buf, 0, sizeof(buf));
		
			// pack the register data
			packetsize = pack(buf, "hs",(int16_t)2, query_string); //2: query
				
			// send
			if ((numbytes = send(sockfd, buf, packetsize, MSG_NOSIGNAL)) == -1)
			{
				perror("send to namenode");
				continue;
			}
			
			printf("\nWaiting for reply\n...");
			
			if ((numbytes = recv(sockfd, buf, MAX_BUF_LEN, 0)) == -1)
			{
				perror("recevie from server");
				continue;
			}
			
			doc_num = unpack_string_array(buf, &doc, MAX_WORD_LEN);
			printf("Number of document is: %d\n", doc_num);
			for (i=0; i< doc_num; i++)
				printf("%d. %s\n",i, doc[i]);
					
			close(sockfd);
			// end = get_timestamp();
			// average[count-1] = end - begin;
			// real_average += end - begin; 
			// }
			
			// for (i=0; i< times; i++)
				// standard += (average[i]-real_average)*(average[i]-real_average);
			// standard = standard/times;
			// standard = sqrt(standard);
		
			// printf("Average time for querying is: %ld, deviration is: %ld\n", real_average/1000, standard/1000);
		}
	}
	
	return 0;
}