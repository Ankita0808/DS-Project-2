/*
*	Helper
*	This Server includes Helper function: Register, Map, Reduce
*
*/

#include "network_function.h"

struct register_info
{
	char server_ip[100];
	char server_port[100];
};

struct pair_t
{
	char word[50];
	char filename[300];
	int value;
	struct pair_t *prev;
	struct pair_t *next;
	struct pair_t *next2;
};
struct letter_t
{
    char alphabet;
    
    struct word_t *words;
};

struct word_t
{
    char word[50];
    int valid;
    
    struct entry_t *head;
    struct entry_t *tail;
    
    struct word_t *conflict;
};

struct entry_t
{
    char filename[300];
    int value;
    
    struct entry_t *prev;
    struct entry_t *next;
};


/**************** REGISTER FUNCTION **********************/
int readNameNodeIP (char *server_ip, char *server_port, int redundancy)
{
	// read IP address and port from file define in header.h
	FILE *ptr_file;
	char buf[100];
	// read the global file
	if (redundancy==0)
		ptr_file =fopen(NAMENODE_FILENAME, "r");
	else
		ptr_file = fopen(NAMENODE_FILENAME2, "r");

	if (!ptr_file)
	{
		return 0;
		printf("no file open");
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

void *register_periodically(void *arg)
{
	char namenode_ip[100], namenode_port[100], buf[MAX_BUF_LEN];
	int sockfd, numbytes, packetsize, reply_code=100;
	int program_version;
	int redundancy=0;
	struct register_info *t;
		
	t = (struct register_info *) arg;
	// Prevent SIGPIPE
	signal (SIGPIPE, SIG_IGN);
	// read port mapper ip and port through file
	//readNameNodeIP(namenode_ip, namenode_port);
	//printf("Register to namenode \n");
	
	while (1)
	{
		readNameNodeIP(namenode_ip, namenode_port, redundancy);
		// open socket to connect to namenode
		if ((sockfd = connectTCP_server(namenode_ip, namenode_port)) == 0)
		{
			perror("Connection to namenode:");
			sleep(HELPER_REGISTER_TIME_OUT);
			redundancy=(redundancy+1)%2;
			continue;
		}
		
		memset(buf, 0, sizeof(buf));
		
		// pack the register data
		packetsize = pack(buf, "hss",(int16_t)1, t->server_ip, t->server_port); // 1: register
		
		// send
		if ((numbytes = send(sockfd, buf, packetsize,MSG_NOSIGNAL)) == -1)
		{
			perror("send to namenode");
			sleep(HELPER_REGISTER_TIME_OUT);
			redundancy=(redundancy+1)%2;

			continue;
		}
		close(sockfd);
		sleep(HELPER_REGISTER_TIME_OUT/2);
	} // end while
	
	pthread_exit(NULL);

}

/* register to the name node */
int sendRegister(char *server_ip, char *server_port)
{
	struct register_info *t;
	pthread_t checkTimeout_thread;
	
	t = (struct register_info *)malloc(sizeof(struct register_info));
	strcpy(t->server_ip, server_ip);
	strcpy(t->server_port, server_port);
	
	if (pthread_create(&checkTimeout_thread, NULL, register_periodically, (void *)t) != 0) //register_periodically (void *)t
	{
		printf("Failed to create thread to register\n");
		return 0;
	}
	return 1;
	
}



/**************** END REGISTER FUNCTION **********************/

/**************** MAP REDUCE FUNCTION ************************/

unsigned int hash(char *str, int len)
{
    unsigned int sum = 0; 
    unsigned int h = 0;
    char *p = (char *)str;
    char *s = (char *)str;

    while(p - s < len) 
    {
        unsigned short a = *(p++) * (p-s);
        sum += sum ^ a; 
        h += a;
    }
    return ((sum << 16) | h) % MAX_PRIME_LESS_THAN_HASH_LEN;
}

void inset_entry(struct word_t *word, struct entry_t *entry)
{
    if(word->head == NULL)
    {
        word->head = entry;
        word->tail = entry;
        return;
    }
    
    struct entry_t *temp = word->head;
    while((temp != NULL) && temp->value > entry->value)
    {
        temp = temp->next;
    }
    
    if(temp == NULL)
    {
        word->tail->next = entry;
        entry->prev = word->tail;
        word->tail = entry;
    }
    else if(temp == word->head)
    {
        temp->prev = entry;
        entry->next = temp;
        word->head = entry;
    }
    else
    {
        temp->prev->next = entry;
        entry->prev = temp->prev;
        temp->prev = entry;
        entry->next = temp;
    }
}

struct letter_t * openIIT(char *resultURL, char letter)
{
	// open IIT file
	char full_name[300];
	strcpy(full_name, resultURL);
	int index = strlen(full_name);
	full_name[index++] = letter;
	full_name[index++] = '.';
	full_name[index++] = 't';
	full_name[index++] = 'x';
	full_name[index++] = 't';
	full_name[index++] = '\0';
	
	// create letter
	struct letter_t *alpha = (struct letter_t *)malloc(sizeof(struct letter_t));
	alpha->alphabet = letter;
		
	// create hash table and initialize
    alpha->words = (struct word_t *)malloc(HASH_LENGTH * sizeof(struct word_t));
    int i;
    for(i = 0; i < HASH_LENGTH; i++)
    {
        alpha->words[i].valid = 0;
        alpha->words[i].head = NULL;
        alpha->words[i].tail = NULL;
        alpha->words[i].conflict = NULL;
    }
	
	FILE *fp = fopen(full_name, "r+");
	if(fp == NULL)
	{
		fp = fopen(full_name, "w+");
		//fputs("", fp);
	}
	/* read every line */
	char line[MAX_LINE_LEN];
	while(fgets(line, MAX_LINE_LEN, fp) > 0)
	{
		char word[MAX_WORD_LEN];
		char filename[MAX_BUF_LEN];
		int value;
		char *token;
		
		token = strtok(line, " \n");
		if (token == NULL) continue;
		strcpy(word, token);
		token = strtok(NULL, " \n");
		strcpy(filename, token);
		token = strtok(NULL, " \n");
		value = atoi(token);
		
		// create new entry
        struct entry_t *new_entry = (struct entry_t *)malloc(sizeof(struct entry_t));
        strcpy(new_entry->filename, filename);
        new_entry->value = value;
        new_entry->prev = NULL;
        new_entry->next = NULL;

		struct word_t *w;
        
        // put the entry in to the hash table
		unsigned int word_index = hash(word, strlen(word));
		if(alpha->words[word_index].valid == 0)
		{
			strcpy(alpha->words[word_index].word, word);
			alpha->words[word_index].valid = 1;
			alpha->words[word_index].head = new_entry;
			alpha->words[word_index].tail = new_entry;
			
			w = &alpha->words[word_index];
		}
		else
		{
			struct word_t *temp_word;
			struct word_t *last_word;
			for(temp_word = &(alpha->words[word_index]); temp_word != NULL; temp_word = temp_word->conflict)
			{
				last_word = temp_word;
				if(strcmp(temp_word->word, word) == 0)
					break;
			}

			// find word
			if(temp_word != NULL)
			{
				// insert it at tail
				temp_word->tail->next = new_entry;
				new_entry->prev = temp_word->tail;
				temp_word->tail = new_entry;
				
				w = temp_word;
			}
			// no such word
			else
			{
				struct word_t *new_word = (struct word_t *)malloc(sizeof(struct word_t));
				strcpy(new_word->word, word);
				new_word->valid = 1;
				new_word->head = new_entry;
				new_word->tail = new_entry;
				new_word->conflict = NULL;
				
				last_word->conflict = new_word;
				w = new_word;
			}
		}
     
		// there are other entries
		while(token = strtok(NULL, " \n"))
		{
			strcpy(filename, token);
			token = strtok(NULL, " \n");
			value = atoi(token);
			// create new entry
			struct entry_t *new_entry = (struct entry_t *)malloc(sizeof(struct entry_t));
			strcpy(new_entry->filename, filename);
			new_entry->value = value;
			new_entry->prev = NULL;
			new_entry->next = NULL;
			
			w->tail->next = new_entry;
			new_entry->prev = w->tail;
			w->tail = new_entry;
		}
	}
	fclose(fp);
	return alpha;
}

void writeIIT(char *resultURL, char letter, struct letter_t *lett)
{
	// open IIT file
	char full_name[300];
	strcpy(full_name, resultURL);
	int index = strlen(full_name);
	full_name[index++] = letter;
	full_name[index++] = '.';
	full_name[index++] = 't';
	full_name[index++] = 'x';
	full_name[index++] = 't';
	full_name[index++] = '\0';
	FILE *fp = fopen(full_name, "r+");
	if(fp == NULL)
	{
		printf("can not open IIT file %s\n", full_name);
	}
	
	int i;
	for(i = 0; i < HASH_LENGTH; i++)
	{
		struct word_t *word = &(lett->words[i]);
		while((word != NULL) && (word->valid == 1))
		{
			fprintf(fp, "%s", word->word);
			struct entry_t *entry = word->head;
			while(entry != NULL)
			{
				fprintf(fp, " %s %d", entry->filename, entry->value);
				
				struct entry_t *e = entry;
				entry = entry->next;
				free(e);
			}
			fprintf(fp, "\n");
			struct word_t *w = word;
			word = word->conflict;
			if(w != &(lett->words[i]))
				free(w);
		}
	}
	free(lett->words);
	free(lett);
	
	fclose(fp);
}

void update(struct pair_t *pair, char letter, char *resultURL)
{
	// open IIT file
	struct letter_t *alpha = openIIT(resultURL, letter);

	// every word
	struct pair_t * temp_pair;
	for(temp_pair = pair; temp_pair != NULL; temp_pair = temp_pair->next)
	{
		// every file name
		struct pair_t * tempP;
		for(tempP = temp_pair; tempP != NULL; tempP = tempP->next2)
		{
			unsigned int word_index = hash(tempP->word, strlen(tempP->word));
			
			struct word_t *word = &(alpha->words[word_index]);
			// not valid in hash table
			if(word->valid == 0)
			{
				// create new entry
				struct entry_t *new_entry = (struct entry_t *)malloc(sizeof(struct entry_t));
				strcpy(new_entry->filename, tempP->filename);
				new_entry->value = tempP->value;
				new_entry->prev = NULL;
				new_entry->next = NULL;
            
				// put the entry in to the hash table
				strcpy(word->word, tempP->word);
				word->valid = 1;
				word->head = new_entry;
				word->tail = new_entry;
			}
			// there is something in the hash table
			else
			{
				struct word_t *last_word = NULL;
				while(word != NULL)
				{
					if(strcmp(word->word, tempP->word) == 0)
                    {
						break;
                    }
					last_word = word;
					word = word->conflict;
				}
            
				// find this word
				if(word != NULL)
				{
					struct entry_t *entry;
					for(entry = word->head; entry != NULL; entry = entry->next)
					{
						// file name match
						if(strcmp(entry->filename, tempP->filename) == 0)
						{
							entry->value = entry->value + tempP->value;
                    
							// first remove this entry from the list
							if(word->head == entry)
								word->head = entry->next;
							else
								entry->prev->next = entry->next;
							if(word->tail == entry)
								word->tail = entry->prev;
							else
								entry->next->prev = entry->prev;
							entry->prev = NULL;
							entry->next = NULL;
                        
							// move to proper place
							inset_entry(word, entry);
                    
							break;
						}
					}
					// no file name match
					if(entry == NULL)
					{
						// create new entry and initialize
						struct entry_t *new_entry = (struct entry_t *)malloc(sizeof(struct entry_t));
						new_entry->value = tempP->value;
						strcpy(new_entry->filename, tempP->filename);
						new_entry->prev = NULL;
						new_entry->next = NULL;
                    
						// put in proper place
						inset_entry(word, new_entry);
					}
				}
				// no such word
				else
				{
					// create new word and initialize
					struct word_t *new_word = (struct word_t *)malloc(sizeof(struct word_t));
					new_word->valid = 1;
					strcpy(new_word->word, tempP->word);
					new_word->conflict = NULL;
					last_word->conflict = new_word;
                
					// create new entry and initialize
					struct entry_t *new_entry = (struct entry_t *)malloc(sizeof(struct entry_t));
					new_entry->value = tempP->value;
					strcpy(new_entry->filename, tempP->filename);
					new_entry->prev = NULL;
					new_entry->next = NULL;
                
					new_word->head = new_entry;
					new_word->tail = new_entry;
				}
			}
		}
	}
	
	writeIIT(resultURL, letter, alpha);
}

int map(char *url, int offset, int size, char *intermediat_file)
{
	int index = offset;
	int count = size;
	char c = '\0';
	int t = 0;
	char temp[50];
	struct pair_t *pair;
	struct pair_t *head1 = NULL;
	struct pair_t *tail1 = NULL;
	struct pair_t *head2 = NULL;
	struct pair_t *tail2 = NULL;
	struct pair_t *iter;
	int skip_first_one;
	
	FILE *fp;
	fp = fopen(url, "r");
	if(fp == NULL)
	{
		printf("map: can not open input file %s\n", url);
		return;
	}
	
	// find start position
	while(index > 0)
	{
		c = fgetc(fp);
		index--;
	}
	
	if((c >= 97 && c <= 122) || (c >= 65 && c <= 90))
		skip_first_one = 1;
	else
		skip_first_one = 0;

	// read "size" letters
	while(count > 0)
	{
		c = fgetc(fp);
		count--;
		
		// a - z, A - Z
		if((c >= 97 && c <= 122) || (c >= 65 && c <= 90))
		{
			// change to lower case
			if(c >= 65 && c <= 90)
				c = c + 32;
			
			temp[t++] = c;
		}
		else
		{
			temp[t] = '\0';
			t = 0;
			
			if(skip_first_one == 1)
			{
				skip_first_one = 0;
				continue;
			}
			
			// generate new key-value pair
			if(temp[0] != '\0')
			{
				pair = (struct pair_t *)malloc(sizeof(struct pair_t));
				strcpy(pair->word, temp);
				pair->value = 1;
				pair->prev = NULL;
				pair->next = NULL;
			
				// put into first queue
				if(head1 == NULL)
				{
					head1 = pair;
					tail1 = pair;
				}
				else
				{
					tail1->next = pair;
					pair->prev = tail1;
					tail1 = pair;
				}
			}
		}
	}

	// continue reading until the last word completes
	if(t != 0)
	{
		for(c = fgetc(fp); (c >= 97 && c <= 122) || (c >= 65 && c <= 90); c = fgetc(fp))
		{
			// change to lower case
			if(c >= 65 && c <= 90)
				c = c + 32;

			temp[t++] = c;
		}
		temp[t] = '\0';
		pair = (struct pair_t *)malloc(sizeof(struct pair_t));
		strcpy(pair->word, temp);
		pair->value = 1;
		pair->prev = NULL;
		pair->next = NULL;
		
		// put into first queue
		if(head1 == NULL)
		{
			head1 = pair;
			tail1 = pair;
		}
		else
		{
			tail1->next = pair;
			pair->prev = tail1;
			tail1 = pair;
		}
	}
	
	fclose(fp);

	// combining
	while(head1 != NULL)
	{
		pair = head1;
		head1 = head1->next;
		pair->prev = NULL;
		pair->next = NULL;
		
		// the first one
		if(head2 == NULL)
		{
			head2 = pair;
			tail2 = pair;
			continue;
		}
		
		// go through the second queue
		for(iter = head2; iter != NULL; iter = iter->next)
		{
			// match, combine
			if(strcmp(pair->word, iter->word) == 0)
			{
				iter->value++;
				free(pair);
				break;
			}
			// not match, insert into proper place
			else if(strcmp(pair->word, iter->word) < 0)
			{
				if(head2 == iter)
				{
					pair->next = iter;
					iter->prev = pair;
					head2 = pair;
				}
				else
				{
					iter->prev->next = pair;
					pair->prev = iter->prev;
					pair->next = iter;
					iter->prev = pair;
				}
				break;
			}
		}
		if(iter == NULL)
		{
			tail2->next = pair;
			pair->prev = tail2;
			tail2 = pair;
		}
	}

	// write into intermediate file
	fp = fopen(intermediat_file, "w");
	if(fp == NULL)
	{
		printf("map: can not open output file %s\n", intermediat_file);
		while(head2 != NULL)
		{
			pair = head2;
			head2 = head2->next;
			free(pair);
		}
		return;
	}
	while(head2 != NULL)
	{
		pair = head2;
		head2 = head2->next;
		
		char *token = strtok(url, " /");
		char name[MAX_WORD_LEN];
		while(token = strtok(NULL, " /"))
		{
			strcpy(name, token);
		}

		fprintf(fp, "%s %s %d\n", pair->word, name, pair->value);
		free(pair);
	}
	fclose(fp);
}

int reduce(char *URL, char letter, char *resultURL)
{
	struct pair_t *head = NULL;
	struct pair_t *tail = NULL;
	int i;
	
	DIR *directory = opendir(URL);
	if(directory == NULL)
	{
		printf("can not open directory %s\n", URL);
		return 0;
	}
	
	struct dirent *entry;
	while (entry = readdir(directory))
	{
		if(strcmp(entry->d_name, ".") == 0)
			continue;
		if(strcmp(entry->d_name, "..") == 0)
			continue;
		
		char filename[MAX_URL_LEN];
		strcpy(filename, URL);
		strcat(filename, entry->d_name);
		
		FILE *fp;
		fp = fopen(filename, "r");
		if(fp==NULL)
		{
			//printf("reduce: Open file %s failed.\n", filename); 
			return 0; 
		}
		//printf("DEBUG: done reading 1 file\n");
		// read the intermediate file
		char temp[MAX_LINE_LEN];
		char tempfilename[MAX_URL_LEN];
		int value;
		int num = fscanf(fp, "%s %s %d", &temp, & tempfilename, &value);
		while((num != EOF) && (num > 0))
		{
			// over
            if(temp[0] < letter)
            {
                num = fscanf(fp, "%s %s %d", &temp, & tempfilename, &value);
                continue;
            }
			if(temp[0] > letter)
				break;
		
			// first one
			if(head == NULL)
			{
				struct pair_t *pair = (struct pair_t *)malloc(sizeof(struct pair_t));
				strcpy(pair->word, temp);
				strcpy(pair->filename, tempfilename);
				pair->value = value;
				pair->prev = NULL;
				pair->next = NULL;
				pair->next2 = NULL;
				head = pair;
				tail = pair;
			}
			else
			{
				// go through the queue
				struct pair_t *iter;
				for(iter = head; iter != NULL; iter = iter->next)
				{
					// match
					if(strcmp(temp, iter->word) == 0)
					{
						// file name also match
						struct pair_t *last_pair;
						struct pair_t *iter2;
						for(iter2 = iter; iter2 != NULL; iter2 = iter2->next2)
						{
							last_pair = iter2;
							if(strcmp(tempfilename, iter2->filename) == 0)
							{
								iter2->value = iter2->value + value;
								break;
							}
						}
						// new file name
						if(iter2 == NULL)
						{
							struct pair_t *pair = (struct pair_t *)malloc(sizeof(struct pair_t));
							strcpy(pair->word, temp);
							strcpy(pair->filename, tempfilename);
							pair->value = value;
							pair->prev = NULL;
							pair->next = NULL;
							pair->next2 = NULL;
							
							last_pair->next2 = pair;
						}
						break;
					}
					// not match, insert into proper place
					else if(strcmp(temp, iter->word) < 0)
					{
						struct pair_t *pair = (struct pair_t *)malloc(sizeof(struct pair_t));
						strcpy(pair->word, temp);
						strcpy(pair->filename, tempfilename);
						pair->value = value;
						pair->prev = NULL;
						pair->next = NULL;
						pair->next2 = NULL;
					
						if(head == iter)
						{
							pair->next = iter;
							iter->prev = pair;
							head = pair;
						}
						else
						{
							iter->prev->next = pair;
							pair->prev = iter->prev;
							pair->next = iter;
							iter->prev = pair;
						}
						break;
					}
				}
				if(iter == NULL)
				{
					struct pair_t *pair = (struct pair_t *)malloc(sizeof(struct pair_t));
					strcpy(pair->word, temp);
					strcpy(pair->filename, tempfilename);
					pair->value = value;
					pair->prev = NULL;
					pair->next = NULL;
					pair->next2 = NULL;
				
					tail->next = pair;
					pair->prev = tail;
					tail = pair;
				}
			}
			
			num = fscanf(fp, "%s %s %d", &temp, & tempfilename, &value);
		}
		
		fclose(fp);
	}
	closedir(directory);
	
	//DEK_TODO: Potentially a problem here- do not want it to actually update if main server crashes
	update(head, letter, resultURL);

	// free
	while(head != NULL)
	{
		struct pair_t *pair = head;
		head = head->next;
		
		struct pair_t *iter3 = pair->next2;
		while(iter3 != NULL)
		{
			struct pair_t *temppair = iter3;
			iter3 = iter3->next2;
			
            free(temppair);
		}
		free(pair);
	}
	
	return 1;
}

int query(char *query_word[], int n, char *IIURL, char ***doc)
{
	int i;
	
	// store result
	struct entry_t **entries = (struct entry_t **)malloc(n * sizeof(struct entry_t *));
	for(i = 0; i < n; i++)
	{
		entries[i] = NULL;
	}
	
	//query every word
	for(i = 0; i < n; i++)
	{
		char *target = query_word[i];
		// open the IIT file
		struct letter_t *letter = openIIT(IIURL, target[0]);
		struct word_t *word;
		int j;
		for(j = 0; j < HASH_LENGTH; j++)
		{
			word = &(letter->words[j]);
			while(word != NULL)
			{
				// found word
				if(word->valid && (strcmp(word->word, target) == 0))
				{
					break;
				}
				
				word = word->conflict;
			}
			if(word != NULL)
			{
				entries[i] = word->head;
				break;
			}
		}
	}
	*doc = (char **)malloc(1000*sizeof(char *));
	int *sum = (int *)malloc(1000*sizeof(int));
	int num_doc = 0;
	
	// go through first list
	struct entry_t *entry = entries[0];
	while(entry != NULL)
	{
		int total = entry->value;
		
		// for each other list
		int found = 1;
		for(i = 1; i < n; i++)
		{
			// go through that list
			struct entry_t *temp = entries[i];
			while(temp != NULL)
			{
				if(strcmp(entry->filename, temp->filename)==0)
				{
					total = total + temp->value;
					break;
				}
				
				temp = temp->next;
			}
			// not match for this list
			if(temp == NULL)
			{
				found = 0;
				break;
			}
		}
		if(found == 1)
		{
			char *str = (char *)malloc(300*sizeof(char));
			strcpy(str, entry->filename);
			(*doc)[num_doc] = str;
			sum[num_doc] = total;
			num_doc++;
		}
		
		entry = entry->next;
	}
	// reorder
	for(i = 0; i < num_doc; i++)
	{
		int max = sum[i];
		int index = -1;
		int j;
		for(j = i+1; j < num_doc; j++)
		{
			if(sum[j] > max)
			{
				max = sum[j];
				index = j;
			}
		}
		if(index != -1)
		{
			int swap = sum[i];
			sum[i] = sum[index];
			sum[index] = swap;
			
			char *str = (*doc)[i];
			(*doc)[i] = (*doc)[index];
			(*doc)[index] = str;
		}
	}
	return num_doc;
}

/**************** END MAP REDUCE FUNCTION ********************/

/**************** TASK FUNCTION ******************************/
int receiveTask(int sockfd, char *helper_ip, char *helper_port)
{
	int  clientfd, work_id, numbytes, packetsize, connfd;
	char buf[MAX_BUF_LEN], url[MAX_URL_LEN],server_ip[100], server_port[100], intermediate_file[MAX_URL_LEN];
	char tmp_char[MAX_URL_LEN], ii[MAX_URL_LEN], **words, **doc, letter;
	long int segment_size, offset;
	int16_t task, word_num;;
	while (1)
	{
		connfd = accept(sockfd,NULL, NULL);
		if (connfd == -1)
		{
			perror("helper: accept");
		}	
		
		// receive task from master
		if (recv(connfd,buf,MAX_BUF_LEN,0) == -1)
		{
			perror ("receive error from master");
			break;
		}
		unpack(buf,"h", &task);
		switch (task)
		{
			case 1: // map task
				printf("Doing Map Task\n");
				unpack(buf,"hhsllsss", &task, &work_id, url, &offset, &segment_size, 
							server_ip, server_port, intermediate_file);
							
				char template[] = "/fileXXXXXX";
				mkstemp(template);
				strcat(intermediate_file,template);
				
				
				printf("url is: %s \n segment_size is %d\n offset is %d\n", url, segment_size, offset);
				map(url, offset, 102400, intermediate_file);
				
				// send back intermediate_file to master
				if ((clientfd = connectTCP_server(server_ip, server_port)) == 0)
				{
					perror("Connection to helper:");
					return 0;
				}
				memset(buf, 0 , sizeof(buf));
				// pack the register data: word_id, helper_ip, helper_port, map intermediate file
				packetsize = pack(buf, "hsss", work_id, helper_ip, helper_port, intermediate_file);
				// send
				if ((numbytes = send(clientfd, buf, packetsize,MSG_NOSIGNAL)) == -1)
				{
					perror("send to helper");
					pthread_exit(NULL);
				}
				close(clientfd);
				printf("Task done, helper is free now\n");
				break;
			
			case 2: // reduce task
				printf("Doing Reduce Task\n");
				unpack(buf,"hhssss", &task, &work_id, ii, server_ip, server_port, intermediate_file);
				
				strcat(intermediate_file,"/");
				
				letter = work_id+97;
				printf("Reduce word %c\n", letter);
				if (!reduce(intermediate_file, letter, ii)) break;
				// send back intermediate_file to master
				if ((clientfd = connectTCP_server(server_ip, server_port)) == 0)
				{
					perror("Connection to helper:");
					return 0;
				}
				memset(buf, 0 , sizeof(buf));
				// pack the register data: word_id, helper_ip, helper_port, map intermediate file
				packetsize = pack(buf, "hss", work_id, helper_ip, helper_port);
				// send
				if ((numbytes = send(clientfd, buf, packetsize,MSG_NOSIGNAL)) == -1)
				{
					perror("send to helper");
					pthread_exit(NULL);
				}
				close(clientfd);
				printf("Task done, helper is free now\n");
				break;
				
			case 3: //query task
				
				printf("Doing Query Task\n");
				unpack(buf,"hhh100s100s100s", &task, &work_id, &word_num, ii, server_ip, server_port );
				//printf("number of work_id is: %d\nIIT DIR is: %s\n", work_id, ii);
				unpack_string_array(buf+6+300, &words, MAX_WORD_LEN);
				
				// int i;
				// for (i=0; i< word_num; i++)
					// printf("Words to query are: %s\n",words[i]);
				
				int num = query(words, word_num, ii, &doc);
				
				printf("Number of doc is: %d\n", num);
				// for(i = 0; i < num; i++)
				// {
					// printf("%s\n", doc[i]);
				// }
				
				if ((clientfd = connectTCP_server(server_ip, server_port)) == 0)
				{
					perror("Connection to Server for Query Part:");
					return 0;
				}
				memset(buf, 0 , sizeof(buf));
				// pack the register data: word_id, helper_ip, helper_port, map intermediate file
				packetsize = pack(buf, "h100s100s", work_id, helper_ip, helper_port);
				packetsize += pack_string_array(buf+2+200, doc, 
													num, MAX_WORD_LEN);
				
				
				// send
				if ((numbytes = send(clientfd, buf, MAX_BUF_LEN, MSG_NOSIGNAL)) == -1)
				{
					perror("Send to Server, query part:");
					pthread_exit(NULL);
				}
				close(clientfd);
				printf("Task done, helper is free now\n");
				
				// For test only
				// packetsize = unpack_string_array(buf+2+200, &doc, MAX_WORD_LEN);
				// printf("Number of doc is: %d\n", packetsize);
				// for(i = 0; i < packetsize; i++)
				// {
					// printf("%s\n", doc[i]);
				// }
				
				
				free(words);
				free(doc);
				//query part
				
				break;
			default:
				// fault message
				break;
		}
		
	}
}

/**************** END TASK FUNCTION **************************/

int main()
{
	int sockfd;
	char helper_ip[100], helper_port[100];
	
	if ((sockfd = createTCP_server(helper_ip, helper_port)) == 0)
	{
		return -1;
	}
	sendRegister(helper_ip, helper_port);
	//sleep(3);
	receiveTask(sockfd, helper_ip, helper_port);
	return 0;
	
	
}
