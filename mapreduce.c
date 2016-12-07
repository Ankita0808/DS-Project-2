#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>


///////////////////////////////////////////////////////

struct pair_t
{
	char word[50];
	char filename[300];
	int value;
	struct pair_t *prev;
	struct pair_t *next;
	struct pair_t *next2;
};

void update(struct pair_t *pair, char letter, char *resultURL);

void map(char *url, int offset, int size, char *intermediat_file)
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
		
		fprintf(fp, "%s %s %d\n", pair->word, url, pair->value);
		free(pair);
	}
	fclose(fp);
}


void reduce(char *URL, char letter, char *resultURL)
{
	struct pair_t *head = NULL;
	struct pair_t *tail = NULL;
	int i;
	
	DIR *directory = opendir(URL);
	if(directory == NULL)
	{
		printf("can not open directory %s\n", URL);
		return;
	}
	
	struct dirent *entry;
	while (entry = readdir(directory))
	{
		if(strcmp(entry->d_name, ".") == 0)
			continue;
		if(strcmp(entry->d_name, "..") == 0)
			continue;
		
		char filename[300];
		strcpy(filename, URL);
		strcat(filename, entry->d_name);
		
		FILE *fp;
		fp = fopen(filename, "r");
		if(fp==NULL)
		{
			printf("reduce: Open file %s failed.\n", filename); 
			return; 
		}

		// read the intermediate file
		char temp[50];
		char tempfilename[300];
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
}

///////////////////////////////////////////////////////

#define HASH_LENGTH 5000
#define MAX_PRIME_LESS_THAN_HASH_LEN 4999

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
	FILE *fp = fopen(full_name, "r+");
	if(fp == NULL)
	{
		printf("can not open IIT file %s\n", full_name);
	}
	
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
	
	/* read every line */
	char line[1000];
	while(fgets(line, 1000, fp) > 0)
	{
    
		char word[50];
		char filename[300];
		int value;
		char *token;
		
		token = strtok(line, " \n");
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
			(*doc)[i] = (*doc)[j];
			(*doc)[j] = str;
		}
	}

	return num_doc;
}

///////////////////////////////////////////////////////
/*
#define MAX_URL_LEN 300
#define MAX_URL_LEN 300
#define MAX_WORK 3000

struct work_table {
	char URL[MAX_URL_LEN];
	int state; //0: not done; 1: doing; 2: finished
	int offset;
	char mapFile[MAX_URL_LEN];
};

struct work_table work_table[MAX_WORK];

int work_total = 0;

void init(char *URL, int size)
{
	struct dirent *entry;
	FILE* fp;
	char filename[300];
	
	DIR *directory = opendir(URL);
	
	if(directory == NULL)
	{
		printf("can not open directory %s\n", URL);
		return;
	}
	
	while (entry = readdir(directory))
	{
		if(strcmp(entry->d_name, ".") == 0)
			continue;
		if(strcmp(entry->d_name, "..") == 0)
			continue;
		
		strcpy(filename, URL);
		strcat(filename, entry->d_name);
		fp = fopen(filename, "rb");
		if(fp==NULL)
		{
			printf("ERROR: Open file %s failed.\n", filename); 
			return; 
		}
		fseek( fp, SEEK_SET, SEEK_END ); 
		int fsize=ftell(fp); 
		fclose(fp);
		
		int offset = 0;
		while(fsize > 0)
		{
			work_table[work_total].offset = offset;
			strcpy(work_table[work_total].URL, filename);
			
			//////////////////////////////////////////////
			map(work_table[work_total].URL, work_table[work_total].offset, size, temp_result);
			//////////////////////////////////////////////
			
			work_total++;
			fsize = fsize - size;
			offset = offset + size;
		}
	}
    
    //////////////////////////////////////////////
    printf("reduce begin\n");
    reduce("result/", 'a', "result1/");
    printf("reduce done\n");
	//////////////////////////////////////////////
    
	closedir(directory);
}
*/
///////////////////////////////////////////////////////

int main()
{
    /**
	int i;
	char *filename[5];
	filename[0] = "file0.txt";
	filename[1] = "file1.txt";
	filename[2] = "file2.txt";
	filename[3] = "file3.txt";
	filename[4] = "file4.txt";

	for(i = 0; i < 5; i++)
	{
		map("book.txt", i * 9216, 9216, filename[i]);
	}
	for(i = 0; i < 5; i++)
	{
		map("book.txt", i * 9216, 9216, filename[i]);
	}
	
	reduce(filename, 5, "A", "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", "result.txt");

	//map("book.txt", 0, 1024 * 45, "inter-result.txt");
    **/
	
	//init("/afs/cs.pitt.edu/usr0/ankita/public/CS2510Prj2anm249/docs/", 1048576);
	/*
	printf("map1\n");
	 map("/afs/cs.pitt.edu/usr0/ankita/public/CS2510Prj2anm249/docs/", 0, 1048576, "/afs/cs.pitt.edu/usr0/ankita/public/CS2510Prj2anm249/intermediate_file/file0.txt");
	printf("map2\n");
	 map("/afs/cs.pitt.edu/usr0/ankita/public/CS2510Prj2anm249/docs/", 0, 2097152, "/afs/cs.pitt.edu/usr0/ankita/public/CS2510Prj2anm249/intermediate_file/file1.txt");
	printf("map3\n");
	 map("/afs/cs.pitt.edu/usr0/ankita/public/CS2510Prj2anm249/docs/", 0, 2097152, "/afs/cs.pitt.edu/usr0/ankita/public/CS2510Prj2anm249/intermediate_file/file2.txt");
	printf("map2\n");
	 map("/afs/cs.pitt.edu/usr0/ankita/public/CS2510Prj2anm249/docs/", 0, 2097152, "/afs/cs.pitt.edu/usr0/ankita/public/CS2510Prj2anm249/intermediate_file/file3.txt");
	printf("reduce\n");
	reduce("/afs/cs.pitt.edu/usr0/ankita/public/CS2510Prj2anm249/docs/", 'a', "/afs/cs.pitt.edu/usr0/ankita/public/CS2510Prj2anm249/inverted_index/");
	*/
	char *queryword[2];
	queryword[0] = "a";
	queryword[1] = "austere";
	char **rr;
	printf("query\n");
	int num = query(queryword, 2, "/afs/cs.pitt.edu/usr0/ankita/public/CS2510Prj2anm249/inverted_index/", &rr);
	int i;
	for(i = 0; i < num; i++)
	{
		printf("%s\n", rr[i]);
	}
	return 0;
}