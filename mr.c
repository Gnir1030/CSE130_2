#include "mr.h"
#define _POSIX_C_SOURCE 200809L
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <math.h>

#include "hash.h"
#include "kvlist.h"

typedef struct
{
    mapper_t mapper;
    kvlist_t* input;
    kvlist_t* output;
} Arg;

pthread_mutex_t mutex;

void *maplist(void* multiple_arg){
    Arg* arg = (Arg*)multiple_arg;
    kvlist_iterator_t* iter = kvlist_iterator_new(arg->input);
    kvpair_t* pair;
    while( (pair = kvlist_iterator_next(iter)) != NULL){
        pthread_mutex_lock(&mutex);
        arg->mapper(pair, arg->output);
        pthread_mutex_unlock(&mutex);
    }
    kvlist_iterator_free(&iter);
    return NULL;
}

void map_reduce(mapper_t mapper, size_t num_mapper, reducer_t reducer,
                size_t num_reducer, kvlist_t* input, kvlist_t* output) {
                    //kvlist_print(1, input);

                    //split phase
                    kvlist_t* lstArray[num_mapper]; //splited list array
                    size_t input_size = 0; //size of input
                    kvlist_iterator_t* iter = kvlist_iterator_new(input);
                    while (kvlist_iterator_next(iter) != NULL){
                        input_size++;
                    }
                    kvlist_iterator_free(&iter);


                    size_t split = (input_size/num_mapper) + (input_size/num_mapper < 1 ? 1:0); //size of splited list

                    kvlist_iterator_t* iter2 = kvlist_iterator_new(input);
                    for(size_t i = 0; i < num_mapper -1; i++){
                        lstArray[i] = kvlist_new();
                        size_t counter = split;
                        while(counter != 0){
                            kvlist_append(lstArray[i], kvpair_clone(kvlist_iterator_next(iter2)));
                            counter--;
                        }
                    } //before last element

                    lstArray[num_mapper-1] = kvlist_new();
                    kvpair_t *pair;
                    while( (pair = kvlist_iterator_next(iter2)) != NULL){
                        kvlist_append(lstArray[num_mapper-1], kvpair_clone(pair));
                        //dprintf(1, "%s, ", pair->value);
                    }// put the rest in the last
                    kvlist_iterator_free(&iter2);

                    //test split
                    for(size_t i = 0; i < num_mapper; i++){
                        dprintf(1, "%luth list :\n", i);
                        kvlist_print(1, lstArray[i]);
                    }

                    //map phase                    
                    Arg *multiple_arg[num_mapper]; //wrapping parameters for maplist
                    //kvlist_t* mapArray[num_mapper]; //mapped list array
                    kvlist_t* mapped = kvlist_new();
                    pthread_t thr[num_mapper];
                    pthread_mutex_init(&mutex, NULL);
                    for(size_t i = 0; i< num_mapper; i++){
                        multiple_arg[i] = (Arg*)malloc(sizeof(Arg));
                        //mapArray[i] = kvlist_new();
                        multiple_arg[i]->mapper = mapper;
                        multiple_arg[i]->input = lstArray[i];
                        multiple_arg[i]->output = mapped;
                        pthread_create(&thr[i], NULL, maplist, (void *)multiple_arg[i]);
                    }

                    for(size_t i = 0; i< num_mapper; i++){
                        pthread_join(thr[i], NULL);
                        free(multiple_arg[i]);
                        kvlist_free(&lstArray[i]);
                    }

                    pthread_mutex_destroy(&mutex);
                    kvlist_sort(mapped);
                    kvlist_print(1,mapped);

//shuffle
                    kvlist_iterator_t* iter_mapped = kvlist_iterator_new(mapped);
                    kvpair_t* pair_mapped;
                    int count = 0;
                    while((pair_mapped = kvlist_iterator_next(iter_mapped)) != NULL){
                        //kvpair_t* pair_grouped;
                        while(hash(pair_mapped->key) == hash(kvlist_iterator_next(iter_mapped)->key)){

                        }
                        count++;
                    }

                    for(size_t i = 0; i< num_mapper; i++){
                        //dprintf(1, "%luth mapped list :\n", i);
                        //kvlist_print(1, mapArray[i]);
                        //kvlist_free(&mapArray[i]);
                    }

                    dprintf(1, "%d group\n", count);
                    //Shuffle phase
                    


                    kvlist_t* list = kvlist_new();
                    dprintf(1, "%lu, %lu, %lu\n", num_mapper+ num_reducer, input_size, split);
                    reducer("great",list, output);
                    kvlist_free(&list);
    }
