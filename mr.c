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

typedef struct
{
    reducer_t reducer;
    kvlist_t* input;
    kvlist_t* output;
} Arg2;

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

void *redlist(void* multiple_arg){
    Arg2* arg = (Arg2*)multiple_arg;
    kvlist_iterator_t* iter = kvlist_iterator_new(arg->input);
    kvpair_t* pair = kvlist_iterator_next(iter);
    char* past_key;
    while(pair != NULL){
        past_key = pair->key;
        kvlist_t* list = kvlist_new();
        while(pair != NULL && strcmp(past_key, pair->key) == 0){
            kvlist_append(list, kvpair_clone(pair));
            pair = kvlist_iterator_next(iter);
        }
        arg->reducer(past_key, list, arg->output);
        kvlist_free(&list);
    }
    kvlist_iterator_free(&iter);
    return NULL;
}

void map_reduce(mapper_t mapper, size_t num_mapper, reducer_t reducer,
                size_t num_reducer, kvlist_t* input, kvlist_t* output) {
                    //kvlist_print(1, input);

                    //split phase
                    kvlist_t* lstArray[num_mapper]; //splited list array
                    for(size_t i = 0; i < num_mapper; i++){
                         lstArray[i] = kvlist_new();
                    }

                    kvlist_iterator_t* iter = kvlist_iterator_new(input);
                    kvpair_t* raw_pair;
                    size_t index = 0;
                    while((raw_pair = kvlist_iterator_next(iter)) != NULL){
                        kvlist_append(lstArray[index], kvpair_clone(raw_pair));
                        index++;
                        if(index >= num_mapper){
                            index = 0;
                        }
                    }

                    kvlist_iterator_free(&iter);
                    
                    //test split
                    for(size_t i = 0; i < num_mapper; i++){
                        dprintf(1, "%luth list :\n", i);
                        kvlist_print(1, lstArray[i]);
                    }

                    //map phase                    
                    Arg *multiple_arg[num_mapper]; //wrapping parameters for maplist
                    kvlist_t* mapArray[num_mapper]; //mapped list array
                    //kvlist_t* mapped = kvlist_new();
                    pthread_t thr[num_mapper];

                    for(size_t i = 0; i< num_mapper; i++){
                        multiple_arg[i] = (Arg*)malloc(sizeof(Arg));
                        mapArray[i] = kvlist_new();
                        multiple_arg[i]->mapper = mapper;
                        multiple_arg[i]->input = lstArray[i];
                        multiple_arg[i]->output = mapArray[i];
                        //multiple_arg[i]->output = mapped;
                        pthread_create(&thr[i], NULL, maplist, (void *)multiple_arg[i]);
                    }

                    for(size_t i = 0; i< num_mapper; i++){
                        pthread_join(thr[i], NULL);
                        free(multiple_arg[i]);
                        kvlist_free(&lstArray[i]);
                    }

                    //kvlist_sort(mapped);
                    for(size_t i = 0; i < num_mapper; i++){
                        kvlist_print(1, mapArray[i]);
                    }

                    //shuffle
                    // kvlist_iterator_t* iter_mapped = kvlist_iterator_new(mapped);
                    // kvpair_t* pair_mapped;
                    // kvpair_t* pair_first;
                    // pair_mapped = kvlist_iterator_next(iter_mapped);
                    // int count = 0;
                    // while(pair_mapped != NULL){
                    //     pair_first = pair_mapped;
                    //     while(pair_mapped != NULL && strcmp(pair_first->key, pair_mapped->key) == 0){
                    //         dprintf(1, "%s, %s\n", pair_first->key, pair_mapped->key);
                    //         dprintf(1, "%lu, %lu\n", hash(pair_first->key), hash(pair_mapped->key));
                    //         pair_mapped = kvlist_iterator_next(iter_mapped);
                    //     }
                    //     count++;
                    // }

                    //Shuffle phase
                    kvlist_t* shuffle[num_reducer];
                    for(size_t i = 0; i< num_reducer; i++ ){
                        shuffle[i] = kvlist_new();
                    }

                    for(size_t i = 0; i < num_mapper; i++){
                        kvlist_iterator_t* iter_mapped = kvlist_iterator_new(mapArray[i]);
                        kvpair_t* pair_mapped;

                        while((pair_mapped = kvlist_iterator_next(iter_mapped)) != NULL){
                            int index = hash(pair_mapped->key)%num_reducer;
                            kvlist_append(shuffle[index], kvpair_clone(pair_mapped));
                        }
                        kvlist_iterator_free(&iter_mapped);
                        kvlist_free(&mapArray[i]);
                    }

                    for(size_t i = 0; i< num_reducer; i++){
                        dprintf(1, "%luth shuffled list :\n", i);
                        kvlist_sort(shuffle[i]);
                        kvlist_print(1, shuffle[i]);
                    }
                    
                    //kvlist_free(&mapped);

                    kvlist_t* redArray[num_reducer];
                    Arg2 *multiple_arg2[num_reducer];
                    pthread_t thr2[num_reducer];
                    for(size_t i = 0; i< num_reducer; i++){
                        multiple_arg2[i] = (Arg2*)malloc(sizeof(Arg2));
                        redArray[i] = kvlist_new();
                        multiple_arg2[i]->reducer = reducer;
                        multiple_arg2[i]->input = shuffle[i];
                        multiple_arg2[i]->output = redArray[i];
                        pthread_create(&thr2[i], NULL, redlist, (void *)multiple_arg2[i]);
                    }


                    for(size_t i = 0; i< num_reducer; i++){
                        pthread_join(thr2[i], NULL);
                        free(multiple_arg2[i]);
                        kvlist_free(&shuffle[i]);
                    }


                    for(size_t i = 0; i< num_reducer; i++){
                        kvlist_extend(output, redArray[i]);
                        kvlist_free(&redArray[i]);
                    }
    }
