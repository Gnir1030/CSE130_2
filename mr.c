#include "mr.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <math.h>

#include "hash.h"
#include "kvlist.h"

void map_reduce(mapper_t mapper, size_t num_mapper, reducer_t reducer,
                size_t num_reducer, kvlist_t* input, kvlist_t* output) {
                    //split phase
                    kvlist_t* lstArray[num_mapper];
                    size_t input_size = 0;
                    kvlist_iterator_t* iter = kvlist_iterator_new(input);
                    while (kvlist_iterator_next(iter) != NULL){
                        input_size++;
                    }
                    kvlist_iterator_free(&iter);
                    size_t split = input_size/num_mapper;

                    kvlist_iterator_t* iter2 = kvlist_iterator_new(input);
                    for(size_t i = 0; i < num_mapper; i++){
                        lstArray[i] = kvlist_new();
                        size_t counter = split;
                        while(counter != 0){
                            kvlist_append(lstArray[i], kvlist_iterator_next(iter2));
                            counter--;
                        }
                    }
                    kvlist_iterator_free(&iter2);

                    kvlist_print(1, lstArray[0]);

                    kvlist_iterator_t* itor = kvlist_iterator_new(input);
                    kvlist_t* list = kvlist_new();
                    for(;;){
                        kvpair_t* pair = kvlist_iterator_next(itor);
                        if(pair == NULL){
                            break;
                        }
                        mapper(pair, list);
                    }
                    kvlist_print(1, list);
                    kvlist_iterator_free(&itor);
                    printf("%lu, %lu, %lu\n", num_mapper+ num_reducer, input_size, split);
                    reducer("great",list, output);
                    kvlist_free(&list);
    }
