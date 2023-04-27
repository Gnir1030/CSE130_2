#include "mr.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "hash.h"
#include "kvlist.h"

void map_reduce(mapper_t mapper, size_t num_mapper, reducer_t reducer,
                size_t num_reducer, kvlist_t* input, kvlist_t* output) {
                    kvlist_t* list = kvlist_new();
                    kvlist_iterator_t* itor = kvlist_iterator_new(input);
                    for(;;){
                        kvpair_t* pair = kvlist_iterator_next(itor);
                        if(pair == NULL){
                            break;
                        }
                        mapper(pair, list);
                    }
                    kvlist_print(1, list);
                    kvlist_iterator_free(&itor);
                    printf("%lu", num_mapper+ num_reducer);
                    reducer("great",list, output);
                    kvlist_free(&list);
    }
