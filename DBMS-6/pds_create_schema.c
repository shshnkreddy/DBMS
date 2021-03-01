#include<stdio.h>
#include<string.h>
#include<stdlib.h>
#include<errno.h>

#include "pds.h"

int main(int argc, char* argv[]){
    if(argc!=3){
        fprintf(stderr, "Usage: %s DBName schemaFileName \n", argv[0]);
        exit(1);
    }

    int status = pds_create_schema(argv[2]);
    return 0;
}