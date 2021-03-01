#include<stdio.h>
#include<string.h>
#include<stdlib.h>
#include<errno.h>

#include "bst.h"
#include "pds.h"

struct PDS_RepoInfo repo_handle;

int pds_open(char *repo_name, int rec_size)
{
	char repo_file[30];
    char ndx_file[30];

	if(repo_handle.repo_status == PDS_REPO_OPEN)
		return PDS_REPO_ALREADY_OPEN;

	strcpy(repo_handle.pds_name,repo_name);

	strcpy(repo_file,repo_name);
	strcat(repo_file,".dat");
    strcpy(ndx_file,repo_name);
    strcat(ndx_file,".ndx");

	repo_handle.pds_data_fp = (FILE *) fopen(repo_file,"rb+");
	if(repo_handle.pds_data_fp == NULL){
		perror(repo_file);
	}

    repo_handle.pds_ndx_fp = (FILE *) fopen(ndx_file,"rb+");
    if(repo_handle.pds_ndx_fp == NULL){
        perror(ndx_file);
    }

	repo_handle.repo_status = PDS_REPO_OPEN;
	repo_handle.rec_size = rec_size;
	repo_handle.pds_bst = NULL;

	int status = pds_load_ndx();
    
    
    fclose(repo_handle.pds_ndx_fp);
	return status;
}

int pds_load_ndx(){
    int i = 0;
    int status = PDS_SUCCESS;
    while(1){
        struct PDS_NdxInfo* ndx = (struct PDS_NdxInfo*)malloc(sizeof(struct PDS_NdxInfo));
        if(!fread(ndx,sizeof(struct PDS_NdxInfo), 1, repo_handle.pds_ndx_fp))
            break;
        ++i;  
        int key = ndx -> key;
        //printf("%d ", ndx->key);
        status = bst_add_node(&repo_handle.pds_bst,key,ndx);
    }
    //printf("BST CREATED %d records found\n",i);
    if(status != BST_SUCCESS) return PDS_NDX_SAVE_FAILED;
    return PDS_SUCCESS;
}

int put_rec_by_key(int key, void* rec){
    int offset, status, writesize;
    struct PDS_NdxInfo* ndx_entry;

    fseek(repo_handle.pds_data_fp,0,SEEK_END);
    offset = ftell(repo_handle.pds_data_fp);

    ndx_entry = (struct PDS_NdxInfo* ) malloc (sizeof(struct PDS_NdxInfo));
    ndx_entry -> key = key;
    ndx_entry -> offset = offset;
    
    status = bst_add_node(&repo_handle.pds_bst,key,ndx_entry);

    if(status != BST_SUCCESS){
        printf("Unable to add index entry for the key %d Error - %d\n", key, status);
        free(ndx_entry);
        status = PDS_ADD_FAILED;
    }

    else{
        status = PDS_SUCCESS;
        writesize = fwrite(rec,repo_handle.rec_size, 1, repo_handle.pds_data_fp);
    }

    return status;
}

int get_rec_by_key(int key, void* rec){
    struct PDS_NdxInfo* ndx_entry;
    struct BST_Node* bst_node;

    int offset, status, readsize;
    bst_node = bst_search(repo_handle.pds_bst,key);
    if(bst_node == NULL){
        //printf("Not found \n");
        status = PDS_REC_NOT_FOUND;
    }

    else{
        ndx_entry = (struct PDS_NdxINfo*) bst_node -> data;
        offset = ndx_entry -> offset;
        fseek(repo_handle.pds_data_fp, offset, SEEK_SET);
        readsize = fread(rec,repo_handle.rec_size,1,repo_handle.pds_data_fp);
        //printf("Found %d \n", ndx_entry -> key);
        status = PDS_SUCCESS;
    }

    return status;
}

//int cnt = 0;
void PreOrder(struct BST_Node* node){
    if(node==NULL) return;
    
    struct PDS_NdxInfo* ndx = (struct PDS_NdxInfo*) node -> data;
    fwrite(ndx, sizeof(struct PDS_NdxInfo), 1, repo_handle.pds_ndx_fp);
    //++cnt; printf("%d ",ndx->key);
    
    if(node->left_child!=NULL) PreOrder(node->left_child);
    if(node->right_child!=NULL) PreOrder(node->right_child);
}

int pds_close(){
    char ndx_file[30];
    strcpy(ndx_file,repo_handle.pds_name);
    strcat(ndx_file,".ndx");

    repo_handle.pds_ndx_fp = (FILE*)fopen(ndx_file,"wb");
    if(repo_handle.pds_ndx_fp == NULL){
    	perror(ndx_file);
	}

    if(repo_handle.pds_bst != NULL){
        //cnt = 0;
        PreOrder(repo_handle.pds_bst);
       // printf("%d records added\n", cnt);
    }
    bst_destroy(repo_handle.pds_bst);

    strcpy(repo_handle.pds_name,"");
    fclose(repo_handle.pds_data_fp);
    fclose(repo_handle.pds_ndx_fp);
    
    repo_handle.repo_status = PDS_REPO_CLOSED;
    
    return PDS_SUCCESS;
}