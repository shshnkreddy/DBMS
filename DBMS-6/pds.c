#include<stdio.h>
#include<string.h>
#include<stdlib.h>
#include<errno.h>

#include "bst.h"
#include "pds.h"

struct PDS_DB_Master db_handle;

int pos;

static int find_iter(char* name, int type){
    if(type == 0){
        for(int i = 0; i < db_handle.db_info.num_entities; ++i){
            if(strcmp(name,db_handle.entity_info[i].entity_name) == 0) return i;
        }
        return MAX_ENTITY;
    }
    else{
        for(int i = 0; i < db_handle.db_info.num_relationships; ++i){
            if(strcmp(name,db_handle.rel_info[i].link_name) == 0) return i;
        }
        return MAX_LINKS;
    }
}
    
int pds_create_schema (char *schema_file_name){
    FILE* fp = (FILE*) fopen(schema_file_name, "r");
    if(fp == NULL) perror(schema_file_name);

    char db_name[30];
    char db_file_name[30];
    fscanf(fp,"%s",db_name);
    strcpy(db_file_name,db_name);
    strcat(db_file_name,".db");

    FILE* db_fp = (FILE*) fopen(db_file_name,"wb");  //FIX
    if(db_fp == NULL) perror(db_file_name);

    struct PDS_DBInfo pds_dbinfo;
    int iter1 = 0, iter2 = 0;
    char input[30];
    for(int i = 0; i < MAX_ENTITY; ++i){
        pds_dbinfo.entities[i].entity_size = 0;
        strcpy(pds_dbinfo.entities[i].entity_name,"");
    }

    for(int i = 0; i < MAX_RELATIONSHIPS; ++i){
        strcpy(pds_dbinfo.links[i].link_name,"");
        strcpy(pds_dbinfo.links[i].pds_name1,"");
        strcpy(pds_dbinfo.links[i].pds_name2,"");
    }

    while(fscanf(fp,"%s",input)!=EOF){
        if(strcmp(input,"entity") == 0){
            char entity_name[30];
            int rec_size = 0;

            fscanf(fp,"%s %d", entity_name, &rec_size);

            struct PDS_EntityInfo pds_entity;
            strcpy(pds_entity.entity_name,entity_name);
            pds_entity.entity_size = rec_size;

            //printf("%s ", entity_name);

            pds_dbinfo.entities[iter1] = pds_entity;
            ++iter1;
        }
        else if(strcmp(input,"relationship") == 0){
            char entity1_name[30], entity2_name[30];
            char link_name[30];

            fscanf(fp,"%s %s %s", link_name, entity1_name, entity2_name);

            struct PDS_LinkInfo pds_link;
            strcpy(pds_link.link_name,link_name);
            strcpy(pds_link.pds_name1,entity1_name);
            strcpy(pds_link.pds_name2,entity2_name);

           // printf("%s %s %s", link_name, entity1_name, entity2_name);

            pds_dbinfo.links[iter2] = pds_link;
            ++iter2;
        }
        else return PDS_FILE_ERROR;
    }

    pds_dbinfo.num_entities = iter1;
    pds_dbinfo.num_relationships = iter2;

    int writesize = fwrite(&pds_dbinfo, sizeof(struct PDS_DBInfo), 1, db_fp);
    fclose(db_fp);

    return PDS_SUCCESS;
}

static int pds_link_open(char* link_name, struct PDS_LinkFileInfo* pds_link){
    strcpy(pds_link->link_name,link_name);
    char link_file_name[30];
    strcpy(link_file_name,link_name);
    strcat(link_file_name,".lnk");

    pds_link->pds_link_fp = (FILE*) fopen(link_file_name, "rb+"); // CHECK FILE MODE
    if(pds_link->pds_link_fp == NULL) perror(link_file_name);
    
    pds_link -> link_status = 1;     //FIX THIS
    for(int i = 0; i < MAX_FREE; ++i){       
        pds_link -> free_list[i] = -1;
    }             

    return PDS_SUCCESS;
}

int pds_open( char *repo_name, int rec_size ){
    char repo_file[30];
    char ndx_file[30];

    if(db_handle.entity_info[pos].repo_status == PDS_ENTITY_OPEN)
		return PDS_ENTITY_ALREADY_OPEN;

    strcpy(db_handle.entity_info[pos].entity_name,repo_name);

	strcpy(repo_file,repo_name);
	strcat(repo_file,".dat");
    strcpy(ndx_file,repo_name);
    strcat(ndx_file,".ndx");    

    db_handle.entity_info[pos].pds_data_fp = (FILE *) fopen(repo_file,"rb+");
	if(db_handle.entity_info[pos].pds_data_fp == NULL){
		perror(repo_file);
	}

    db_handle.entity_info[pos].pds_ndx_fp = (FILE *) fopen(ndx_file,"rb+");
    if(db_handle.entity_info[pos].pds_ndx_fp == NULL){
        perror(ndx_file);
    }

    db_handle.entity_info[pos].repo_status = PDS_ENTITY_OPEN;
	db_handle.entity_info[pos].entity_size = rec_size;
	db_handle.entity_info[pos].pds_bst = NULL;

	int status = pds_load_ndx(&(db_handle.entity_info[pos]));
    
    for(int i = 0; i < MAX_FREE; ++i){
        db_handle.entity_info[pos].free_list[i] = -1;
    }
    
    fclose(db_handle.entity_info[pos].pds_ndx_fp);
	return status;
}

int pds_db_open( char *db_name ){
	if(db_handle.db_status == PDS_DB_OPEN) return PDS_DB_ALREADY_OPEN;

    char db_file_name[30];
    strcpy(db_file_name,db_name);
    strcat(db_file_name,".db");

    FILE* db_schema_fp = (FILE*) fopen(db_file_name, "rb");
    if(db_schema_fp == NULL) perror(db_file_name);

    int readsize = fread(&(db_handle.db_info), sizeof(struct PDS_DBInfo), 1, db_schema_fp); 

    pos = 0;
    int status1 = 0;
    //printf("db_open : %d entities present\n", db_handle.db_info.num_entities);
    for(int i = 0; i < db_handle.db_info.num_entities; ++i){
        pos = i;
        status1 = pds_open(db_handle.db_info.entities[i].entity_name, db_handle.db_info.entities[i].entity_size);
        //printf("%s ", db_handle.db_info.entities[i].entity_name);
        if(status1 != PDS_SUCCESS) return PDS_FILE_ERROR;
    }
    //printf("\n");

    pos = 0;
    int status2 = 0;
    for(int i = 0; i < db_handle.db_info.num_relationships; ++i){
        pos = i;
        status2 = pds_link_open(db_handle.db_info.links[i].link_name, &db_handle.rel_info[i]);
        if(status2 != PDS_SUCCESS) return PDS_FILE_ERROR;
    }

    fclose(db_schema_fp);
    db_handle.db_status = PDS_DB_OPEN;
    return PDS_SUCCESS;
}

int pds_load_ndx( struct PDS_RepoInfo *repo_handle ){
	int status = PDS_SUCCESS;
    while(1){
        struct PDS_NdxInfo* ndx = (struct PDS_NdxInfo*)malloc(sizeof(struct PDS_NdxInfo));
        if(!fread(ndx,sizeof(struct PDS_NdxInfo), 1, repo_handle -> pds_ndx_fp))
            break;
        
        int key = ndx -> key;
        status = bst_add_node(&(repo_handle -> pds_bst),key,ndx);
    }
    if(status != BST_SUCCESS) return PDS_NDX_SAVE_FAILED;
    return PDS_SUCCESS;
}

int put_rec_by_key( char *entity_name, int key, void *rec ){
	int iter = find_iter(entity_name,0);
    if(iter == MAX_ENTITY){
        return PDS_ADD_FAILED;
    }

    int offset, status, writesize;
    struct PDS_NdxInfo* ndx_entry;

    int _free = 0;
    for(int i = 0; i < MAX_FREE; ++i){
        if(db_handle.entity_info[iter].free_list[i] != -1){
            offset = db_handle.entity_info[iter].free_list[i];
            _free = 1;
            db_handle.entity_info[iter].free_list[i] = -1;
            break;
        }
    }

    if(_free == 0){
        fseek(db_handle.entity_info[iter].pds_data_fp,0,SEEK_END);
        offset = ftell(db_handle.entity_info[iter].pds_data_fp);
    }

    ndx_entry = (struct PDS_NdxInfo* ) malloc (sizeof(struct PDS_NdxInfo));
    ndx_entry -> key = key;
    ndx_entry -> offset = offset;

    status = bst_add_node(&(db_handle.entity_info[iter].pds_bst),key,ndx_entry);
    
    if(status != BST_SUCCESS){
        printf("Unable to add index entry for the key %d Error - %d\n", key, status);
        free(ndx_entry);
        status = PDS_ADD_FAILED;
    }

    else{
        status = PDS_SUCCESS;
        fseek(db_handle.entity_info[iter].pds_data_fp, offset, SEEK_SET);
        int write_key = fwrite(&key,sizeof(int),1,db_handle.entity_info[iter].pds_data_fp);
        writesize = fwrite(rec,db_handle.entity_info[iter].entity_size, 1, db_handle.entity_info[iter].pds_data_fp);
    }

    return status;
}

int get_rec_by_ndx_key( char *entity_name, int key, void *rec ){
	int iter = find_iter(entity_name,0);
    if(iter == MAX_ENTITY) return PDS_REC_NOT_FOUND;
    
    struct PDS_NdxInfo* ndx_entry;
    struct BST_Node* bst_node;

    int offset, status, readsize;
    bst_node = bst_search(db_handle.entity_info[iter].pds_bst,key);
    if(bst_node == NULL){
        status = PDS_REC_NOT_FOUND;
    }

    else{
        ndx_entry = (struct PDS_NdxINfo*) bst_node -> data;
        offset = ndx_entry -> offset;
        fseek(db_handle.entity_info[iter].pds_data_fp, offset, SEEK_SET);
        int x;
        int read_key = fread(&x,sizeof(int),1,db_handle.entity_info[iter].pds_data_fp);
        readsize = fread(rec,db_handle.entity_info[iter].entity_size,1,db_handle.entity_info[iter].pds_data_fp);
        status = PDS_SUCCESS;
    }

    return status;
}

int get_rec_by_non_ndx_key( 
char* entity_name, // The entity file from which data is to be read */
void* key,  			// The search key */
void* rec,  			// The output record */
int (*matcher)(void *rec, void *key), /*Function pointer for matching*/
int* io_count  		//Count of the number of records read */
){
	int iter = find_iter(entity_name,0);
    if(iter == MAX_ENTITY) return PDS_REC_NOT_FOUND;

    fseek(db_handle.entity_info[iter].pds_data_fp,0,SEEK_SET);
    *io_count = 0;
    int x;
    
    while(fread(&x,sizeof(int),1,db_handle.entity_info[iter].pds_data_fp)){
        
        struct BST_Node* bst = bst_search(db_handle.entity_info[iter].pds_bst,x);
        if(bst != NULL) ++(*io_count);
        int writesize = fread(rec,db_handle.entity_info[iter].entity_size,1,db_handle.entity_info[iter].pds_data_fp);

        if(matcher(rec,key) == 0 && bst != NULL){         
            return PDS_SUCCESS;
        }
    }
   
    return PDS_REC_NOT_FOUND;
}

int update_by_key( char *entity_name, int key, void *newrec ){
	int iter = find_iter(entity_name,0);
    if(iter == MAX_ENTITY) return PDS_MODIFY_FAILED;

    struct BST_Node* bst = bst_search(db_handle.entity_info[iter].pds_bst, key);
    if(bst == NULL) return PDS_MODIFY_FAILED;

    struct PDS_NdxInfo* ndx_entry = (struct PDS_NdxInfo*) bst -> data; 
    int offset = ndx_entry -> offset;
    fseek(db_handle.entity_info[iter].pds_data_fp, offset, SEEK_SET);
    fwrite(&key,sizeof(int), 1, db_handle.entity_info[iter].pds_data_fp);
    fwrite(newrec,db_handle.entity_info[iter].entity_size,1,db_handle.entity_info[iter].pds_data_fp);

    return PDS_SUCCESS;
}

int delete_by_key( char *entity_name, int key ){
	int iter = find_iter(entity_name,0);
    if(iter == MAX_ENTITY) return PDS_DELETE_FAILED;

    struct BST_Node* bst = bst_search(db_handle.entity_info[iter].pds_bst, key);
    if(bst==NULL) return PDS_DELETE_FAILED;

    struct PDS_NdxInfo* ndx_entry = (struct PDS_NdxInfo*) bst -> data;
    int offset = ndx_entry -> offset;

    for(int i = 0; i < db_handle.db_info.num_relationships; ++i){
        if(!strcmp(db_handle.db_info.links[i].pds_name1, entity_name) || !strcmp(db_handle.db_info.links[i].pds_name2, entity_name)){
            struct PDS_Link link_read;
            fseek(db_handle.rel_info[i].pds_link_fp, 0, SEEK_SET);

            while(fread(&link_read, sizeof(struct PDS_Link), 1, db_handle.rel_info[i].pds_link_fp)){
                if(link_read.key == key || link_read.linked_key == key) return PDS_DELETE_FAILED;
            }
        }
    }
    
    int status = bst_del_node(&(db_handle.entity_info[iter].pds_bst), key);
    if(status != BST_SUCCESS) return PDS_DELETE_FAILED;

    int added = 0;
    for(int i = 0; i < MAX_FREE; ++i){
        if(db_handle.entity_info[iter].free_list[i] == -1){
            db_handle.entity_info[iter].free_list[i] = offset;
            added = 1;
            break;
        }
    }

    if(added == 0) return PDS_DELETE_FAILED;
    return PDS_SUCCESS;
}

int link_data(char *link_name, int key, int linked_key){
	char entity1[30],entity2[30];
    int iter_rel = -1;
    iter_rel = find_iter(link_name,1);

    char* token = strtok(link_name, "_");
    int is_first = 1;

    while(token!=NULL){
        if(is_first == 1){
            strcpy(entity1,token);
            is_first = 0;
        }
        else{
            strcpy(entity2,token);
        }
        token = strtok(NULL," ");
    }

    int iter_ent1 = -1,iter_ent2 = -1; 

    iter_ent1 = find_iter(entity1,0);
    iter_ent2 = find_iter(entity2,0);
    
    if(iter_ent1 == db_handle.db_info.num_entities || iter_ent2 == db_handle.db_info.num_entities || iter_rel == db_handle.db_info.num_relationships) return PDS_LINK_FAILED;

    struct BST_Node* node1 = bst_search(db_handle.entity_info[iter_ent1].pds_bst, key);
    struct BST_Node* node2 = bst_search(db_handle.entity_info[iter_ent2].pds_bst, linked_key);

    if(node1 != NULL && node2 != NULL){
        struct PDS_Link link_read;
        fseek(db_handle.rel_info[iter_rel].pds_link_fp, 0, SEEK_SET);
        while(fread(&link_read,sizeof(struct PDS_Link),1,db_handle.rel_info[iter_rel].pds_link_fp)){
            if(link_read.key == key && link_read.linked_key == linked_key) return PDS_LINK_FAILED;
        }

        struct PDS_Link new_link;
        new_link.key = key;
        new_link.linked_key = linked_key;

        fwrite(&new_link, sizeof(struct PDS_Link), 1, db_handle.rel_info[iter_rel].pds_link_fp);
    }

    else return PDS_LINK_FAILED;

    return PDS_SUCCESS;
}

int get_linked_data( char *link_name, int data_key, struct PDS_LinkedKeySet* linked_data ){
    
    int iter_rel;
    iter_rel = find_iter(link_name, 1);
    if(iter_rel == db_handle.db_info.num_relationships) return PDS_LINK_FAILED;

    struct PDS_Link link_read; 
    linked_data -> link_count = 0;
    linked_data -> key = data_key;

    fseek(db_handle.rel_info[iter_rel].pds_link_fp, 0, SEEK_SET);
    while(fread(&link_read, sizeof(struct PDS_Link), 1, db_handle.rel_info[iter_rel].pds_link_fp)){
        if(link_read.key == data_key){
            linked_data -> linked_keys[linked_data->link_count] = link_read.linked_key;
            linked_data -> link_count += 1;
        }
    }

    return linked_data -> link_count;
}

static void PreOrder(struct BST_Node* node, int iter){
    if(node==NULL) return;
    
    struct PDS_NdxInfo* ndx = (struct PDS_NdxInfo*) node -> data;
    fwrite(ndx, sizeof(struct PDS_NdxInfo), 1, db_handle.entity_info[iter].pds_ndx_fp);
    
    if(node->left_child!=NULL) PreOrder(node->left_child, iter);
    if(node->right_child!=NULL) PreOrder(node->right_child, iter);
}

int pds_close( char *entity_name ){
	int iter = find_iter(entity_name,0);
    if(iter == MAX_ENTITY) return PDS_FILE_ERROR;


    char ndx_file[30];
    strcpy(ndx_file,db_handle.entity_info[iter].entity_name);
    strcat(ndx_file,".ndx");

    db_handle.entity_info[iter].pds_ndx_fp = (FILE*)fopen(ndx_file,"wb");
    if(db_handle.entity_info[iter].pds_ndx_fp == NULL){
    	perror(ndx_file);
	}

    if(db_handle.entity_info[iter].pds_bst != NULL){
        PreOrder(db_handle.entity_info[iter].pds_bst, iter);
    }
    bst_destroy(db_handle.entity_info[iter].pds_bst);

    strcpy(db_handle.entity_info[iter].entity_name,"");
    fclose(db_handle.entity_info[iter].pds_data_fp);
    fclose(db_handle.entity_info[iter].pds_ndx_fp);
    
    db_handle.entity_info[iter].repo_status = PDS_ENTITY_CLOSED;
    
    return PDS_SUCCESS;
}

static int pds_link_close(struct PDS_LinkFileInfo* pds_link){
    strcpy(pds_link->link_name,"");
    fclose(pds_link->pds_link_fp);
    pds_link -> link_status = 0;

    return 0;
}

int pds_db_close()
{
	int status2 = 0;
    for(int i = 0; i < db_handle.db_info.num_relationships; ++i){
        status2 = pds_link_close(&(db_handle.rel_info[i]));
        if(status2 != 0) return PDS_FILE_ERROR;
    }

    int status1 = 0;
    for(int i = 0; i < db_handle.db_info.num_entities; ++i){
        status1 = pds_close(db_handle.entity_info[i].entity_name);
        if(status1 != PDS_SUCCESS) return PDS_FILE_ERROR;
    }

    for(int i = 0; i < db_handle.db_info.num_entities; ++i){
        db_handle.db_info.entities[i].entity_size = 0;
        strcpy(db_handle.db_info.entities[i].entity_name,"");
    }

    for(int i = 0; i < db_handle.db_info.num_relationships; ++i){
        strcpy(db_handle.db_info.links[i].link_name,"");
        strcpy(db_handle.db_info.links[i].pds_name1,"");
        strcpy(db_handle.db_info.links[i].pds_name2,"");
    }

    db_handle.db_info.num_entities = 0;
    db_handle.db_info.num_relationships = 0;
    db_handle.db_status = PDS_DB_CLOSED;
    strcpy(db_handle.db_info.db_name,"");
    
    return PDS_SUCCESS;
}