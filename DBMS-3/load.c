int pds_close(){
    char ndx_file[30];
    strcpy(ndx_file,repo_handle.pds_name);
    strcat(ndx_file,".ndx");

    repo_handle.pds_ndx_fp = (FILE*)fopen(ndx_file,"wb");
    if(repo_handle.pds_ndx_fp == NULL){
    	perror(ndx_file);
	}

    if(repo_handle.pds_bst != NULL){
        cnt = 0;
        PreOrder(repo_handle.pds_bst);
        printf("%d records added\n", cnt);
    }
    bst_destroy(repo_handle.pds_bst);

    strcpy(repo_handle.pds_name,"");
    fclose(repo_handle.pds_data_fp);
    fclose(repo_handle.pds_ndx_fp);
    
    repo_handle.repo_status = PDS_REPO_CLOSED;
    
    return PDS_SUCCESS;
}

void PreOrder(struct BST_Node* node){
    if(node==NULL) return;
    
    struct PDS_NdxInfo* ndx = (struct PDS_NdxInfo*) node -> data;
    fwrite(ndx, sizeof(struct PDS_NdxInfo), 1, repo_handle.pds_ndx_fp);
    ++cnt; printf("%d ",ndx->key);
    
    if(node->left_child!=NULL) PreOrder(node->left_child);
    if(node->right_child!=NULL) PreOrder(node->right_child);
}