#include <stdlib.h>
#include <stdio.h>

#include "bst.h"

// Local functions
static int place_bst_node( struct BST_Node *parent, struct BST_Node *node );
static struct BST_Node *make_bst_node( int key, void *data );
static int del_node(struct BST_Node** node);

// Root's pointer is passed because root can get modified for the first node
int bst_add_node( struct BST_Node **root, int key, void *data )
{
	struct BST_Node *newnode = NULL;
	struct BST_Node *parent = NULL;
	struct BST_Node *retnode = NULL;
	int status = 0;

	newnode = make_bst_node( key, data);
	if( *root == NULL ){
		*root = newnode;
		status = BST_SUCCESS;
	}
	else{
		status = place_bst_node( *root, newnode );
	}
	return status;
}

struct BST_Node *bst_search( struct BST_Node *root, int key )
{
	struct BST_Node *retval = NULL;

	if( root == NULL ){
		return NULL;
	}
	else if( root->key == key )
		return root;
	else if( key < root->key )
		return bst_search( root->left_child, key );
	else if( key > root->key )
		return bst_search( root->right_child, key );
}
void bst_print( struct BST_Node *root )
{
	if( root == NULL )
		return;
	else{
		printf("%d ", root->key);
		bst_print( root->left_child );
		bst_print( root->right_child );
	}
}

void bst_free( struct BST_Node *root )
{
	if( root == NULL )
		return;
	else{
		bst_free( root->left_child );
		bst_free( root->right_child );
		free(root);
	}
}

void bst_destroy( struct BST_Node *root )
{
	if( root == NULL )
		return;
	else{
		bst_free( root->left_child );
		bst_free( root->right_child );
		free(root->data);
		free(root);
	}
}

static int place_bst_node( struct BST_Node *parent, struct BST_Node *node )
{
	int retstatus;

	if( parent == NULL ){
		return BST_NULL;
	}
	else if( node->key == parent->key ){
		return BST_DUP_KEY;
	}
	else if( node->key < parent->key ){
		if( parent->left_child == NULL ){
			parent->left_child = node;
			return BST_SUCCESS;
		}
		else{
			return place_bst_node( parent->left_child, node );
		}
	}
	else if( node->key > parent->key ){
		if( parent->right_child == NULL ){
			parent->right_child = node;
			return BST_SUCCESS;
		}
		else{
			return place_bst_node( parent->right_child, node );
		}
	}
}

static struct BST_Node *make_bst_node( int key, void *data )
{
	struct BST_Node *newnode;
	newnode = (struct BST_Node *) malloc(sizeof(struct BST_Node));
	newnode->key = key;
	newnode->data = data;
	newnode->left_child = NULL;
	newnode->right_child = NULL;

	return newnode;
}

//Referenced from http://faculty.winthrop.edu/dannellys/csci271/binary_tree_delete.htm

int bst_del_node( struct BST_Node **root, int key ){
    if((*root) == NULL || root == NULL) return 1;

    if((*root) -> key > key) bst_del_node(&((*root) -> left_child), key);

    else if((*root) -> key < key) bst_del_node(&((*root) -> right_child),key);

    else{
		return del_node(root);
	}
}

static int del_node(struct BST_Node** node){
    struct BST_Node* temp = *node;
    if((*node)->left_child == NULL){
        *node = (*node) -> right_child;
        free(temp->data);
		free(temp);
		return BST_SUCCESS;
    }
    else if((*node) -> right_child == NULL){
        *node = temp->left_child;
        free(temp->data);
		free(temp);
		return BST_SUCCESS;
    }
    else{
        temp =(*node) -> left_child;
        while(temp->right_child!=NULL) temp = temp -> right_child;

        (*node)->key = temp->key; 
        (*node)->data = temp->data;

		int status = bst_del_node(&((*node)->left_child),temp->key);
		return status;
    }
}