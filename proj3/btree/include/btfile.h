/*
 * btfile.h
 *
 * sample header file
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */
 
#ifndef _BTREE_H
#define _BTREE_H

#include "btindex_page.h"
#include "btleaf_page.h"
#include "index.h"
#include "btreefilescan.h"
#include "bt.h"

// Define the headerPage to hold the information about the tree as a whole
struct HeaderPage{
	PageId rootPageId;
	AttrType keyType;
	int keysize;	
};

// Define your error code for B+ tree here
enum btErrCodesa{
	NO_HEADER,
	HEADER_PIN_ERROR,
	HEADER_ALLOC_ERROR,
	ADD_FILE_ENTRY_ERROR,
	UNPIN_HEADER_ERROR,
	CANT_PIN_PAGE,
	CANT_UNPIN_PAGE,
	DELETE_FILE_ENTRY_ERROR,
	FREE_PAGE_ERROR,
	DELETE_TREE_ERROR,
	INSERT_FAILED,
	ROOT_ALLOC_ERROR,
};

class BTreeFile: public IndexFile
{
  public:
    BTreeFile(Status& status, const char *filename);
    // an index with given filename should already exist,
    // this opens it.
    
    BTreeFile(Status& status, const char *filename, const AttrType keytype, const int keysize);
    // if index exists, open it; else create it.
    
    ~BTreeFile();
    // closes index
    
    Status destroyFile();
    // destroy entire index file, including the header page and the file entry
   
    Status insert(const void *key, const RID rid);
    // insert <key,rid> into appropriate leaf page
    
    Status Delete(const void *key, const RID rid);
    // delete leaf entry <key,rid> from the appropriate leaf
    // you need not implement merging of pages when occupancy
    // falls below the minimum threshold (unless you want extra credit!)
    
    IndexFileScan *new_scan(const void *lo_key = NULL, const void *hi_key = NULL);
    // create a scan with given keys
    // Cases:
    //      (1) lo_key = NULL, hi_key = NULL
    //              scan the whole index
    //      (2) lo_key = NULL, hi_key!= NULL
    //              range scan from min to the hi_key
    //      (3) lo_key!= NULL, hi_key = NULL
    //              range scan from the lo_key to max
    //      (4) lo_key!= NULL, hi_key!= NULL, lo_key = hi_key
    //              exact match ( might not unique)
    //      (5) lo_key!= NULL, hi_key!= NULL, lo_key < hi_key
    //              range scan from lo_key to hi_key

    int keysize();
  
     private:
	// file name of the btreefile
	char * filename;
	// header page Id;
	PageId headerPageId;
	// header page
	HeaderPage * headerPage;
	
	//--- assist functions ---
	
	Status deleteSubTree(PageId pageId);  
    // Supporting function for destroyFile(), recursively delete the subtree of input node
	
	//--- insert helper ---
	//--- key: index entry value to be inserted to upper layer ---
	//--- split: flag whether insert new index entry to upper layer ---
	//--- if split == false, key could be just initialize(not used by upper laye	r) ---
	Status insertHelper(const void* key, const RID rid, 
			PageId pageNo, Keytype key, bool& split);


};

#endif
