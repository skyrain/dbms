/*
 * btfile.C - function members of class BTreeFile 
 * 
 * Johannes Gehrke & Gideon Glass  951022  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include "minirel.h"
#include "buf.h"
#include "db.h"
#include "new_error.h"
#include "btfile.h"
#include "btreefilescan.h"

// Define your error message here
const char* BtreeErrorMsgs[] = {
  // Possible error messages
  // _OK
  "Can't find header"// CANT_FIND_HEADER
  "Can't pin header"// CANT_PIN_HEADER,
  "Can't alloc header"// CANT_ALLOC_HEADER
  "Can't add file entry"// CANT_ADD_FILE_ENTRY
  "Can't unpin header"// CANT_UNPIN_HEADER
  "Can't pin page"// CANT_PIN_PAGE
  "Can't unpin page"// CANT_UNPIN_PAGE
  // INVALID_SCAN
  // SORTED_PAGE_DELETE_CURRENT_FAILED
  "Can't delete file entry"// CANT_DELETE_FILE_ENTRY
  "Can't free a page"// CANT_FREE_PAGE,
  "Can't delete subtree"// CANT_DELETE_SUBTREE,
  // KEY_TOO_LONG
  // INSERT_FAILED
  "Can't create root"// COULD_NOT_CREATE_ROOT
  // DELETE_DATAENTRY_FAILED
  // DATA_ENTRY_NOT_FOUND
  // CANT_GET_PAGE_NO
  // CANT_ALLOCATE_NEW_PAGE
  // CANT_SPLIT_LEAF_PAGE
  // CANT_SPLIT_INDEX_PAGE
};

static error_string_table btree_table( BTREE, BtreeErrorMsgs);

// BTreeFile constructor, if B+ tree with given filename exist already, open it.
BTreeFile::BTreeFile (Status& returnStatus, const char *filename)
{
	Status status;
	PageId headId = INVALID_PAGE;
	// Call DB function to open the btree file.
	status = MINIBASE_DB->get_file_entry(filename, headId);
	if(status != OK)
	{
		returnStatus = MINIBASE_FIRST_ERROR(BTREE, NO_HEADER);
		return;
	}
		
	status = MINIBASE_BM->pinPage(headerPageId, (Page *&)(this->headerPage));
	if(status != OK)
	{
		returnStatus = MINIBASE_FIRST_ERROR(BTREE, HEADER_PIN_ERROR);
		return;
	}
	// give the value to headerPageId.
	this->headerPageId = headId;
	// give the value to filename;
	this->filename = new char[strlen(filename) + 1];
	strcpy(this->filename, filename);

	returnStatus = OK;
}

// BTreeFile constructor, if B+ tree with given filename doen not exit, create it.
BTreeFile::BTreeFile (Status& returnStatus, const char *filename, 
                      const AttrType keytype,
                      const int keysize)
{
	Status status;
        PageId headId = INVALID_PAGE;
        // Call DB function to open the btree file.
        status = MINIBASE_DB->get_file_entry(filename, headId);
        if(status != OK)
        {	
		PageId headId = INVALID_PAGE;
		// allocate a header page by calling newPage.
		status = MINIBASE_BM->newPage(headId, (Page *&)(this->headerPage));
		if(status != OK)
		{
			returnStatus = MINIBASE_FIRST_ERROR(BTREE, HEADER_ALLOC_ERROR);
			return;
		}	

		// allocate a root page to the header page.
		PageId rootPageId = INVALID_PAGE;
		Page * rootPage;
		// allocate a root page by calling newPage.
		status = MINIBASE_BM->newPage(rootPageId, (Page *&)rootPage);
		if(status != OK){
			returnStatus = MINIBASE_FIRST_ERROR(BTREE, ROOT_ALLOC_ERROR);
			return;
		}

		// Store the meta info into the header page.
		headerPage->rootPageId = rootPageId;
		headerPage->keyType = keytype;
		headerPage->keysize = keysize;
		((BTLeafPage *)rootPage)->init(rootPageId);

		// then unpin the root page
		status = MINIBASE_BM->unpinPage(rootPageId, true);
		if(status != OK)
		{
			returnStatus = MINIBASE_FIRST_ERROR(BTREE, CANT_UNPIN_PAGE);
			return;
		}

		// Add the file into Database by calling DB->
		status = MINIBASE_DB->add_file_entry(filename, headId);
		if(status != OK)
		{
			returnStatus = MINIBASE_FIRST_ERROR(BTREE, ADD_FILE_ENTRY_ERROR);
			return;
		}
		
		// At last give these info to the Btfile class.
        	// give the value to headerPageId.
	        this->headerPageId = headId;
        	// give the value to filename;
	        this->filename = new char[strlen(filename) + 1];
        	strcpy(this->filename, filename);

        }else{
		// else we pin and open the existing btree directly
        	status = MINIBASE_BM->pinPage(headerPageId, (Page *&)(this->headerPage));
	        if(status != OK)
       	 	{
                	returnStatus = MINIBASE_FIRST_ERROR(BTREE, HEADER_PIN_ERROR);
               	 	return;
        	}
	        // give the value to headerPageId.
        	this->headerPageId = headId;
	        // give the value to filename;
        	this->filename = new char[strlen(filename) + 1];
	        strcpy(this->filename, filename);
        	returnStatus = OK;
	}
}

// Decontructor of Btree file, unpin the header page delete the filename;	
BTreeFile::~BTreeFile ()
{
	// delete the memory space of name space
	delete [] filename;
	
	Status status;
	// unpin the header page, set it dirty
	status = MINIBASE_BM->unpinPage(headerPageId, true);
	if(status != OK)
		MINIBASE_FIRST_ERROR(BTREE, CANT_UNPIN_PAGE);
}

// Delete the whole btree file from the database, unpin each page from the index recursively, by calling get_first and get_next. Delete the file at last.
Status BTreeFile::destroyFile ()
{
	// start from the root, recursively free each leaf page under index page
	Status status;
	PageId root = INVALID_PAGE;
	root = headerPage->rootPageId;
	
	// if the root is not null, recursively delete the child of the node
	if(root != INVALID_PAGE)
	{	
		status = deleteSubTree(root);
		if(status != OK)
			return MINIBASE_FIRST_ERROR(BTREE, CANT_PIN_PAGE);
	}
	
	PageId headId = INVALID_PAGE;
	headId = this->headerPageId;
	// unpin and free the header page.
	status = MINIBASE_BM->unpinPage(headId);
	if(status != OK)
		return MINIBASE_FIRST_ERROR(BTREE, CANT_UNPIN_PAGE);
	
	status = MINIBASE_BM->freePage(headId);
	if(status != OK)
		return MINIBASE_FIRST_ERROR(BTREE, CANT_UNPIN_PAGE);
	
	// delete from db 
	status = MINIBASE_DB->delete_file_entry(filename);
	if(status != OK)
		return MINIBASE_FIRST_ERROR(BTREE, DELETE_FILE_ENTRY_ERROR);
	
	return OK;
}

// Supporting function for destroyFile(), recursively delete the subtree of input node
Status BTreeFile::deleteSubTree(PageId pageId){
	Status status;
	SortedPage * curPage;
	
	status = MINIBASE_BM->pinPage(pageId, (Page *&)curPage);
	if(status != OK)
		return MINIBASE_FIRST_ERROR(BTREE, CANT_PIN_PAGE);
	
	// if the current page is a index page then, find it's child node.
	if(curPage->get_type() == INDEX)
	{
		BTIndexPage *tmpPage;
		tmpPage = (BTIndexPage *)curPage;
		RID tmpRid;
		PageId nextId;
		void *tmpKey = NULL;	
	
		// Recursively delete the child node of certain index node.
		status = tmpPage->get_first(tmpRid, tmpKey, nextId);
		while(status != NOMORERECS){
			status = deleteSubTree(nextId);
			if(status != OK)
				return MINIBASE_FIRST_ERROR(BTREE, DELETE_TREE_ERROR);
			status = tmpPage->get_next(tmpRid, tmpKey, nextId);
		}
	}

	// unpin and free the page at last.
	status = MINIBASE_BM->unpinPage(pageId);
	if(status != OK)
		return MINIBASE_FIRST_ERROR(BTREE, CANT_UNPIN_PAGE);
	
	status = MINIBASE_BM->freePage(pageId);
	if(status != OK)
		return MINIBASE_FIRST_ERROR(BTREE, FREE_PAGE_ERROR);
	
	return OK;
}

Status BTreeFile::insert(const void *key, const RID rid) {
  // put your code here
  return OK;
}

Status BTreeFile::Delete(const void *key, const RID rid) {
  // put your code here
  return OK;
}
    
IndexFileScan *BTreeFile::new_scan(const void *lo_key, const void *hi_key) {
  // put your code here
  return NULL;
}

int keysize(){
  // put your code here
  return 0;
}
