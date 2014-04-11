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
  // CANT_DELETE_FILE_ENTRY
  // CANT_FREE_PAGE,
  // CANT_DELETE_SUBTREE,
  // KEY_TOO_LONG
  // INSERT_FAILED
  // COULD_NOT_CREATE_ROOT
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
		
	Page *headPage;
	status = MINIBASE_BM->pinPage(headerPageId, (Page *&)headPage);
	if(status != OK)
	{
		returnStatus = MINIBASE_FIRST_ERROR(BTREE, HEADER_PIN_ERROR);
		return;
	}
	// give the page address to headerPage;
	this->headerPage = headPage;
	// give the value to headerPageId.
	this->headerPageId = headId;
	// give the value to filename;
	this->filename = new char[strlen(filename) + 1];
	strcpy(this->filename, filename);
}

BTreeFile::BTreeFile (Status& returnStatus, const char *filename, 
                      const AttrType keytype,
                      const int keysize)
{
  // put your code here
}

BTreeFile::~BTreeFile ()
{
  // put your code here
}

Status BTreeFile::destroyFile ()
{
  // put your code here
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
