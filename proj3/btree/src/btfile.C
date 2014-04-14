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
  "INSERT_FAILED"
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

//--- key: index entry key value to be inserted to upper layer ---
//--- rid: the intended inserted rid for record ---
//--- pageNo: current page's page id ---
//--- upPageNo: copy up or push up PageId, points to new generated page ---
//--- split: flag whether insert new index entry to upper layer ---
//--- if split == false, key could be just initialize(not used by upper layer) ---
//--- uPage: upperPage obj, used for redistribution ---
//--- ls: left sibling

Status BTreeFile::insertHelper(const void* key, const RID rid, PageId pageNo, 
		Keytype key, PageId& lUpPageNo, bool& split, HFPage*& uPage)
{
	Status status;
	//--- retrieve pageNo page ---
	HFPage* currPage;
	status = MINIBASE_BM->pinPage(pageNo, (Page *&)currPage);
	if(status != OK)
		return MINIBASE_CHAIN_ERROR(BUFMGR, status);
	
	if(currPage->type == INDEX)
	{
		//--- retrieve next level page ---
		PageId nextPageId = 0;
		status = get_page_no(&key, headerPage->keyType, nextPageId);
		if(status != OK)
			return MINBASE_RESULTING_ERROR(BTREE, status, INSERT_FAILED); 

		Keytype lowerKey;
		PageId lowerUpPageNo = 0;
		bool lowerSplit = false;
		status = insertHelper(&key, rid, nextPageId, lowerKey, 
				lowerUpPageNo, lowerSplit, currPage);

		//--- check whether lower level adds new index entry ---
		//--- to this level ---
		if(lowerSplit)
		{
			//--- call insertKey() to push up loweKey -----
			RID insertRid;
			status = insertKey(&key, headerPage->keyType, nextPageId, insertRid);
			//--- if insertKey() returns NO_SPACE ---
			//--- ?? can catch this NO_SPACE ---
			if(status != OK && minibase_errors.error_index() != NO_SPACE)
				return MINIBASE_FIRST_ERROR(BTREE, INSERT_FAILED);
			else
			{
				//--- try redistribution first ---
				
				//--- 1. retrieve left sibling & try to redistribute---
				//--- add least key into left sibling ---
				bool lRedi = true;
				bool rRedi = true;
				//--- 1.1 retrieve left sibling ---
				Rid tmpRid;
				Keytype tmpKey;
				PageId tmpPageNo;
				status = get_first_sp(tmpRid, &tmpKey, tmpPageNo, uPage->curPage);
				if(status != OK)
					return MINIBASE_CHAIN_ERROR(BTREE, status);
					
				//--- if key < least key in uPage, no left sibling ---
				if(keyCompare(&key, &tmpKey, headerPage->keyType) < 0)
				{
					lRedi = false;
				}	
				else //--- find left sibling  & try to do left redistribution ---
				{
					//--- locate left sibling ---
					PageId lsibling = tmpPageNo;
					Rid nextTmpRid = tmpRid;
					Keytype nextTmpKey;
					PageId nextTmpPageNo;
					status = get_next_sp(nextTmpRid, &nextTmpKey, 
							nextTmpPageNo, uPage->curPage);
					//--- if only one index entry in parent node ---
					if(status == NOMORERECS)
							lsibling = getLeftLink();
					else if(status != OK)
					{
						return MINIBASE_CHAIN_ERROR(BTREE, status);
					}
					else
					{
						Rid nnextTmpRid = nextTmpRid;
						Keytype nnextTmpKey;
						PageId nnextTmpPageNo;
						status = get_next_sp(nnextTmpRid, &nnextTmpKey,
								nnextTmpPageNo, uPage->curPage);
						if(status != OK && status != NOMORERECS)
							return MINIBASE_CHAIN_ERROR(BTREE, status);
						
						//--- if key is just bigger than first key ---
						if(keyCompare(&key, &nextTmpKey, headerPage->keyType) < 0)
							lsibling = getLeftLink();
						else if(status == NOMORERECS) // if only two entries
						{
							lsibling = tmpPageNo;
						}
						else //-- more than 2 keys, and key bigger than second key -
						{
							while(true)
							{
								if(keyCompare(&key, &nnextTmpKey, 
											headerPage->keyType) < 0)
								{
									lsibling = tmpPageNo;
									break;
								}

								//--- update search ---
								status = get_next_sp(tmpRid, &tmpKey, 
										tmpPageNo, uPage->curPage);
								if(status != OK)
									return MINIBASE_CHAIN_ERROR(BTREE, status);

								status = get_next_sp(nextTmpRid, &nextTmpKey, nextTmpPageNo,
										uPage->curPage);
								if(status != OK)
									return MINIBASE_CHAIN_ERROR(BTREE, status);

								status = get_next_sp(nnextTmpRid, &nnextTmpKey,
										nnextTmpPageNo, uPage->curPage);
								if(status != OK && status != NOMORERECS)
									return MINIBASE_CHAIN_ERROR(BTREE, status);

								//--- check if nnext reach end ---
								if(status == NOMORERECS)
								{
									lsibling = tmpPageNo;
									break;
								}
							}
						}
					} // find left sibling page no 

					if(lRedi)
					{
						//--- do left redistribution --- 

						//--- pin left sibling ---
						HFPage* ls;
						status = MINIBASE_BM->pinPage(lsibling, (Page*& )ls);
						if(status != OK)
							return MINIBASE_CHAIN_ERROR(BUFMGR, status);
						//--- get overflow least key index entry in pageNo page ---

						//--- copy up the least key ----
						//--- to replace previous key ---
						
						//--- insert new index entry in the overflow page ---

						//--- insert the copied up least key into left sibling ---

					}
				}// try left sibling redist --
				
				//--- 2. retrieve right sibling & try to redistribute---
				//--- add least key into left sibling ---
				if(!lRedi)
				{

				}
				
				//--- 3. if both redistribution returns false, split & ---
				//---construct key push up ---
				if(!lRedi && !rRedi)
				{
					//--- mark split = true ---
				}
			}// need redis or split 
		} // lowerSplit true
	}
	else
	{
		RID keyRid;
		status = insertRec(key, headerPage->keyType, rid, keyRid);
		if(status != OK && minibase_errors.error_index() != NO_SPACE)
			return MINIBASE_FIRST_ERROR(BTREE, INSERT_FAILED);
		else
		{
			//--- try redistribution first ---

			//--- if redistribution returns false, construct key copy up--
			//--- mark split = true  ---
		}
	}

	//--- unpin pageNo page, and return OK ---
	status = MINIBASE_BM->unpinPage(pageNo, true);
	if(status != OK)
		return MINIBASE_CHAIN_ERROR(BUFMGR, status);

	return OK;
}

Status BTreeFile::insert(const void *key, const RID rid) {
	// put your code here
	Status status;
	//--- retrieve root page ---
	HFPage* rootPage;
	status = MINIBASE_BM->pinPage(headerPage->rootPageId, (Page*& rootPage));
	if(status != OK)
		return MINIBASE_CHAIN_ERROR(BUFMGR, status);

	//-- call insertHelper ---
	Keytype lowerKey;
	bool lowerSplit = false;
	status = insertHelper(key, rid, headerPage->rootPageId, lowerKey, lowerSplit);
	if(status != OK)
		return MINIBASE_RESULTING_ERROR(BTREE, status, INSERT_FAILED);

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
