/*****************************************************************************/
/************************ Implementation of B+ Tree ***************************/
/*****************************************************************************/

/*
	btfile.C
	Define the B tree file and all the operations.

	- Overall description of your algorithms and data structures
	// BTreeFile constructor, if B+ tree with given filename exist already, open it.
	BTreeFile(Status& status, const char *filename); 

	// BTreeFile constructor, if B+ tree with given filename doen not exit, create it.
	BTreeFile(Status& status, const char *filename, const AttrType keytype, const int keysize);
	
	// Decontructor of Btree file, unpin the header page delete the filename;	
	BTreeFile::~BTreeFile ()

	// Delete the whole btree file from the database, unpin each page from the index recursively, by calling get_first and get_next. Delete the file at last.
	Status BTreeFile::destroyFile ()

	// Supporting function for destroyFile(), recursively delete the subtree of input node
	Status BTreeFile::deleteSubTree(PageId pageId)
	
	// Supporting function for insert.
	//--- key: index entry key value to be inserted to upper layer ---
	//--- rid: the intended inserted rid for record ---
	//--- pageNo: current page's page id ---
	//--- upPageNo: copy up or push up PageId, points to new generated page ---
	//--- split: flag whether insert new index entry to upper layer ---
	//--- if split == false, key could be just initialize(not used by upper layer) ---
	//--- uPage: upperPage obj, used for redistribution ---
	//--- ls: left sibling
	Status BTreeFile::insertHelper(const void* key, const RID rid, PageId pageNo, void* l_Key, PageId& l_UpPageNo, bool& l_split, HFPage*& uPage)

	// Supporting function for Delete(), recursively find the leaf node and delete the leaf node.
	// Only implemented delete in the leaf node without redistribution when the record drop below the half.
	Status deleteHelper(const void *key, const RID rid, PageId pageNo);
      
	// insert <key,rid> into appropriate leaf page
	Status insert(const void *key, const RID rid);
	
	// delete leaf entry <key,rid> from the appropriate leaf
	Status Delete(const void *key, const RID rid);
    
	// create a scan with given keys, dealing with the following cases.
	// It will call the btreefilescan to do the job recursively.
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
	IndexFileScan *new_scan(const void *lo_key = NULL, const void *hi_key = NULL);
		
	// It will return the key size of the key
	int keysize();

	- Anything unusual in your implementation

	Design and implementation details are the same as what descriped in the course website.

	- What functionalities not supported well

	All functionalities are implemented, but not work properly so far, we will fix that soon.

*/

#include "minirel.h"
#include "buf.h"
#include "db.h"
#include "new_error.h"
#include "btfile.h"
#include "btreefilescan.h"
#include "heapfile.h"

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
  "Delete entry failed"// DELETE_DATAENTRY_FAILED
  "Record not found"// DATA_ENTRY_NOT_FOUND
  "Can't get page no"// CANT_GET_PAGE_NO
  // CANT_ALLOCATE_NEW_PAGE
  // CANT_SPLIT_LEAF_PAGE
  // CANT_SPLIT_INDEX_PAGE
  "Can't allocate leaf page"
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


Status BTreeFile::indexRootSplit(PageId pageNo,
		const void *lowerKey, PageId lowerUpPageNo, HFPage*& currPage)
{
	//--- split ---
	//--- create a new root page ---
	// allocate a root page to the header page.
	PageId rootPageId = INVALID_PAGE;
	Page * rootPage;
	// allocate a root page by calling newPage.
	status = MINIBASE_BM->newPage(rootPageId, (Page *&)rootPage);
	if(status != OK){
		return  MINIBASE_FIRST_ERROR(BTREE, ROOT_ALLOC_ERROR);
	}

	// Store the meta info into the header page.
	headerPage->rootPageId = rootPageId;
	((BTIndexPage *)rootPage)->init(rootPageId);

	//--- create a new index page l2 to hold data---
	PageId newPageId = INVALID_PAGE;
	Page * newPage;
	status = MINIBASE_BM->newPage(newPageId, (Page *&)newPage);
	if(status != OK){
		return  MINIBASE_FIRST_ERROR(BTREE, ROOT_ALLOC_ERROR);
	}
	((BTIndexPage *)newPage)->init(newPageId);
	//--- copy mid to end entries to new page from pageNo page ---

	//--- 1. get the total index entries amount & whether overflow entry --
	//--- is before mid or after mid ---
	RID tRid;
	Keytype tKey;
	PageId tPageNo;
	status = ((BTIndexPage*)currPage)->get_first(tRid, &tKey, tPageNo);
	if(status != OK)
		return MINIBASE_CHAIN_ERROR(BTREE, status);

	int totalEntry = 0;
	int lKeyPos = 0;
	bool flag = true;

	while(true)
	{
		if(keyCompare(&lowerKey, &tKey, 
					headerPage->keyType) < 0 && flag)
		{
			lKeyPos = totalEntry;
			flag = false;
		}

		status = ((BTIndexPage*)currPage)->get_next(tRid, &tKey, tPageNo);
		if(status != OK && status != NOMORERECS)
			return MINIBASE_CHAIN_ERROR(BTREE, status);
		if(status == NOMORERECS)
			break;

		totalEntry++;
	}

	totalEntry++;

	status = ((BTIndexPage*)currPage)->get_first(tRid, &tKey, tPageNo);
	if(status != OK)
		return MINIBASE_CHAIN_ERROR(BTREE, status);

	//--- 2. skip front entries ---
	//--- ?? right middle --- 
	for(int i = 0; i < (totalEntry + 1) / 2; i++)
	{
		status = ((BTIndexPage*)currPage)->get_next(tRid, &tKey, tPageNo);
		if(status != OK && status != NOMORERECS)
			return MINIBASE_CHAIN_ERROR(BTREE, status);
	}

	//--- 3. copy back entries to new page & delete if from currPage---
	while(true)
	{
		//--- copy ---
		status = ((BTIndexPage*)newPage)->insertKey(&tKey, 
				headerPage->keyType, tPageNo, tRid);
		//--- if insertKey() returns NO_SPACE ---
		//--- ?? can catch this NO_SPACE ---
		if(status != OK && status != DONE)
			return MINIBASE_FIRST_ERROR(BTREE, INSERT_FAILED);

		//--- delete ---
		status = ((BTIndexPage*)currPage)->deleteKey(&tKey, headerPage->keyType, tInsertRid);
		if(status != OK)
			return MINIBASE_CHAIN_ERROR(BTREE, status);

		//--- get next copy ---
		status = ((BTIndexPage*)currPage)->get_next(tRid, &tKey, tPageNo);
		if(status != OK && status != NOMORERECS)
			return MINIBASE_CHAIN_ERROR(BTREE, status);
		if(status == NOMORERECS)
			break;
	}
	//--- 4. insert new entry  ---
	if(lKeyPos < (totalEntry + 1) / 2)
	{
		status = ((BTIndexPage*)currPage)->insertKey(&lowerKey, 
				headerPage->keyType, lowerUpPageNo, tRid);
		//--- if insertKey() returns NO_SPACE ---
		//--- ?? can catch this NO_SPACE ---
		if(status != OK && status != DONE)
			return MINIBASE_FIRST_ERROR(BTREE, INSERT_FAILED);
	}
	else
	{
		status = ((BTIndexPage*)newPage)->insertKey(&lowerKey, 
				headerPage->keyType, lowerUpPageNo, tRid);
		//--- if insertKey() returns NO_SPACE ---
		//--- ?? can catch this NO_SPACE ---
		if(status != OK && status != DONE)
			return MINIBASE_FIRST_ERROR(BTREE, INSERT_FAILED);
	}

	//--- 5. insert mid entry(first entry in new page) to root page ---
	status = ((BTIndexPage*)newPage)->get_first(tRid, &tKey, tPageNo);
	if(status != OK)
		return MINIBASE_CHAIN_ERROR(BTREE, status);

	status = ((BTIndexPage*)rootPage)->insertKey(&tKey, headerPage->keyType, 
			newPageId, tRid);
	//--- if insertKey() returns NO_SPACE ---
	//--- ?? can catch this NO_SPACE ---
	if(status != OK && status != DONE)
		return MINIBASE_FIRST_ERROR(BTREE, INSERT_FAILED);

	//--- 6. delete step 5 mid entry from new page(push up) ---
	status = ((BTIndexPage*)newPage)->get_first(tRid, &tKey, tPageNo);
	if(status != OK)
		return MINIBASE_CHAIN_ERROR(BTREE, status);

	//--- 6.1. set new page left link ---
	((BTIndexPage*)newPage)->setLeftLink(tPageNo);

	//--- 6.2 delete ---
	status = ((BTIndexPage*)newPage)->deleteKey(&tKey, headerPage->keyType, insertRid);
	if(status != OK)
		return MINIBASE_FIRST_ERROR(BTREE, status);

	//---7. set root page left link ---
	((BTIndexPage*)rootPage)->setLeftLink(pageNo);


	//---  unpin rootpage and new page & cur page ---
	status = MINIBASE_BM->unpinPage(rootPageId, true);
	if(status != OK)
		return MINIBASE_CHAIN_ERROR(BUFMGR, status);
	status = MINIBASE_BM->unpinPage(newPageId, true);
	if(status != OK)
		return MINIBASE_CHAIN_ERROR(BUFMGR, status);
	status = MINIBASE_BM->unpinPage(pageNo, true);
	if(status != OK)
		return MINIBASE_CHAIN_ERROR(BUFMGR, status);

	return OK;

}

Status BTreeFile::indexLeftRedistribution(const void* lowerKey,
		const PageId lowerUpPageNo, bool& lRedi, HFPage* currPage, HFPage* uPage)
{
	//--- 1.1 retrieve left sibling ---
	RID tmpRid;
	Keytype tmpKey;
	PageId tmpPageNo;
	status = ((BTIndexPage*)uPage)->get_first(tmpRid, &tmpKey, tmpPageNo);
	if(status != OK)
		return MINIBASE_CHAIN_ERROR(BTREE, status);

	bool lsIsLeftLink = false;
	//--- if lowerkey < least key in uPage, no left sibling ---
	if(keyCompare(&lowerKey, &tmpKey, headerPage->keyType) < 0)
	{
		lRedi = false;
		return OK;
	}
	//--- lower key > least key in uPage 
	else //--- find left sibling  & try to do left redistribution ---
	{
		//--- locate left sibling ---
		PageId lsibling = tmpPageNo;
		Keytype lKey; //--- key in uPage to be updated ---

		RID nextTmpRid = tmpRid;
		Keytype nextTmpKey;
		PageId nextTmpPageNo;
		status =((BTIndexPage*)uPage)->get_next(nextTmpRid, &nextTmpKey, 
				nextTmpPageNo);
		//--- if only one index entry in parent node ---
		if(status == NOMORERECS)
		{
			lsibling = ((BTIndexPage*)uPage)->getLeftLink();
			lsIsLeftLink = true;
			lKey = tmpKey;
		}
		else if(status != OK)
		{
			return MINIBASE_CHAIN_ERROR(BTREE, status);
		}
		else
		{
			RID nnextTmpRid = nextTmpRid;
			Keytype nnextTmpKey;
			PageId nnextTmpPageNo;
			status = ((BTIndexPage*)uPage)->get_next(nnextTmpRid, &nnextTmpKey,
					nnextTmpPageNo);
			if(status != OK && status != NOMORERECS)
				return MINIBASE_CHAIN_ERROR(BTREE, status);

			//--- if key is just bigger than first key ---
			if(keyCompare(&lowerKey, &nextTmpKey, headerPage->keyType) < 0)
			{ 
				lsibling = ((BTIndexPage*)uPage)->getLeftLink();
				lsIsLeftLink = true;
				lKey = tmpKey;
			}
			else if(status == NOMORERECS) // if only two entries
			{
				lsibling = tmpPageNo;
				lKey = nextTmpKey;
			}
			else //-- more than 2 keys, and key bigger than second key -
			{
				while(true)
				{
					if(keyCompare(&lowerKey, &nnextTmpKey, 
								headerPage->keyType) < 0)
					{
						lsibling = tmpPageNo;
						lKey = nextTmpKey;
						break;
					}

					//--- update search ---
					status =((BTIndexPage*)uPage)->get_next(tmpRid, &tmpKey, 
							tmpPageNo);
					if(status != OK)
						return MINIBASE_CHAIN_ERROR(BTREE, status);

					status = ((BTIndexPage*)uPage)->get_next(nextTmpRid, 
							&nextTmpKey, nextTmpPageNo);
					if(status != OK)
						return MINIBASE_CHAIN_ERROR(BTREE, status);

					status =((BTIndexPage*)uPage)->get_next(nnextTmpRid, &nnextTmpKey,
							nnextTmpPageNo);
					if(status != OK && status != NOMORERECS)
						return MINIBASE_CHAIN_ERROR(BTREE, status);

					//--- check if nnext reach end ---
					if(status == NOMORERECS)
					{
						lsibling = tmpPageNo;
						lKey = nextTmpKey;
						break;
					}
				}
			}
		} // find left sibling page no 

		//--- do left redistribution --- 

		//--- pin left sibling ---
		HFPage* ls;
		status = MINIBASE_BM->pinPage(lsibling, (Page*& )ls);
		if(status != OK)
			return MINIBASE_CHAIN_ERROR(BUFMGR, status);

		//--- compare key and currPage first entry ---
		RID tRid;
		Keytype tKey;
		PageId tPageNo;
		status = ((BTIndexPage*)currPage)->get_first(tRid, &tKey, tPageNo);
		if(status != OK)
			return MINIBASE_CHAIN_ERROR(BTREE, status);

		//--- if lower key is least, insert it into left sibling --
		if(keyCompare(&lowerKey, &tKey, headerPage->keyType) < 0)
		{
			status = ((BTIndexPage*)ls)->insertKey(&lowerKey, headerPage->keyType, lowerUpPageNo, tmpRid);
			if(status != OK && status == DONE)
			{
				lRedi = false;	
				return OK;
			}
			else if(status != OK)
				return MINIBASE_CHAIN_ERROR(BTREE, status);

			//--- if left sibling has space --
			//--- get end entry of left sibling & ---
			//---inset into uPage to replace lKey --- 
			if(lRedi)
			{
				//--- get end entry of left sibling ---
				status = ((BTIndexPage*)ls)->get_first(tRid, &tKey, tPageNo);
				if(status != OK && status != NOMORERECS)
					return MINIBASE_CHAIN_ERROR(BTREE, status);

				//--- ?? should use memcpy ---
				Keytype endKey = tKey;

				while(true)
				{
					status = ((BTIndexPage*)ls)->get_next(tRid, &tKey, tPageNo);
					if(status != OK && status != NOMORERECS)
						return MINIBASE_CHAIN_ERROR(BTREE, status);

					if(status == NOMORERECS)
						break;

					endKey = tKey;
				}
				//--- push up end entry into uPage --- 
				status = ((BTIndexPage *)uPage)->deleteKey(&lKey,
						headerPage->keyType, tmpRid);
				if(status != OK)
					return MINIBASE_CHAIN_ERROR(BTREE, status);

				status = ((BTIndexPage *)uPage)->insertKey(&endKey, 
						headerPage->keyType, pageNo, tmpRid);
				if(status != OK)
					return MINIBASE_CHAIN_ERROR(BTREE, status);

				//--- unpin cur, left sbiling ---
				status = MINIBASE_BM->unpinPage(pageNo, true);
				if(status != OK)
					return MINIBASE_CHAIN_ERROR(BUFMGR, status);
				status = MINIBASE_BM->unpinPage(lsibling, true);
				if(status != OK)
					return MINIBASE_CHAIN_ERROR(BUFMGR, status);

				return OK;
			}
		}
		else //--- insert pageNo least key into left sibling & uPage &
			//--- delete itself ---
			//--- insert key into pageNo page ---
		{
			//--- get least key in pageNo page ---
			status = ((BTIndexPage*)currPage)->get_first(tRid, &tKey, tPageNo);
			if(status != OK && status != NOMORERECS)
				return MINIBASE_CHAIN_ERROR(BTREE, status);

			//--- insert least key into left sibling ---
			status = ((BTIndexPage *)ls)->insertKey(&tKey, 
					headerPage->keyType, tPageNo, tmpRid);
			if(status != OK && status == DONE)
			{
				lRedi = false;	
				return OK;
			}
			else if(status != OK)
				return MINIBASE_CHAIN_ERROR(BTREE, status);

			if(lRedi)
			{
				//--- delete least key in pageNo page ---
				status = ((BTIndexPage *)currPage)->deleteKey(&tKey,
						headerPage->keyType, tmpRid);
				if(status != OK)
					return MINIBASE_CHAIN_ERROR(BTREE, status);

				//--- delete useless key in uPage ---
				status = ((BTIndexPage *)uPage)->deleteKey(&lKey,
						headerPage->keyType, tmpRid);
				if(status != OK)
					return MINIBASE_CHAIN_ERROR(BTREE, status);

				//--- insert least key into uPage ---
				status = ((BTIndexPage *)uPage)->insertKey(&tKey, 
						headerPage->keyType, pageNo, tmpRid);
				if(status != OK)
					return MINIBASE_CHAIN_ERROR(BTREE, status);

				//--- insert new entry into pageNo page ---
				status = ((BTIndexPage *)currPage)->insertKey(&lowerKey, 
						headerPage->keyType, lowerUpPageNo, tmpRid);
				if(status != OK)
					return MINIBASE_CHAIN_ERROR(BTREE, status);

				//--- unpin cur, l sibling page ---
				status = MINIBASE_BM->unpinPage(pageNo, true);
				if(status != OK)
					return MINIBASE_CHAIN_ERROR(BUFMGR, status);
				status = MINIBASE_BM->unpinPage(lsibling, true);
				if(status != OK)
					return MINIBASE_CHAIN_ERROR(BUFMGR, status);
				
				return OK;
			}
		}
	}
	return OK;
}

Status BTreeFile::indexRightRedistribution(const void* lowerKey,
		const PageId lowerUpPageNo, bool& rRedi, 
		HFPage* currPage, HFPage* uPage)
{
	RID tmpRid;
	Keytype tmpKey;
	PageId tmpPageNo;	
	status = ((BTIndexPage*)uPage)->get_first(tmpRid, &tmpKey, tmpPageNo);
	if(status != OK)
		return MINIBASE_CHAIN_ERROR(BTREE, status);

	//--- locate right sibling ---
	PageId rsibling = tmpPageNo;
	Keytype rKey;

	//--- if lower key < least key in uPage, find right sibling ---
	if(keyCompare(&lowerKey, &tmpKey, headerPage->keyType) < 0)
	{
		rsibling = tmpPageNo;
		rKey = tmpKey;
	}
	else//--- lower key >= least key in uPage ---
	{
		RID nextTmpRid = tmpRid;
		Keytype nextTmpKey;
		PageId nextTmpPageNo;
		status =((BTIndexPage*)uPage)->get_next(nextTmpRid, &nextTmpKey, 
				nextTmpPageNo);
		//--- if only one index entry in parent node ---
		if(status == NOMORERECS)
		{
			rRedi = false;
			return OK;
		}
		else if(status != OK)
		{
			return MINIBASE_CHAIN_ERROR(BTREE, status);
		}
		else //--- else if >= 2 index entry in parent node ---
		{
			RID nnextTmpRid = nextTmpRid;
			Keytype nnextTmpKey;
			PageId nnextTmpPageNo;
			status = ((BTIndexPage*)uPage)->get_next(nnextTmpRid, &nnextTmpKey, nnextTmpPageNo);
			if(status != OK && status != NOMORERECS)
				return MINIBASE_CHAIN_ERROR(BTREE, status);

			//--- if key is just bigger than first key ---
			if(keyCompare(&lowerKey, &nextTmpKey, headerPage->keyType) < 0)
			{ 
				rsibling = nextTmpPageNo;
				rKey = nextTmpKey;
			}
			else if(status == NOMORERECS) // if only two entries
			{
				rRedi = false;
				return OK;
			}
			else //-- more than 2 keys, and key bigger than second key -
			{
				while(true)
				{
					if(keyCompare(&lowerKey, &nnextTmpKey, 
								headerPage->keyType) < 0)
					{
						rsibling = nnextTmpPageNo;
						rKey = nnextTmpKey;
						break;
					}

					//--- update search ---
					status =((BTIndexPage*)uPage)->get_next(tmpRid, &tmpKey, 
							tmpPageNo);
					if(status != OK)
						return MINIBASE_CHAIN_ERROR(BTREE, status);

					status = ((BTIndexPage*)uPage)->get_next(nextTmpRid, 
							&nextTmpKey, nextTmpPageNo);
					if(status != OK)
						return MINIBASE_CHAIN_ERROR(BTREE, status);

					status =((BTIndexPage*)uPage)->get_next(nnextTmpRid, &nnextTmpKey,
							nnextTmpPageNo);
					if(status != OK && status != NOMORERECS)
						return MINIBASE_CHAIN_ERROR(BTREE, status);

					//--- check if nnext reach end ---
					if(status == NOMORERECS)
					{
						rRedi = false;
						return OK:
					}
				}
			}
		}  
	}

//??? wait to check


	//--- pin right sibling ---
	HFPage* rs;
	status = MINIBASE_BM->pinPage(rsibling, (Page*& )rs);
	if(status != OK)
		return MINIBASE_CHAIN_ERROR(BUFMGR, status);

	//--- compare key and currPage end entry ---
	RID tRid;
	Keytype tKey;
	PageId tPageNo;
	status = ((BTIndexPage*)currPage)->get_first(tRid, &tKey, tPageNo);
	if(status != OK)
		return MINIBASE_CHAIN_ERROR(BTREE, status);

	//--- if key is biggest among currPage key, ---
	//--- insert it into right sibling,  ---
	//--- otherwise insert end key of currPage into right sibling --


	//--- get end entry of currPage ---
	//--- ?? should use memcpy ---
	Keytype endKey = tKey;
	PageId endPageNo = tPageNo;
	while(true)
	{
		status = ((BTIndexPage*)currPage)->get_next(tRid, 
				&tKey, tPageNo);
		if(status != OK && status != NOMORERECS)
			return MINIBASE_CHAIN_ERROR(BTREE, status);

		if(status == NOMORERECS)
			break;

		endKey = tKey;
		endPageNo = tPageNo;
	}
	//--- compare lower key with endKey of currPage ---
	if(keyCompare(&lowerKey, &endKey,
				headerPage->keyType) < 0) // lower key smaller
	{
		//--- insert endKey into right sibling ---
		status = ((BTIndexPage*)rs)->insertKey(&endKey, 
				headerPage->keyType, endPageNo, insertRid);

		if(status != OK &&
				status == DONE)
			rRedi = false;	
		else if(status != OK)
			return MINIBASE_CHAIN_ERROR(BTREE, status);

		//--- if right sibling has space --
		if(rRedi)
		{
			//--- delete cur page end try ---
			status = ((BTIndexPage *)currPage)->deleteKey(&endKey,
					headerPage->keyType, insertRid);
			if(status != OK)
				return MINIBASE_CHAIN_ERROR(BTREE, status);

			//--- insert new entry in cur page ---
			status = ((BTIndexPage *)currPage)->insertKey(&lowerKey,
					headerPage->keyType, lowerUpPageNo, insertRid);
			if(status != OK)
				return MINIBASE_CHAIN_ERROR(BTREE, status);

			//--- push up end entry into uPage --- 
			status = ((BTIndexPage *)uPage)->deleteKey(&rKey,
					headerPage->keyType, insertRid);
			if(status != OK)
				return MINIBASE_CHAIN_ERROR(BTREE, status);

			status = ((BTIndexPage *)uPage)->insertKey(&endKey, 
					headerPage->keyType, rsibling, insertRid);
			if(status != OK)
				return MINIBASE_CHAIN_ERROR(BTREE, status);

			//--- unpin cur, right sibling ---
			status = MINIBASE_BM->unpinPage(pageNo, true);
			if(status != OK)
				return MINIBASE_CHAIN_ERROR(BUFMGR, status);
			status = MINIBASE_BM->unpinPage(rsibling, true);
			if(status != OK)
				return MINIBASE_CHAIN_ERROR(BUFMGR, status);

			return OK;
		}
	}
	else //--- lower key biggest 
	{
		//--- insert lowerKey into right sibling ---
		status = ((BTIndexPage*)rs)->insertKey(&lowerKey, 
				headerPage->keyType, lowerUpPageNo, insertRid);

		if(status != OK &&
				status == DONE)
			rRedi = false;	
		else if(status != OK)
			return MINIBASE_CHAIN_ERROR(BTREE, status);

		//--- if right sibling has space --
		if(rRedi)
		{
			//--- push up end entry into uPage --- 
			status = ((BTIndexPage *)uPage)->deleteKey(&rKey,
					headerPage->keyType, insertRid);
			if(status != OK)
				return MINIBASE_CHAIN_ERROR(BTREE, status);

			//--- get least key of right sibling ---
			status = ((BTIndexPage*)rs)->get_first(tRid, 
					&tKey, tPageNo);
			if(status != OK)
				return MINIBASE_CHAIN_ERROR(BTREE, status);

			//--- insert right sibling least key into uPage--
			status = ((BTIndexPage *)uPage)->insertKey(&tKey, 
					headerPage->keyType, rsibling, insertRid);
			if(status != OK)
				return MINIBASE_CHAIN_ERROR(BTREE, status);

			//--- unpin currpage & right sibling ---
			status = MINIBASE_BM->unpinPage(pageNo, true);
			if(status != OK)
				return MINIBASE_CHAIN_ERROR(BUFMGR, status);

			status = MINIBASE_BM->unpinPage(rsibling, true);
			if(status != OK)
				return MINIBASE_CHAIN_ERROR(BUFMGR, status);

			return OK;
		}
	} // 777 --- lower key compare with currpage end key --
}//--- 738 after locate right sibling page no, 


//--- key: index entry key value to be inserted to upper layer ---
//--- rid: the intended inserted rid for record ---
//--- pageNo: current page's page id ---
//--- upPageNo: copy up or push up PageId, points to new generated page ---
//--- split: flag whether insert new index entry to upper layer ---
//--- if split == false, key could be just initialize(not used by upper layer) ---
//--- uPage: upperPage obj, used for redistribution ---
//--- ls: left sibling

Status BTreeFile::insertHelper(const void* key, const RID rid, PageId pageNo, 
		void* l_Key, PageId& l_UpPageNo, bool& l_split, HFPage* uPage)
{
	Status status;
	//--- retrieve pageNo page ---
	HFPage* currPage;
	status = MINIBASE_BM->pinPage(pageNo, (Page *&)currPage);
	if(status != OK)
		return MINIBASE_CHAIN_ERROR(BUFMGR, status);
	
	if(currPage->get_type() == INDEX)
	{
		//--- retrieve next level page ---
		PageId nextPageId = 0;
		status = ((BTIndexPage*)currPage)->get_page_no(key, 
				headerPage->keyType, nextPageId);
		if(status != OK)
			return MINIBASE_FIRST_ERROR(BTREE, INSERT_FAILED); 

		Keytype lowerKey;
		PageId lowerUpPageNo = 0;
		bool lowerSplit = false;
		status = insertHelper(key, rid, nextPageId, &lowerKey, 
				lowerUpPageNo, lowerSplit, currPage);

		//--- check whether lower level adds new index entry ---
		//--- to this level ---
		if(lowerSplit)
		{
			//--- call insertKey() to push up loweKey -----
			RID insertRid;
			status = ((BTIndexPage*)currPage)->insertKey(&lowerKey, 
					headerPage->keyType, lowerUpPageNo, insertRid);
			//--- if insertKey() returns NO_SPACE ---
			//--- ?? can catch this NO_SPACE ---
			if(status != OK && status != DONE)
				return MINIBASE_FIRST_ERROR(BTREE, INSERT_FAILED);
			// weng else if(status != DONE)//---- need redis or split ---
			else if(status == DONE)//---- need redis or split ---
			{
				//--- check whether pageNo is the root page ---
				//--- if so, split & update root page ---
				if(pageNo == headerPage->rootPageId)
				{
					status = indexRootSplit(pageNo, &lowerKey,
							lowerUpPageNo, currPage);
					if(status != OK)
						return MINIBASE_CHAIN_ERROR(BTREE, status);

				}//--- if currpage is root page
				
				//--- if pageNo page is not root page ---
				//--- try redistribution first ---

				//--- 1. retrieve left sibling & try to redistribute---
				//--- add least key into left sibling ---
				bool lRedi = true;
				bool rRedi = true;
				
				status = indexLeftRedistribution(&lowerKey, lowerUpPageNo, 
						lRedi, currPage, uPage);
				if(status != OK)
					return MINIBASE_CHAIN_ERROR(BTREE, status);

				//--- 2. retrieve right sibling & try to redistribute---
				//--- add least key into left sibling ---
				if(!lRedi)
				{
//??? try right redistribution

				}//-------650 try right redis end ----

				
				//--- 3. if both redistribution returns false, split & ---
				//---construct key push up ---
				if(!lRedi && !rRedi)
				{
					//--- mark split = true ---
					l_split = true;

					//--- split ---
					//--- create a new index page l2 to hold data---
					PageId newPageId = INVALID_PAGE;
					Page * newPage;
					status = MINIBASE_BM->newPage(newPageId, (Page *&)newPage);
					if(status != OK){
						return MINIBASE_FIRST_ERROR(BTREE, ROOT_ALLOC_ERROR);
						// weng //return MINIBASE_FIRST_ERROR(BTREE, ROOT_ALLOC_ERROR);
					}
					((BTIndexPage *)newPage)->init(newPageId);
					//--- copy mid to end entries to new page from pageNo page ---

					//--- 1. get the total index entries amount & whether overflow entry --
					//--- is before mid or after mid ---
					RID tRid;
					Keytype tKey;
					PageId tPageNo;
						status = ((BTIndexPage*)currPage)->get_first(tRid, &tKey, tPageNo);
					if(status != OK)
						return MINIBASE_CHAIN_ERROR(BTREE, status);

					int totalEntry = 0;
					int lKeyPos = 0;

					while(true)
					{
						if(keyCompare(&lowerKey, &tKey, 
									headerPage->keyType) < 0)
							lKeyPos = totalEntry;

						status = ((BTIndexPage*)currPage)->get_next(tRid, &tKey, tPageNo);
						if(status != OK && status != NOMORERECS)
							return MINIBASE_CHAIN_ERROR(BTREE, status);
						if(status == NOMORERECS)
							break;

						totalEntry++;
					}

					totalEntry++;

					status = ((BTIndexPage*)currPage)->get_first(tRid, &tKey, tPageNo);
					if(status != OK)
						return MINIBASE_CHAIN_ERROR(BTREE, status);

					//--- 2. skip front entries ---
					//--- ?? right middle --- 
					for(int i = 0; i < (totalEntry + 1) / 2; i++)
					{
						status = ((BTIndexPage*)currPage)->get_next(tRid, &tKey, tPageNo);
						if(status != OK && status != NOMORERECS)
							return MINIBASE_CHAIN_ERROR(BTREE, status);
					}

					//--- 3. copy back entries to new page & delete if from currPage---
					while(true)
					{
						//--- copy ---
						RID tInsertRid;
						status = ((BTIndexPage*)newPage)->insertKey(&tKey, 
								headerPage->keyType, tPageNo, tInsertRid);
						//--- if insertKey() returns NO_SPACE ---
						//--- ?? can catch this NO_SPACE ---
						if(status != OK && status != DONE)
							return MINIBASE_FIRST_ERROR(BTREE, INSERT_FAILED);

						//--- delete ---
						status = ((BTIndexPage*)currPage)->deleteKey(&tKey, headerPage->keyType, tInsertRid);
						if(status != OK)
							return MINIBASE_CHAIN_ERROR(BTREE, status);

						//--- get next copy ---
						status = ((BTIndexPage*)currPage)->get_next(tRid, &tKey, tPageNo);
						if(status != OK && status != NOMORERECS)
							return MINIBASE_CHAIN_ERROR(BTREE, status);
						if(status == NOMORERECS)
							break;
					}
					//--- 4. insert new entry  ---
					if(lKeyPos < (totalEntry + 1) / 2)
					{
						status = ((BTIndexPage*)currPage)->insertKey(&lowerKey, 
								headerPage->keyType, lowerUpPageNo, insertRid);
						//--- if insertKey() returns NO_SPACE ---
						//--- ?? can catch this NO_SPACE ---
						if(status != OK && status != DONE)
							return MINIBASE_FIRST_ERROR(BTREE, INSERT_FAILED);
					}
					else
					{
						status = ((BTIndexPage*)newPage)->insertKey(&lowerKey, 
								headerPage->keyType, lowerUpPageNo, insertRid);
						//--- if insertKey() returns NO_SPACE ---
						//--- ?? can catch this NO_SPACE ---
						if(status != OK && status != DONE)
							return MINIBASE_FIRST_ERROR(BTREE, INSERT_FAILED);
					}

					//--- 5. update lowerKey, lowerUpPageNo as ---
					//--- mid entry(first entry in new page), and newPageId  ---
					status = ((BTIndexPage*)newPage)->get_first(tRid, &tKey, tPageNo);
					if(status != OK)
						return MINIBASE_CHAIN_ERROR(BTREE, status);
					
					// weng &tKey
					l_Key = &tKey;
					l_UpPageNo = newPageId;

					//--- 6. delete step 5 mid entry from new page(push up) ---
					status = ((BTIndexPage*)newPage)->get_first(tRid, &tKey, tPageNo);
					if(status != OK)
						return MINIBASE_CHAIN_ERROR(BTREE, status);

					status = ((BTIndexPage*)newPage)->deleteKey(&tKey, 
							headerPage->keyType, insertRid);
					if(status != OK)
						return MINIBASE_FIRST_ERROR(BTREE, status);

					//---  unpin cur, new page ---
					status = MINIBASE_BM->unpinPage(newPageId, true);
					if(status != OK)
						return MINIBASE_CHAIN_ERROR(BUFMGR, status);
					status = MINIBASE_BM->unpinPage(pageNo, true);
					if(status != OK)
						return MINIBASE_CHAIN_ERROR(BUFMGR, status);

					return OK;

				}// need redis or split : line 282 
				
				//---  unpin  new page ---
				status = MINIBASE_BM->unpinPage(pageNo, true);
				if(status != OK)
					return MINIBASE_CHAIN_ERROR(BUFMGR, status);

				//--- directly insert lower key into the currPage --

				} // lowerSplit true

				return OK;
			}//?? redis or split 
		} //--- ??lowe splitis true
	}//? currPage is index page

///*			
		else //--- currPage is leaf page ---
		{
			//--- call insertRec() to insert key & rid -----
			RID insertRid;
			status = ((BTLeafPage*)currPage)->insertRec(key, 
					headerPage->keyType, rid, insertRid);
			//--- if insertRec() returns NO_SPACE ---
			//--- ?? can catch this NO_SPACE ---
			if(status != OK && status != DONE)
				return MINIBASE_FIRST_ERROR(BTREE, INSERT_FAILED);
			else if(status == DONE)//---- need redis or split ---
			{
				//--- check whether pageNo is the root page ---
				//--- if so, split & update root page ---
				if(pageNo == headerPage->rootPageId)
				{
					//--- split ---
					//--- create a new root page ---
					// allocate a root page to the header page.
					// weng 
					AttrType keytype = headerPage->keyType; 
					int keysize = headerPage->keysize;					
	
					PageId rootPageId = INVALID_PAGE;
					Page * rootPage;
					// allocate a root page by calling newPage.
					status = MINIBASE_BM->newPage(rootPageId, (Page *&)rootPage);
					if(status != OK){
						return MINIBASE_FIRST_ERROR(BTREE, ROOT_ALLOC_ERROR);
						
					}

					// Store the meta info into the header page.
					headerPage->rootPageId = rootPageId;
					// weng keytype is the same
					headerPage->keyType = keytype;
					headerPage->keysize = keysize;
					((BTIndexPage *)rootPage)->init(rootPageId);

					//--- create a new leaf page l2 to hold data---
					PageId newPageId = INVALID_PAGE;
					Page * newPage;
					status = MINIBASE_BM->newPage(newPageId, (Page *&)newPage);
					if(status != OK){
						return MINIBASE_FIRST_ERROR(BTREE, ROOT_ALLOC_ERROR);
						
					}
					((BTLeafPage *)newPage)->init(newPageId);
					//--- copy mid to end entries to new page from pageNo page ---

					//--- 1. get the total index entries amount & whether overflow entry --
					//--- is before mid or after mid ---
					RID tRid;
					Keytype tKey;
					// weng PageId tDataRid;
					RID tDataRid;
						status = ((BTLeafPage*)currPage)->get_first(tRid, &tKey, tDataRid);
					if(status != OK)
						return MINIBASE_CHAIN_ERROR(BTREE, status);

					int totalEntry = 0;
					int lKeyPos = 0;

					while(true)
					{
						// weng KeyCompare(&l_Key...
						if(keyCompare(l_Key, &tKey, 
									headerPage->keyType) < 0)
							lKeyPos = totalEntry;

						status = ((BTLeafPage*)currPage)->get_next(tRid, &tKey,
								tDataRid);
						if(status != OK && status != NOMORERECS)
							return MINIBASE_CHAIN_ERROR(BTREE, status);
						if(status == NOMORERECS)
							break;

						totalEntry++;
					}

					totalEntry++;

					status = ((BTLeafPage*)currPage)->get_first(tRid, &tKey, tDataRid);
					if(status != OK)
						return MINIBASE_CHAIN_ERROR(BTREE, status);

					//--- 2. skip front entries ---
					//--- ?? right middle --- 
					// weng totalEntry - 1
					for(int i = 0; i < totalEntry / 2; i++)
					{
						status = ((BTLeafPage*)currPage)->get_next(tRid,
								&tKey, tDataRid);
						if(status != OK && status != NOMORERECS)
							return MINIBASE_CHAIN_ERROR(BTREE, status);
					}

					//--- 3. copy back entries to new page & delete if from currPage---
					while(true)
					{
						//--- copy ---
						RID tInsertRid;
						status = ((BTLeafPage*)newPage)->insertRec(&tKey, 
								headerPage->keyType, tDataRid, tInsertRid);
						//--- if insertKey() returns NO_SPACE ---
						//--- ?? can catch this NO_SPACE ---
						if(status != OK && status != DONE)
							return MINIBASE_FIRST_ERROR(BTREE, INSERT_FAILED);

						//--- delete ---
						status = ((SortedPage*)currPage)->deleteRecord(tRid); 

						if(status != OK)
							return MINIBASE_CHAIN_ERROR(BTREE, status);

						//--- get next copy ---
						status = ((BTLeafPage*)currPage)->get_next(tRid, &tKey,
								tDataRid);

						if(status != OK && status != NOMORERECS)
							return MINIBASE_CHAIN_ERROR(BTREE, status);
						if(status == NOMORERECS)
							break;
					}
					//--- 4. insert new entry  ---
					if(lKeyPos < (totalEntry + 1) / 2)
					{
						status = ((BTLeafPage*)currPage)->insertRec(key, 
								headerPage->keyType, rid, insertRid);
						//--- if insertKey() returns NO_SPACE ---
						//--- ?? can catch this NO_SPACE ---
						if(status != OK && status != DONE)
							return MINIBASE_FIRST_ERROR(BTREE, INSERT_FAILED);
					}
					else
					{
						status = ((BTLeafPage*)newPage)->insertRec(key, 
								headerPage->keyType, rid, insertRid);
						//--- if insertKey() returns NO_SPACE ---
						//--- ?? can catch this NO_SPACE ---
						if(status != OK && status != DONE)
							return MINIBASE_FIRST_ERROR(BTREE, INSERT_FAILED);
					}

					//--- 5. insert mid entry(first entry in new page) to root page ---
					status = ((BTLeafPage*)newPage)->get_first(tRid, &tKey, tDataRid);
					if(status != OK)
						return MINIBASE_CHAIN_ERROR(BTREE, status);

					status = ((BTIndexPage*)rootPage)->insertKey(&tKey, 
							headerPage->keyType, newPageId, insertRid);
					//--- if insertKey() returns NO_SPACE ---
					//--- ?? can catch this NO_SPACE ---
					if(status != OK && status != DONE)
						return MINIBASE_FIRST_ERROR(BTREE, INSERT_FAILED);

					//---6. set root page left link ---
					((BTIndexPage*)rootPage)->setLeftLink(pageNo);

					//---  unpin rootpage and new page & cur page ---
					status = MINIBASE_BM->unpinPage(rootPageId, true);
					if(status != OK)
						return MINIBASE_CHAIN_ERROR(BUFMGR, status);
					status = MINIBASE_BM->unpinPage(newPageId, true);
					if(status != OK)
						return MINIBASE_CHAIN_ERROR(BUFMGR, status);
					status = MINIBASE_BM->unpinPage(pageNo, true);
					if(status != OK)
						return MINIBASE_CHAIN_ERROR(BUFMGR, status);

					return OK;
				}

				//--- if pageNo page is not root page ---
				//--- try redistribution first ---

				//--- 1. retrieve left sibling & try to redistribute---
				//--- add least key into left sibling ---
				bool lRedi = true;
				bool rRedi = true;
				//--- 1.1 retrieve left sibling ---
				RID tmpRid;
				Keytype tmpKey;
				PageId tmpPageNo;
				status = ((BTIndexPage*)uPage)->get_first(tmpRid, &tmpKey, tmpPageNo);
				if(status != OK)
					return MINIBASE_CHAIN_ERROR(BTREE, status);

				//--- if key < least key in uPage, no left sibling ---
				if(keyCompare(key, &tmpKey, headerPage->keyType) < 0)
				{
					lRedi = false;
				}	
				else //--- find left sibling  & try to do left redistribution ---
				{
					//--- locate left sibling ---
					PageId lsibling = tmpPageNo;
					Keytype lKey;
					//bool deLKey = true;

					RID nextTmpRid = tmpRid;
					Keytype nextTmpKey;
					PageId nextTmpPageNo;
					status =((BTIndexPage*)uPage)->get_next(nextTmpRid, &nextTmpKey, 
							nextTmpPageNo);
					//--- if only one index entry in parent node ---
					if(status == NOMORERECS)
					{
						lsibling = ((BTIndexPage*)uPage)->getLeftLink();
						lKey = tmpKey;
						//deLKey = false;
					}
					else if(status != OK)
					{
						return MINIBASE_CHAIN_ERROR(BTREE, status);
					}
					else
					{
						RID nnextTmpRid = nextTmpRid;
						Keytype nnextTmpKey;
						PageId nnextTmpPageNo;
						status = ((BTIndexPage*)uPage)->get_next(nnextTmpRid, &nnextTmpKey,
								nnextTmpPageNo);
						if(status != OK && status != NOMORERECS)
							return MINIBASE_CHAIN_ERROR(BTREE, status);

						//--- if key is just bigger than first key ---
						if(keyCompare(key, &nextTmpKey, headerPage->keyType) < 0)
						{ 
							lsibling = ((BTIndexPage*)uPage)->getLeftLink();
							lKey = tmpKey;
							//deLKey = false;
						}
						else if(status == NOMORERECS) // if only two entries
						{
							lsibling = tmpPageNo;
							lKey = tmpKey;
						}
						else //-- more than 2 keys, and key bigger than second key -
						{
							while(true)
							{
								if(keyCompare(key, &nnextTmpKey, 
											headerPage->keyType) < 0)
								{
									lsibling = tmpPageNo;
									lKey = tmpKey;
									break;
								}

								//--- update search ---
								status =((BTIndexPage*)uPage)->get_next(tmpRid, &tmpKey, 
										tmpPageNo);
								if(status != OK)
									return MINIBASE_CHAIN_ERROR(BTREE, status);

								status = ((BTIndexPage*)uPage)->get_next(nextTmpRid, 
										&nextTmpKey, nextTmpPageNo);
								if(status != OK)
									return MINIBASE_CHAIN_ERROR(BTREE, status);

								status =((BTIndexPage*)uPage)->get_next(nnextTmpRid, &nnextTmpKey,
										nnextTmpPageNo);
								if(status != OK && status != NOMORERECS)
									return MINIBASE_CHAIN_ERROR(BTREE, status);

								//--- check if nnext reach end ---
								if(status == NOMORERECS)
								{
									lsibling = tmpPageNo;
									lKey = tmpKey;
									break;
								}
							}
						}
					} // find left sibling page no 

					//--- do left redistribution --- 

					//--- pin left sibling ---
					HFPage* ls;
					status = MINIBASE_BM->pinPage(lsibling, (Page*& )ls);
					if(status != OK)
						return MINIBASE_CHAIN_ERROR(BUFMGR, status);

					//--- compare key and currPage first entry ---
					RID tRid;
					Keytype tKey;
					RID tDataRid;
					status = ((BTLeafPage*)currPage)->get_first(tRid, &tKey, tDataRid);
					if(status != OK)
						return MINIBASE_CHAIN_ERROR(BTREE, status);

					//--- if key is least, insert it into left sibling --
					if(keyCompare(key, &tKey, headerPage->keyType) < 0)
					{
						status = ((BTLeafPage*)ls)->insertRec(key, 
								headerPage->keyType, tDataRid, insertRid);
						if(status != OK && status == DONE)
							lRedi = false;	
						else if(status != OK)
							return MINIBASE_CHAIN_ERROR(BTREE, status);

						//--- if left sibling has space --
						//--- get end entry of left sibling & ---
						//---inset into uPage to replace lKey --- 
						if(lRedi)
						{
							//--- get end entry of left sibling ---
							status = ((BTLeafPage*)ls)->get_first(tRid, 
									&tKey, tDataRid);
							if(status != OK && status != NOMORERECS)
								return MINIBASE_CHAIN_ERROR(BTREE, status);

							//--- ?? should use memcpy ---
							Keytype endKey = tKey;

							while(true)
							{
								status = ((BTLeafPage*)ls)->get_next(tRid, &tKey, 
										tDataRid);
								if(status != OK && status != NOMORERECS)
									return MINIBASE_CHAIN_ERROR(BTREE, status);

								if(status == NOMORERECS)
									break;

								endKey = tKey;
							}
							//--- push up end entry into uPage --- 
							status = ((BTIndexPage *)uPage)->deleteKey(&lKey,
									headerPage->keyType, insertRid);
							if(status != OK)
								return MINIBASE_CHAIN_ERROR(BTREE, status);

							status = ((BTIndexPage *)uPage)->insertKey(&endKey, 
									headerPage->keyType, pageNo, insertRid);
							if(status != OK)
								return MINIBASE_CHAIN_ERROR(BTREE, status);

							//--- unpin cur, left sbiling ---
							status = MINIBASE_BM->unpinPage(pageNo, true);
							if(status != OK)
								return MINIBASE_CHAIN_ERROR(BUFMGR, status);
							status = MINIBASE_BM->unpinPage(lsibling, true);
							if(status != OK)
								return MINIBASE_CHAIN_ERROR(BUFMGR, status);


							return OK;
						}
					}
					else //--- insert pageNo least key into left sibling & uPage &
						//--- delete itself ---
						//--- insert key into pageNo page ---
					{
						//--- get least key in pageNo page ---
						status = ((BTLeafPage*)currPage)->get_first(tRid, 
								&tKey, tDataRid);
						if(status != OK && status != NOMORERECS)
							return MINIBASE_CHAIN_ERROR(BTREE, status);

						//--- insert least key into left sibling ---
						status = ((BTLeafPage*)ls)->insertRec(&tKey, 
								headerPage->keyType, tDataRid, insertRid);
						if(status != OK && status == DONE)
							lRedi = false;	
						else if(status != OK)
							return MINIBASE_CHAIN_ERROR(BTREE, status);

						if(lRedi)
						{
							//--- delete least key in pageNo page ---
							status = ((SortedPage*)currPage)->deleteRecord(tRid);
							if(status != OK)
								return MINIBASE_CHAIN_ERROR(BTREE, status);

							//--- delete useless key in uPage ---
							status = ((BTIndexPage *)uPage)->deleteKey(&lKey,
									headerPage->keyType, insertRid);
							if(status != OK)
								return MINIBASE_CHAIN_ERROR(BTREE, status);

							//--- insert least key into uPage ---
							status = ((BTIndexPage *)uPage)->insertKey(&tKey, 
									headerPage->keyType, pageNo, insertRid);
							if(status != OK)
								return MINIBASE_CHAIN_ERROR(BTREE, status);

							//--- insert new entry into pageNo page ---
							status = ((BTLeafPage*)currPage)->insertRec(key, 
									headerPage->keyType, rid, insertRid);
							if(status != OK)
								return MINIBASE_CHAIN_ERROR(BTREE, status);

							//--- unpin cur, l sibling page ---
							status = MINIBASE_BM->unpinPage(pageNo, true);
							if(status != OK)
								return MINIBASE_CHAIN_ERROR(BUFMGR, status);
							status = MINIBASE_BM->unpinPage(lsibling, true);
							if(status != OK)
								return MINIBASE_CHAIN_ERROR(BUFMGR, status);


							return OK;
						}
					}
				}// try left sibling redist --

				//--- 2. retrieve right sibling & try to redistribute---
				//--- add least key into left sibling ---
				if(!lRedi)
				{
					//--- locate left sibling ---
					PageId rsibling = tmpPageNo;
					Keytype rKey;
					//bool deRKey = true;

					//--- if key < least key in uPage, find right sibling ---
					if(keyCompare(key, &tmpKey, headerPage->keyType) < 0)
					{
						rsibling = tmpPageNo;
						rKey = tmpKey;
					}
					else//--- key >= least key in uPage ---
					{
						RID nextTmpRid = tmpRid;
						Keytype nextTmpKey;
						PageId nextTmpPageNo;
						status =((BTIndexPage*)uPage)->get_next(nextTmpRid, &nextTmpKey, 
								nextTmpPageNo);
						//--- if only one index entry in parent node ---
						if(status == NOMORERECS)
						{
							rRedi = false;
						}
						else if(status != OK)
						{
							return MINIBASE_CHAIN_ERROR(BTREE, status);
						}
						else //--- else if >= 2 index entry in parent node ---
						{
							RID nnextTmpRid = nextTmpRid;
							Keytype nnextTmpKey;
							PageId nnextTmpPageNo;
							status = ((BTIndexPage*)uPage)->get_next(nnextTmpRid, &nnextTmpKey, nnextTmpPageNo);
							if(status != OK && status != NOMORERECS)
								return MINIBASE_CHAIN_ERROR(BTREE, status);

							//--- if key is just bigger than first key ---
							if(keyCompare(key, &nextTmpKey, headerPage->keyType) < 0)
							{ 
								rsibling = nextTmpPageNo;
								rKey = nextTmpKey;
								//deLKey = false;
							}
							else if(status == NOMORERECS) // if only two entries
							{
								rRedi = false;
							}
							else //-- more than 2 keys, and key bigger than second key -
							{
								while(true)
								{
									if(keyCompare(key, &nnextTmpKey, 
												headerPage->keyType) < 0)
									{
										rsibling = nnextTmpPageNo;
										rKey = nnextTmpKey;
										break;
									}

									//--- update search ---
									status =((BTIndexPage*)uPage)->get_next(tmpRid, &tmpKey, 
											tmpPageNo);
									if(status != OK)
										return MINIBASE_CHAIN_ERROR(BTREE, status);

									status = ((BTIndexPage*)uPage)->get_next(nextTmpRid, 
											&nextTmpKey, nextTmpPageNo);
									if(status != OK)
										return MINIBASE_CHAIN_ERROR(BTREE, status);

									status =((BTIndexPage*)uPage)->get_next(nnextTmpRid, &nnextTmpKey,
											nnextTmpPageNo);
									if(status != OK && status != NOMORERECS)
										return MINIBASE_CHAIN_ERROR(BTREE, status);

									//--- check if nnext reach end ---
									if(status == NOMORERECS)
									{
										rRedi = false;
										break;
									}
								}
							}
						} // find right sibling page no 

						//--- do right redistribution --- 
						if(rRedi)
						{
							//--- pin right sibling ---
							HFPage* rs;
							status = MINIBASE_BM->pinPage(rsibling, (Page*& )rs);
							if(status != OK)
								return MINIBASE_CHAIN_ERROR(BUFMGR, status);

							//--- compare key and currPage end entry ---
							RID tRid;
							Keytype tKey;
							RID tDataRid;
							status = ((BTLeafPage*)currPage)->get_first(tRid, &tKey,
									tDataRid);
							if(status != OK)
								return MINIBASE_CHAIN_ERROR(BTREE, status);

							//--- if key is biggest among currPage key, ---
							//--- insert it into right sibling,  ---
							//--- otherwise insert end key of currPage into right sibling --


							//--- get end entry of currPage ---
							//--- ?? should use memcpy ---
							Keytype endKey = tKey;
							RID endRid = tDataRid;
							while(true)
							{
								status = ((BTLeafPage *)currPage)->get_next(tRid, 
										&tKey, tDataRid);
								if(status != OK && status != NOMORERECS)
									return MINIBASE_CHAIN_ERROR(BTREE, status);

								if(status == NOMORERECS)
									break;

								endKey = tKey;
								endRid = tDataRid;
							}
							//--- compare lower key with endKey of currPage ---
							if(keyCompare(key, &endKey,
										headerPage->keyType) < 0) // lower key smaller
							{
								//--- insert endKey into right sibling ---
								status = ((BTLeafPage*)rs)->insertRec(&endKey, 
										headerPage->keyType, endRid, insertRid);

								if(status != OK &&
										status == DONE)
									rRedi = false;	
								else if(status != OK)
									return MINIBASE_CHAIN_ERROR(BTREE, status);

								//--- if right sibling has space --
								if(rRedi)
								{
									//--- delete cur page end try ---
									status = ((SortedPage*)currPage)->deleteRecord(endRid);
									if(status != OK)
										return MINIBASE_CHAIN_ERROR(BTREE, status);

									//--- insert new entry in cur page ---
									status = ((BTLeafPage*)currPage)->insertRec(key,
											headerPage->keyType, rid, insertRid);
									if(status != OK)
										return MINIBASE_CHAIN_ERROR(BTREE, status);

									//--- push up end entry into uPage --- 
									status = ((BTIndexPage *)uPage)->deleteKey(&rKey,
											headerPage->keyType, insertRid);
									if(status != OK)
										return MINIBASE_CHAIN_ERROR(BTREE, status);

									status = ((BTIndexPage *)uPage)->insertKey(&endKey, 
											headerPage->keyType, rsibling, insertRid);
									if(status != OK)
										return MINIBASE_CHAIN_ERROR(BTREE, status);

									//--- unpin cur, right sibling ---
									status = MINIBASE_BM->unpinPage(pageNo, true);
									if(status != OK)
										return MINIBASE_CHAIN_ERROR(BUFMGR, status);
									status = MINIBASE_BM->unpinPage(rsibling, true);
									if(status != OK)
										return MINIBASE_CHAIN_ERROR(BUFMGR, status);

									return OK;
								}
							}
							else //--- lower key biggest 
							{
								//--- insert lowerKey into right sibling ---
								status = ((BTLeafPage*)rs)->insertRec(key, 
										headerPage->keyType, rid, insertRid);

								if(status != OK &&
										status == DONE)
									rRedi = false;	
								else if(status != OK)
									return MINIBASE_CHAIN_ERROR(BTREE, status);

								//--- if right sibling has space --
								if(rRedi)
								{
									//--- push up end entry into uPage --- 
									status = ((BTIndexPage *)uPage)->deleteKey(&rKey,
											headerPage->keyType, insertRid);
									if(status != OK)
										return MINIBASE_CHAIN_ERROR(BTREE, status);

									//--- get least key of right sibling ---
									status = ((BTLeafPage*)rs)->get_first(tRid, 
											&tKey, tDataRid);
									if(status != OK)
										return MINIBASE_CHAIN_ERROR(BTREE, status);

									//--- insert right sibling least key into uPage--
									status = ((BTIndexPage *)uPage)->insertKey(&tKey, 
											headerPage->keyType, rsibling, insertRid);
									if(status != OK)
										return MINIBASE_CHAIN_ERROR(BTREE, status);

									//--- unpin currpage & right sibling ---
									status = MINIBASE_BM->unpinPage(pageNo, true);
									if(status != OK)
										return MINIBASE_CHAIN_ERROR(BUFMGR, status);

									status = MINIBASE_BM->unpinPage(rsibling, true);
									if(status != OK)
										return MINIBASE_CHAIN_ERROR(BUFMGR, status);

									return OK;
								}
							} // 777 --- lower key compare with currpage end key --
						}//--- 738 after locate right sibling page no, 
						//do right redistribution 
					}//-------650 try right redis end ----


					//--- 3. if both redistribution returns false, split & ---
					//---construct key push up ---
					if(!lRedi && !rRedi)
					{
						//--- mark split = true ---
						l_split = true;

						//--- split ---
						//--- create a new leaf page l2 to hold data---
						PageId newPageId = INVALID_PAGE;
						Page * newPage;
						status = MINIBASE_BM->newPage(newPageId, (Page *&)newPage);
						if(status != OK){
							return MINIBASE_FIRST_ERROR(BTREE, ROOT_ALLOC_ERROR);
							
						}
						((BTLeafPage *)newPage)->init(newPageId);
						//--- copy mid to end entries to new page from pageNo page ---

						//--- 1. get the total index entries amount & whether overflow entry --
						//--- is before mid or after mid ---
						RID tRid;
						Keytype tKey;
						RID tDataRid;
						status = ((BTLeafPage*)currPage)->get_first(tRid,
								&tKey, tDataRid);
						if(status != OK)
							return MINIBASE_CHAIN_ERROR(BTREE, status);

						int totalEntry = 0;
						int lKeyPos = 0;

						while(true)
						{
							if(keyCompare(key, &tKey, 
										headerPage->keyType) < 0)
								lKeyPos = totalEntry;

							status = ((BTLeafPage*)currPage)->get_next(tRid, &tKey, 
									tDataRid);
							if(status != OK && status != NOMORERECS)
								return MINIBASE_CHAIN_ERROR(BTREE, status);
							if(status == NOMORERECS)
								break;

							totalEntry++;
						}

						totalEntry++;

						status = ((BTLeafPage*)currPage)->get_first(tRid, 
								&tKey, tDataRid);
						if(status != OK)
							return MINIBASE_CHAIN_ERROR(BTREE, status);

						//--- 2. skip front entries ---
						//--- ?? right middle --- 
						for(int i = 0; i < (totalEntry + 1) / 2; i++)
						{
							status = ((BTLeafPage*)currPage)->get_next(tRid, &tKey, 
									tDataRid);
							if(status != OK && status != NOMORERECS)
								return MINIBASE_CHAIN_ERROR(BTREE, status);
						}

						//--- 3. copy back entries to new page & delete if from currPage---
						while(true)
						{
							//--- copy ---
							RID tInsertRid;
							status = ((BTLeafPage*)newPage)->insertRec(&tKey, 
									headerPage->keyType, tDataRid, tInsertRid);
							//--- if insertKey() returns NO_SPACE ---
							//--- ?? can catch this NO_SPACE ---
							if(status != OK && status != DONE)
								return MINIBASE_FIRST_ERROR(BTREE, INSERT_FAILED);

							//--- delete ---
							//status = ((BTLeafPage*)currPage)->deleteRecord(&tKey, 
							//		headerPage->keyType, tInsertRid);
							// weng, should call deleteRecord(tRid);
							status = ((BTLeafPage *)currPage)->deleteRecord(tRid);
							if(status != OK)
								return MINIBASE_CHAIN_ERROR(BTREE, status);

							//--- get next copy ---
							status = ((BTLeafPage*)currPage)->get_next(tRid, 
									&tKey, tDataRid);
							if(status != OK && status != NOMORERECS)
								return MINIBASE_CHAIN_ERROR(BTREE, status);
							if(status == NOMORERECS)
								break;
						}
						//--- 4. insert new entry  ---
						if(lKeyPos < (totalEntry + 1) / 2)
						{
							status = ((BTLeafPage*)currPage)->insertRec(key, 
									headerPage->keyType, rid, insertRid);
							//--- if insertKey() returns NO_SPACE ---
							//--- ?? can catch this NO_SPACE ---
							if(status != OK && status != DONE)
								return MINIBASE_FIRST_ERROR(BTREE, INSERT_FAILED);
						}
						else
						{
							status = ((BTLeafPage*)newPage)->insertRec(key, 
									headerPage->keyType, rid, insertRid);
							//--- if insertKey() returns NO_SPACE ---
							//--- ?? can catch this NO_SPACE ---
							if(status != OK && status != DONE)
								return MINIBASE_FIRST_ERROR(BTREE, INSERT_FAILED);
						}

						//--- 5. update lowerKey, lowerUpPageNo as ---
						//--- mid entry(first entry in new page), and newPageId  ---
						status = ((BTLeafPage*)newPage)->get_first(tRid, 
								&tKey, tDataRid);
						if(status != OK)
							return MINIBASE_CHAIN_ERROR(BTREE, status);
						// weng l_Key = &tKey
						l_Key = &tKey;
						l_UpPageNo = newPageId;

						//---  unpin cur, new page ---
						status = MINIBASE_BM->unpinPage(newPageId, true);
						if(status != OK)
							return MINIBASE_CHAIN_ERROR(BUFMGR, status);
						status = MINIBASE_BM->unpinPage(pageNo, true);
						if(status != OK)
							return MINIBASE_CHAIN_ERROR(BUFMGR, status);

						return OK;

					}// need redis or split : line 282 

					//---  unpin  new page ---
					status = MINIBASE_BM->unpinPage(pageNo, true);
					if(status != OK)
						return MINIBASE_CHAIN_ERROR(BUFMGR, status);

					return OK;
				}//--- currpage is leaf page
				return OK;
			}
		}
//*/ weng
		//??? unpin currpage, since lowersplit is false
	return OK;
} //---? function

Status BTreeFile::insert(const void *key, const RID rid) {
	// put your code here
	Status status;
	//--- retrieve root page ---
	HFPage* rootPage;
	status = MINIBASE_BM->pinPage(headerPage->rootPageId, (Page*&) rootPage);
	if(status != OK)
		return MINIBASE_CHAIN_ERROR(BUFMGR, status);

	//-- call insertHelper ---
	Keytype lowerKey;
	PageId lowerUpPageNo;
	bool lowerSplit = false;
	HFPage *tmpPage = NULL;
	status = insertHelper(key, rid, headerPage->rootPageId, &lowerKey, 
		lowerUpPageNo, lowerSplit, tmpPage);

	if(status != OK)
		return MINIBASE_FIRST_ERROR(BTREE, INSERT_FAILED);

  return OK;
}

// Delete the data (key, rid) pairs
Status BTreeFile::Delete(const void *key, const RID rid) {
        Status status;
	PageId pageNo = headerPage->rootPageId;
	// delete start from accessing the root node.
        status = deleteHelper(key, rid, pageNo);
        if(status != OK)
                return MINIBASE_FIRST_ERROR(BTREE, DELETE_ERROR);

        return OK;
}

// Supporting function for Delete(), recursively find the leaf node and delete the leaf node.
// Only implemented delete in the leaf node without redistribution when the record drop below the half.
Status BTreeFile::deleteHelper(const void *key, const RID rid, PageId pageNo)
{
        // ************* //
        Status status;
        short type;
        Page *tmpPage = NULL;
        SortedPage *tmpSPage = NULL;
        BTIndexPage *tmpIPage = NULL;
        BTLeafPage *tmpLPage = NULL;

        status = MINIBASE_BM->pinPage(pageNo, tmpPage);
        if(status != OK)
                return MINIBASE_FIRST_ERROR(BTREE, CANT_PIN_PAGE);

        tmpSPage = (SortedPage *)tmpPage;
        type = tmpSPage->get_type();

        // If it's a index page, recursively calling helper until find the leaf page.
        if(type == INDEX){
                PageId tmpId;
                tmpIPage = (BTIndexPage *)tmpPage;
                // Get_page_no will decide which index page to go.
                status = tmpIPage->get_page_no(key, headerPage->keyType, tmpId);
                if(status != OK)
                        return MINIBASE_FIRST_ERROR(BTREE, GET_PAGE_NO_ERROR);

                // Recursively find the page until it's a leaf node.
                status = deleteHelper(key, rid, tmpId);
                if(status != OK)
                        return MINIBASE_FIRST_ERROR(BTREE, DELETE_ERROR);

        }

        // if it's a leaf page, find the record and call deleteRecord.
        if(type == LEAF){
                RID tmpRid;
                AttrType keyType;
                KeyDataEntry tmpEntry;
                tmpLPage = (BTLeafPage *)tmpPage;
                keyType = headerPage->keyType;

                status = tmpLPage->get_first(tmpRid, &tmpEntry.key, tmpEntry.data.rid);
                if(status != OK)
                        return MINIBASE_CHAIN_ERROR(BTREE, status);

                // Calling KeyCompare until find the entry that needed to be deleted
                int res = 0;
                res = keyCompare(key, &tmpEntry.key, keyType);
                while(res >= 0){
                        // find exactlt the data record, delete it
                        if((tmpEntry.data.rid == rid) && res == 0)
                        {
                                status = tmpLPage->deleteRecord(tmpRid);
                                if(status != OK)
                                        return MINIBASE_CHAIN_ERROR(BTREE, status);

                                // ??? duplicate record, and if the page is empty.
                                break;
                        }else
                        {
                                // else get next record,
                                status = tmpLPage->get_next(tmpRid, &tmpEntry.key, tmpEntry.data.rid);
                                if(status != OK)
                                {
                                        if(status == NOMORERECS)
                                                return MINIBASE_FIRST_ERROR(BTREE, REC_NOT_FOUND);
                                        else
                                                return MINIBASE_CHAIN_ERROR(BTREE, status);
                                }
                        }
                }
        }

        // unpin the searching page at end.
        status = MINIBASE_BM->unpinPage(pageNo);
        if(status != OK)
                return MINIBASE_FIRST_ERROR(BTREE, CANT_UNPIN_PAGE);
	
	return OK;
} 

// create a scan with given keys, dealing with the following cases.
// It will call the btreefilescan to do the job recursively.
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
IndexFileScan *BTreeFile::new_scan(const void *lo_key, const void *hi_key) {
	IndexFileScan * scan;	
	int keysize = headerPage->keysize;
	AttrType keytype = headerPage->keyType;
	PageId rootid = headerPage->rootPageId;
	scan = new BTreeFileScan(lo_key, hi_key, keytype, keysize, rootid);
	return scan;
}

int BTreeFile::keysize(){
	int keysize;
	keysize = headerPage->keysize;
	return keysize;
}
