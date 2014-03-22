/*****************************************************************************/
/*************** Implementation of the Buffer Manager Layer ******************/
/*****************************************************************************/

#include "buf.h"


// Define buffer manager error messages here
//enum bufErrCodes  {...};

// Define error message here
static const char* bufErrMsgs[] = { 
  // error message strings go here
  "Not enough memory to allocate hash entry",
  "Inserting a duplicate entry in the hash table",
  "Removing a non-existing entry from the hash table",
  "Page not in hash table",
  "Not enough memory to allocate queue node",
  "Poping an empty queue",
  "OOOOOOPS, something is wrong",
  "Buffer pool full",
  "Not enough memory in buffer manager",
  "Page not in buffer pool",
  "Unpinning an unpinned page",
  "Freeing a pinned page"
};

// Create a static "error_string_table" object and register the error messages
// with minibase system 
static error_string_table bufTable(BUFMGR,bufErrMsgs);

//*************************************************************
// This is the implementation of BufMgr
// Initializes a buffer manager managing "numbuf" buffers.
// Disregard the "replacer" parameter for now. In the full 
// implementation of minibase, it is a pointer to an object
// representing one of several buffer pool replacement schemes.
//************************************************************
BufMgr::BufMgr (int numbuf, Replacer *replacer) {
	// put your code here
	
	// get the number of buffers, and allocate a memory space for the buffer pool.
	unsigned int i;
	numBuffers = numbuf;
	bufPool = new Page[numBuffers];
	for(i = 0; i < numBuffers; i++){
		bufDescr[i].pageId = INVALID_PAGE;
		bufDescr[i].pinCount = 0;
		bufDescr[i].dirtyBit = false;
	}
	
	// init hash table
	hashTable = (HashTable *)malloc(sizeof(HashTable));
	for(i = 0; i < HTSIZE; i++){
		hashTable->directory[i] = NULL;
	}
	
	LRU = NULL;
	MRU = NULL;
}

//*************************************************************
// This is the implementation of ~BufMgr
// Flush all valid dirty pages to disk
//************************************************************
BufMgr::~BufMgr(){
	// put your code here
	// flush all the dirty pages.
	Status status;
	status = flushAllPages();
	if(status != OK)
		 MINIBASE_CHAIN_ERROR(BUFMGR,status);
	// delete the mem space 
	// delete [] bufDescr;
	delete [] bufPool;
	LRU = NULL;
	MRU = NULL;
	
	int i;
	for(i = 0; i < HTSIZE; i++){
		if(hashTable->directory[i] != NULL)
		{
			Bucket* hWalker = hashTable->directory[i];
			while(hWalker!= NULL)
			{
				Bucket* deleteWalker = hWalker;
				hWalker = hWalker->next;
				free(deleteWalker);
			}
		}
	}
	free(hashTable);
	hashTable = NULL;
}

//*************************************************************
// This is the implementation of pinPage
// Check if this page is in buffer pool, otherwise
// find a frame for this page, read in and pin it.
// also write out the old page if it's dirty before reading
// if emptyPage==TRUE, then actually no read is done to bring
// the page
//************************************************************
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page*& page, int emptyPage) { 
	// put your code here	
	// Try to find the Given ID page in the buf pool.
	
 	PageId bufId = -1;
	PageId prevPage = INVALID_PAGE;
	bool needtoWrite = FALSE;
	Status status;
	// lookup the hash table for the bufID 
	status = hashGetFrameId(PageId_in_a_DB, bufId);	
     	
	if(status != OK){
                if(minibase_errors.error_index() == HASHNOTFOUND)
                {	
			bufId = -1;
			minibase_errors.clear_errors();
		}
		else
                        return MINIBASE_FIRST_ERROR(BUFMGR, INTERNALERROR);
        }
	
	// if not find the page in the buf pool, call the replacer to replace a page.
	if(bufId == -1){
		status = replace(bufId);
		
		// replacer fail to find a valid page, then return an error.
		if(status != OK || bufId < 0 || bufId >= (int)numBuffers)
		{
			bufId = 0;
			page = NULL;
			return MINIBASE_CHAIN_ERROR(BUFMGR, status);
		}
		
		
		// replacer find a non-empty page, then remember the previous page, else use it directly. 	
		// also check the Descr uf the replacing page needs to flush to disk.
		prevPage = bufDescr[bufId].pageId;
		needtoWrite = bufDescr[bufId].dirtyBit;
		
		// delete the old element from the hashtable
		/* ****************** hashRemove *********************/
		if(prevPage != INVALID_PAGE){	
			status = hashRemove(prevPage);
			if(status != OK)
				return MINIBASE_FIRST_ERROR(BUFMGR, HASHREMOVEERROR);
		}
		// update the buf descriptor for the replacing page
		bufDescr[bufId].pageId = INVALID_PAGE;
		// ??? Need not to set the pinCount to 0, becuase the replaced one must have 0 pin.
		bufDescr[bufId].dirtyBit = false;
		// hash insert
		/* ******************hashPut********************/
		status = hashPut(PageId_in_a_DB, bufId);
		if(status != OK)
			return MINIBASE_CHAIN_ERROR(BUFMGR, status);	
		
		bufDescr[bufId].pageId = PageId_in_a_DB;
		bufDescr[bufId].dirtyBit = false;
		// ??? pin count add one if insertion is successful
		bufDescr[bufId].pinCount ++;
		
		if(prevPage != INVALID_PAGE && needtoWrite)
		{
			// check if it needs to unpin and the DB error ???.
			status = MINIBASE_DB->write_page(prevPage, &bufPool[bufId]);
			if(status != OK)
				return MINIBASE_CHAIN_ERROR(BUFMGR, status);
		}
	
		// check if the page is not empty, if it is, then read the page content into buf.
		// otherwise, return the empty page directly.
		if(emptyPage == FALSE)
		{
			// check the DB error ??? if return error, return the info to replacer.	
			// rollback the previous operations. bufDescr and hash
			status = MINIBASE_DB->read_page(PageId_in_a_DB, &bufPool[bufId]);	
			if(status != OK)
			{
				MINIBASE_CHAIN_ERROR(BUFMGR, status);
				bufDescr[bufId].pageId = INVALID_PAGE;
				bufDescr[bufId].dirtyBit = false;
				if(bufDescr[bufId].pinCount > 0)
					bufDescr[bufId].pinCount --;
				else
					bufDescr[bufId].pinCount = 0;				
				
				return MINIBASE_CHAIN_ERROR(BUFMGR, status);
			}
		}
	
		// finally, return the actual data page.
		page = &bufPool[bufId];
		return OK;
	}

	// if the given page in the buffer pool
	if(bufId >= 0 && bufId < (int)numBuffers)
	{
		page = &bufPool[bufId];
		bufDescr[bufId].pinCount++;
	}	
	
	return OK;
}//end pinPage

//*************************************************************
// This is the implementation of unpinPage
// hate should be TRUE if the page is hated and FALSE otherwise
// if pincount>0, decrement it and if it becomes zero,
// put it in a group of replacement candidates.
// if pincount=0 before this call, return error.
//************************************************************
Status BufMgr::unpinPage(PageId page_num, int dirty=FALSE, int hate = FALSE){
	// put your code here
	// try to find the given ID page in the buf pool
	PageId bufId = -1;
	Status status;
	ReplaceList *node = NULL;
	
	// find the upin page in Hash table
	status = hashGetFrameId(page_num, bufId);
	
	if(status != OK){
                if(minibase_errors.error_index() == HASHNOTFOUND)
                        return MINIBASE_FIRST_ERROR(BUFMGR, BUFFERPAGENOTFOUND);
                else
                        return MINIBASE_FIRST_ERROR(BUFMGR, INTERNALERROR);
        }
	
	// Check all the possible bad bufId
        if(bufId < 0 || bufId >= (int)numBuffers)
                return MINIBASE_FIRST_ERROR(BUFMGR, BUFFERPAGENOTFOUND);

	if(bufDescr[bufId].pageId == INVALID_PAGE)
		return MINIBASE_FIRST_ERROR(BUFMGR, BUFFERPAGENOTPINNED);
		
	if(bufDescr[bufId].pinCount == 0)
		return MINIBASE_FIRST_ERROR(BUFMGR, BUFFERPAGENOTPINNED);
	// if the page is in the buffer pool, then update the bufDescr. 
	bufDescr[bufId].pinCount --;
	// ??? what if the pinCount == 0? Do I need to inform the replacer?

	if(dirty == TRUE)
		bufDescr[bufId].dirtyBit = true;	
		
	// attention, must call the replacer after all the bufDescr had been updated.
	// inform the replace list
	node = (ReplaceList*)malloc(sizeof(ReplaceList));
	node->frameId = bufId;
	node->hate = hate;
	// add this node to the replace list.
	status = addReplaceList(node);
	if(status != OK)
		return MINIBASE_FIRST_ERROR(BUFMGR, INTERNALERROR);
	
	return OK;
}

//*************************************************************
// This is the implementation of newPage
// call DB object to allocate a run of new pages and
// find a frame in the buffer pool for the first page              
// and pin it. If buffer is full, ask DB to deallocate
// all these pages and return error
//************************************************************
Status BufMgr::newPage(PageId& firstPageId, Page*& firstpage, int howmany) {
	// put your code here
	Status status_1, status_2;
	// call DB to allocate a run of new pages
	status_1 = MINIBASE_DB->allocate_page(firstPageId, howmany);
	if(status_1 != OK)
		return MINIBASE_CHAIN_ERROR(BUFMGR, status_1);
	// pin this empty page, set the empty flag as true;
	status_1 = pinPage(firstPageId, firstpage, TRUE);
	
	// if pin unsuccessful, undo all the operations.
	if(status_1 != OK)
	{
		MINIBASE_FIRST_ERROR(BUFMGR, BUFMGRMEMORYERROR);
		status_2 = MINIBASE_DB->deallocate_page(firstPageId, howmany);
		if(status_2 != OK )
			return MINIBASE_CHAIN_ERROR(BUFMGR, status_2);
		return status_2;
	}
	
	return OK;
}

//*************************************************************
// This is the implementation of freePage
// User should call this method if it needs to delete a page
// this routine will call DB to deallocate the page
//************************************************************
Status BufMgr::freePage(PageId globalPageId){
	// put your code here
	PageId bufId = -1;
	Status status;	

	status = hashGetFrameId(globalPageId, bufId);
	// if the page not in the buffer pool. deallocate it.
	if(status != OK){
		if(minibase_errors.error_index() == HASHNOTFOUND)
		{	
			status = MINIBASE_DB->deallocate_page(globalPageId);
			if(status != OK)
				return MINIBASE_CHAIN_ERROR(BUFMGR,status);
			minibase_errors.clear_errors();
		}
		else
			return MINIBASE_FIRST_ERROR(BUFMGR, INTERNALERROR);
	}	
	
	//Then the page is in the buffer pool.
	// bad hash result
	if(bufId < 0 || bufId >= (int)numBuffers)
		return MINIBASE_FIRST_ERROR(BUFMGR, BUFFERPAGENOTFOUND);
	
	// if the page is still pinned, return error.
	if(bufDescr[bufId].pinCount > 1)
		return MINIBASE_FIRST_ERROR(BUFMGR, BUFFERPAGEPINNED);

	// set the bufDescr as free
	bufDescr[bufId].pinCount --; 
	bufDescr[bufId].pageId = INVALID_PAGE;
	bufDescr[bufId].dirtyBit = false;
	
	// then deallocate the page
	status = MINIBASE_DB->deallocate_page(globalPageId);
	if(status != OK)
		return MINIBASE_CHAIN_ERROR(BUFMGR, status);

	return OK;
}

//*************************************************************
// This is the implementation of flushPage
// Used to flush a particular page of the buffer pool to disk
// Should call the write_page method of the DB class
//************************************************************
Status BufMgr::flushPage(PageId pageid) {
	// put your code here
	PageId bufId = -1;
	Status status;
	
	// find the page first
	status = hashGetFrameId(pageid, bufId);	
	
	// pageid not found in the buffer pool
	if(status != OK){
		if(minibase_errors.error_index() == HASHNOTFOUND)
	        	return MINIBASE_FIRST_ERROR(BUFMGR, BUFFERPAGENOTFOUND);
		else
			return MINIBASE_FIRST_ERROR(BUFMGR, INTERNALERROR);
	}

        // bad hash result
        if(bufId < 0 || bufId >= (int)numBuffers)
                return MINIBASE_FIRST_ERROR(BUFMGR, BUFFERPAGENOTFOUND);
	
	// page is pinned, then return error	
	if(bufDescr[bufId].pinCount != 0)
		return MINIBASE_FIRST_ERROR(BUFMGR, BUFFERPAGEPINNED);
	
	// flush the page into disk, and update the hash table and bufDescr
	status = MINIBASE_DB->write_page(bufDescr[bufId].pageId, &bufPool[bufId]);
	if(status != OK)
		return MINIBASE_CHAIN_ERROR(BUFMGR, status);

	status = hashRemove(pageid);
	if(status != OK)
		return MINIBASE_FIRST_ERROR(BUFMGR, status);

	// update the bufDescr
	bufDescr[bufId].pageId = INVALID_PAGE;
	bufDescr[bufId].dirtyBit = false;
	
	return OK;
}
    
//*************************************************************
// This is the implementation of flushAllPages
// Flush all pages of the buffer pool to disk, as per flushPage.
//************************************************************
Status BufMgr::flushAllPages(){
	// put your code here
	// write all the pages to the disk if it's dirty.
	int pinned = 0;
	unsigned int i;
	Status status;
	//PageId pageid;
	
	// flush all the page to disk no matter what	
	for(i = 0; i < numBuffers; i++)
	{	
		// if buffer pool i is empty or the page is not dirty,
		//  then continue to next buffer frame. which will save disk I/O
		if(bufDescr[i].pageId == INVALID_PAGE || bufDescr[i].dirtyBit == false)
			continue;
		// have page is pinned
		if(bufDescr[i].pinCount != 0)
			pinned ++;
		// flush the page into disk, and update the hash table and bufDescr
		status = MINIBASE_DB->write_page(bufDescr[i].pageId, &bufPool[i]);
		if(status != OK)
			return MINIBASE_CHAIN_ERROR(BUFMGR, status);

		status = hashRemove(bufDescr[i].pageId);
		if(status != OK)
			return MINIBASE_FIRST_ERROR(BUFMGR, status);

		// update the bufDescr
		bufDescr[i].pageId = INVALID_PAGE;
		bufDescr[i].dirtyBit = false;
	}

	// some of the unpinned page has been flushed to the disk
	if(pinned != 0)
		return MINIBASE_FIRST_ERROR(BUFMGR, BUFFERPAGEPINNED);
	
	return OK;
}


/*** Methods for compatibility with project 1 ***/
//*************************************************************
//** This is the implementation of pinPage
//************************************************************
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page*& page, int emptyPage, const char *filename){
  //put your code here
  return OK;
}

//*************************************************************
//** This is the implementation of unpinPage
//************************************************************
Status unpinPage(PageId globalPageId_in_a_DB, int dirty, const char *filename){
  //put your code here
  return OK;
}

//*************************************************************
// This is the implementation of getNumUnpinnedBuffers
// Get number of unpinned buffers
//************************************************************
unsigned int BufMgr::getNumUnpinnedBuffers(){
	//put your code here
	unsigned int ans;
	unsigned int i;
	for(i = 0; i < numBuffers; i++)
	{
		if(bufDescr[i].pinCount == 0)
			ans ++;
	}
	return ans;
}

//--- hash function implementation ---

//--- hash calculation ----------------
//--- calculate the bucket should be --
//--- according to pageId -------------
//--- input should be >0 --------------
int BufMgr::hash(PageId pageId)
{
	return (HTA * pageId + HTB) % HTSIZE;
}

//--- delete hash bucket ---------------
Status BufMgr::hashRemove(PageId pageId)
{
	//--- get the target directory id ---
	int dirId = this->hash(pageId);

	//--- scan the target list and delete -
	Bucket* bWalker = this->hashTable->directory[dirId];
	Bucket* bWalkerFree = bWalker;
	//--- if is empty list ---
	if(bWalker == NULL) return MINIBASE_FIRST_ERROR(BUFMGR, HASHREMOVEERROR);
	//--- else not empty list ---
	//--- 1. check head ------
	if(bWalker->pageId == pageId)
	{
		//--- relocate bucket head position --
		bWalker = bWalker->next;
		this->hashTable->directory[dirId] = bWalker;
		//--- free deleted space ---
		free(bWalkerFree);
		return OK;
	}
	else //2. not at head ---
	{
		while(bWalker->next != NULL)
		{
			if(bWalker->next->pageId == pageId)
			{
				//--- points to going freed memory ----
				bWalkerFree = bWalker->next;
				//--- reconstruct the list -----------
				bWalker->next = bWalker->next->next;
				//--- free useless memory ------------
				free(bWalkerFree);
				return OK;
			}
		}
	}
	//--- if not found ----
	return MINIBASE_FIRST_ERROR(BUFMGR, HASHREMOVEERROR);
}

//--- insert hash bucket ---------------
Status BufMgr::hashPut(PageId pageId, int frameId)
{
	//--- get the target directory id ---
	int dirId = this->hash(pageId);

	//--- insert bucket -----------------
	Bucket* bWalker = this->hashTable->directory[dirId];

	if(bWalker == NULL) //--- 1. if is empty bucket list ---
	{
		bWalker = (Bucket* )calloc(1, sizeof(Bucket));
		//--- check if allocate successfully ------
		if(bWalker == NULL)
			return MINIBASE_FIRST_ERROR(BUFMGR, HASHMEMORY);

		bWalker->pageId = pageId;
		bWalker->frameId = frameId;
		bWalker->next = NULL;

		this->hashTable->directory[dirId] = bWalker;
	}
	else //--- 2. if is not empty bucket list ---
	{
		//--- if list head is duplicate -------
		if(bWalker->pageId == pageId && bWalker->frameId == frameId)
			return MINIBASE_FIRST_ERROR(BUFMGR, HASHDUPLICATEINSERT);
		//--- else check whether following entries are duplicate ---
		while(bWalker->next != NULL)
		{
			bWalker = bWalker->next;

			if(bWalker->pageId == pageId && bWalker->frameId == frameId)
				return MINIBASE_FIRST_ERROR(BUFMGR, HASHDUPLICATEINSERT);
		}

		bWalker->next = (Bucket* )calloc(1, sizeof(Bucket));

		//--- check allocation whether successfully ---
		if(bWalker->next == NULL)
			return MINIBASE_FIRST_ERROR(BUFMGR, HASHMEMORY);

		bWalker->next->pageId = pageId;
		bWalker->next->frameId = frameId;
		bWalker->next->next = NULL;
	}
	return OK;
}


//--- get frame id ----------------------
//--- input: pageId, frameId(arbitary value,---
//--- changed to valid value after execution)--
Status BufMgr::hashGetFrameId(PageId pageId, int& frameId)
{
	//--- get the target directory id ---
	int dirId = this->hash(pageId);

	//--- check the bucket list ---
	Bucket* bWalker = this->hashTable->directory[dirId];
	while(bWalker != NULL)
	{
		if(bWalker->pageId == pageId)
		{
			frameId = bWalker->frameId;
			return OK;
		}

		bWalker = bWalker->next;
	}

	return MINIBASE_FIRST_ERROR(BUFMGR, HASHNOTFOUND);
}


//------ replacement policy implementation --------

//--- find the node previous location in MRU or LRU list-
//--- if is a new node return NULL ----------------------
ReplaceList* BufMgr::findList(ReplaceList* node)
{
	ReplaceList* walker = this->MRU;
	//--- check MRU list ---
	while(walker != NULL)
	{
		if(walker->frameId == node->frameId)
			return walker;

		walker = walker->next;
	}
	//--- check LRU list ---
	walker = this->LRU;
	while(walker != NULL)
	{
		if(walker->frameId == node->frameId)
			return walker;

		walker = walker->next;
	}

	return NULL;
}


//---- every time unPin, add new node to LRU or MRU list ---
//---- or modify the node along with its new access time & -
//----- & its love/hate value(love conquers hate)        ---
Status BufMgr::addReplaceList(ReplaceList* node)
{
	if(node->hate)//--- if hate add to MRU list or modify LRU(previous love)
	{
		ReplaceList* walker = this->MRU;
		ReplaceList* oldNode = this->findList(node);

		if(oldNode == NULL)//--- if input is new node
		{
			if(walker == NULL)//-- if MRU is empty list
			{
				this->MRU = (ReplaceList* )calloc(1, sizeof(ReplaceList));

				if(this->MRU == NULL)
					return MINIBASE_FIRST_ERROR(BUFMGR, QMEMORYERROR);

				memcpy(this->MRU, node, sizeof(ReplaceList));
				//--- free the input node ---
				free(node);
				return OK;
			}
			else // else MRU is not empty list ---
			{
				ReplaceList* oldList = this->MRU;

				this->MRU = (ReplaceList* )calloc(1, sizeof(ReplaceList));
				if(this->MRU == NULL)
					return MINIBASE_FIRST_ERROR(BUFMGR, QMEMORYERROR);

				memcpy(this->MRU, node, sizeof(ReplaceList));
				this->MRU->next = oldList;
				//--- free the input node ---
				free(node);
				return OK;
			}
		}
		else //--- else input is old node --
		{
			//--- if previous is in MRU ---
			//--- move node to MRU head ---
			if(oldNode->hate)
			{
				//--- check MRU head ---
				if(walker->frameId == node->frameId)
				{
					//--- free input node ---
					free(node);	
					return OK;
				}

				//-- if not in MRU head ----
				ReplaceList* deleteNode = walker;
				while(walker->next != NULL)
				{
					if(walker->next->frameId == node->frameId)
					{
						//--- if previous is in the tail ---
						if(walker->next->next == NULL)
							return OK;

						deleteNode = walker->next;
						walker->next = walker->next->next;
						free(deleteNode);
						break;
					}
					walker = walker->next;
				}

				ReplaceList* oldList= this->MRU;
				this->MRU = (ReplaceList* )calloc(1, sizeof(ReplaceList));
				if(this->MRU == NULL)
					return MINIBASE_FIRST_ERROR(BUFMGR, QMEMORYERROR);

				memcpy(this->MRU, node, sizeof(ReplaceList));
				this->MRU->next = oldList;
				//--- free input node ---
				free(node);
				return OK;
			}
			else//--- if previous is in LRU---
				//--- update old node, and move it to LRU tail ---
			{
				walker = this->LRU;
				//--- check LRU head ---
				if(walker->frameId == node->frameId)
				{
					this->LRU = walker->next;
					//--- if only one element ---
					if(this->LRU == NULL)
					{
						this->LRU = walker;
						return OK;
					}

					//-- free previous node ---
					free(walker);

					ReplaceList* newWalker = this->LRU;
						while(newWalker->next != NULL)
							newWalker = newWalker->next;

					newWalker->next = (ReplaceList* )calloc(1, sizeof(ReplaceList));
					if(newWalker->next == NULL)
						return MINIBASE_FIRST_ERROR(BUFMGR, QMEMORYERROR);

					memcpy(newWalker->next, node, sizeof(ReplaceList));
					newWalker->next->hate = false;
					//--- free input node ---
					free(node);	
					return OK;
				}

				//-- if not in LRU head ----
				ReplaceList* deleteNode = walker;
				while(walker->next != NULL)
				{
					if(walker->next->frameId == node->frameId)
					{
						//--- if previous is in the tail ---
						if(walker->next->next == NULL)
							return OK;

						deleteNode = walker->next;
						walker->next = walker->next->next;
						free(deleteNode);
					}
					walker = walker->next;
				}
				walker->next = (ReplaceList* )calloc(1, sizeof(ReplaceList));
				if(walker->next == NULL)
					return MINIBASE_FIRST_ERROR(BUFMGR, QMEMORYERROR);

				memcpy(walker->next, node, sizeof(ReplaceList));
				walker->next->hate = false;
				//--- free input node ---
				free(node);
				return OK;
			}
		}
	}
	else //--- otherwise, love, delete previous from MRU list(if any)--
		//--- and add to or modify LRU list
	{
		ReplaceList* walker = this->LRU;
		ReplaceList* oldNode = this->findList(node);

		if(oldNode == NULL)//--- if input is new node
		{
			if(walker == NULL)//-- if LRU is empty list
			{
				this->LRU = (ReplaceList* )calloc(1, sizeof(ReplaceList));

				if(this->LRU == NULL)
					return MINIBASE_FIRST_ERROR(BUFMGR, QMEMORYERROR);

				memcpy(this->LRU, node, sizeof(ReplaceList));
				//--- free the input node ---
				free(node);
				return OK;
			}
			else // else LRU is not empty list ---
			{
				while(walker->next != NULL)
					walker = walker->next;

				walker->next = (ReplaceList* )calloc(1, sizeof(ReplaceList));
				if(walker->next == NULL)
					return MINIBASE_FIRST_ERROR(BUFMGR, QMEMORYERROR);

				memcpy(walker->next, node, sizeof(ReplaceList));
				//--- free the input node ---
				free(node);
				return OK;
			}
		}
		else //--- else input is old node --
		{
			//--- if previous is in MRU ---
			//--- delete in MRU and add new node to love tail ---
			if(oldNode->hate)
			{
				//--- delete in MRU ---
				walker = this->MRU;
				//--- check MRU head ---
				if(walker->frameId == node->frameId)
				{
					this->MRU = walker->next;
					//--- free MRU old node ---
					free(walker);
				}
				else
				{
					//-- if not in MRU head ----
					ReplaceList* deleteNode = walker;
					while(walker->next != NULL)
					{
						if(walker->next->frameId == node->frameId)
						{
							deleteNode = walker->next;
							walker->next = walker->next->next;
							free(deleteNode);
							break;
						}
						walker = walker->next;
					}
				}

				//--- add to LRU tail ---
				walker = this->LRU;

				if(walker == NULL)
				{
					this->LRU = (ReplaceList* )calloc(1, sizeof(ReplaceList));
					if(this->LRU == NULL)
						return MINIBASE_FIRST_ERROR(BUFMGR, QMEMORYERROR);
					memcpy(this->LRU, node, sizeof(ReplaceList));
					//--- free input node---
					free(node);
					return OK;
				}
				else
				{
					while(walker->next != NULL)
						walker = walker->next;

					walker->next = (ReplaceList* )calloc(1, sizeof(ReplaceList));
					if(walker->next == NULL)
						return MINIBASE_FIRST_ERROR(BUFMGR, QMEMORYERROR);
					memcpy(walker->next, node, sizeof(ReplaceList));
					//--- free input node---
					free(node);
					return OK;
				}
			}
			else//--- if previous is in LRU---
				//---move it to LRU tail ---
			{
				walker = this->LRU;
				//--- check LRU head ---
				if(walker->frameId == node->frameId)
				{
					this->LRU = walker->next;
					//--- if only one element ---
					if(this->LRU == NULL)
					{
						this->LRU = walker;
						return OK;
					}

					//-- free previous node ---
					free(walker);

					ReplaceList* newWalker = this->LRU;
						while(newWalker->next != NULL)
							newWalker = newWalker->next;

					newWalker->next = (ReplaceList* )calloc(1, sizeof(ReplaceList));
					if(newWalker->next == NULL)
						return MINIBASE_FIRST_ERROR(BUFMGR, QMEMORYERROR);

					memcpy(newWalker->next, node, sizeof(ReplaceList));
					//--- free input node ---
					free(node);	
					return OK;
				}

				//-- if not in LRU head ----
				ReplaceList* deleteNode = walker;
				while(walker->next != NULL)
				{
					if(walker->next->frameId == node->frameId)
					{
						//--- if previous is in the tail ---
						if(walker->next->next == NULL)
							return OK;

						deleteNode = walker->next;
						walker->next = walker->next->next;
						free(deleteNode);
					}
					walker = walker->next;
				}
				walker->next = (ReplaceList* )calloc(1, sizeof(ReplaceList));
				if(walker->next == NULL)
					return MINIBASE_FIRST_ERROR(BUFMGR, QMEMORYERROR);

				memcpy(walker->next, node, sizeof(ReplaceList));
				//--- free input node ---
				free(node);
				return OK;
			}
		}
	}
}

//---- input: frameId(arbitary value, changed after execution)---
Status BufMgr::replace(int& frameId)
{
	//--- check MRU:hate list first --
	ReplaceList* walker = this->MRU;
	if(this->MRU != NULL)
	{
		if(this->bufDescr[this->MRU->frameId].pinCount == 0)
		{
			frameId = this->MRU->frameId;
			this->MRU = this->MRU->next;
			free(walker);
			return OK;
		}

		ReplaceList* deleteWalker = walker->next;
		while(walker->next != NULL)
		{
			if(this->bufDescr[walker->next->frameId].pinCount == 0)
			{
				deleteWalker = walker->next;
				frameId = walker->next->frameId;
				walker->next = walker->next->next;
				free(deleteWalker);
				return OK;
			}
			walker = walker->next;
		}
	}
	//--- check LRU:love list ----
	walker = this->LRU;
	if(this->LRU != NULL)
	{
		if(this->bufDescr[this->MRU->frameId].pinCount == 0)
		{
			frameId = this->LRU->frameId;
			this->LRU = this->LRU->next;
			free(walker);
			return OK;
		}

		ReplaceList* deleteWalker = walker->next;
		while(walker->next != NULL)
		{
			if(this->bufDescr[walker->next->frameId].pinCount == 0)
			{
				deleteWalker = walker->next;
				frameId = walker->next->frameId;
				walker->next = walker->next->next;
				free(deleteWalker);
				return OK;
			}
			walker = walker->next;
		}
	}
	
	// ??? both empty, check if there is empty frame.
	unsigned int index;
	for(index = 0; index < numBuffers; index++){
		if(bufDescr[index].pinCount == 0)
			frameId = (int)index;
			break;
	}
	
	if(index < numBuffers)
		return OK;	
	else
		return MINIBASE_FIRST_ERROR(BUFMGR, QEMPTY); // ??? the error should be no candidate
}

