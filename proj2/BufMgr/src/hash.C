//---- hash table 初始化， directory array 赋值为NULL －－－

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
