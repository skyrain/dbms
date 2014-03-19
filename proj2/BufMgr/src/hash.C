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
void BufMgr::hashRemove(PageId pageId)
{
	//--- get the target directory id ---
	int dirId = this->hash(pageId);

	//--- scan the target list and delete -
	Bucket* bWalker = directory[dirId];
	Bucket* bWalkerFree = bWalker;
	//--- if is empty list ---
	if(bWalker == NULL) return;
	//--- else not empty list ---
	//--- 1. check head ------
	if(bWalker->pageId == pageId)
	{
		//--- relocate bucket head position --
		bWalker = bWalker->next;
		directory[dirId] = bWalker;
		//--- free deleted space ---
		free(bWalkerFree);
		return;
		
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
				return;
			}
		}
	}
}

//--- insert hash bucket ---------------
void BufMgr::hashPut(PageId pageId, int frameId)
{
	//--- get the target directory id ---
	int dirId = this->hash(pageId);
	
	//--- insert bucket -----------------
	Bucket* bWalker = directory[dirId];
	
	if(bWalker == NULL) //--- 1. if is empty bucket list ---
	{
		bWalker = (Bucket* )calloc(1, sizeof(Bucket));
		bWalker->pageId = pageId;
		bWalker->frameId = frameId;
		bWalker->next = NULL;

		directory[dirId] = bWalker;
	}
	else //--- 2. if is not empty bucket list ---
	{
		while(bWalker->next != NULL)
			bWalker = bWalker->next;

		bWalker->next = (Bucket* )calloc(1, sizeof(Bucket));
		bWalker->next->pageId = pageId;
		bWalker->next->frameId = frameId;
		bWalker->next->next = NULL;
	}
}

//--- get frame id ----------------------
//--- return: -1 not page not in pool ---
//--- otherwise return the frameId ------
int BufMgr::hashGetFrameId(PageId pageId)
{
	//--- get the target directory id ---
	int dirId = this->hash(pageId);
	
	//--- check the bucket list ---
	Bucket* bWalker = directory[dirId];
	while(bWalker != NULL)
	{
		if(bWalker->pageId == pageId)
			return bWalker->frameId;

		bWalker = bWalker->next;
	}

	return -1;
}
