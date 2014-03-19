//------ replacement policy implementation --------

  "Not enough memory to allocate queue node",
   20   "Poping an empty queue",
QMEMORYERROR, QEMPTY


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
	walekr = this->LRU;
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
	if(node->hate)//--- if hate add to MRU list
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
			}
			else // else MRU is not empty list ---
			{
				while(walker->next != NULL)
					walker = walker->next;

				walker->next = (ReplaceList* )calloc(1, sizeof(ReplaceList));
				if(walker->next == NULL)
					return MINIBASE_FIRST_ERROR(BUFMGR, QMEMORYERROR);

				memcpy(walker->next, node, sizeof(ReplaceList));
				//--- free the input node ---
				free(node);
			}
		}
		else //--- else input is old node --
		{
			//--- if previous is in MRU ---
			//--- move node to MRU tail ---
			if(oldNode->hate)
			{
				//--- check MRU head ---
				if(walker->frameId == node->frameId)
				{
					this->MRU = walker->next;
					//--- if only one element ---
					if(this->MRU == NULL)
					{
						this->MRU = walker;
						return OK;
					}

					//-- free previous node ---
					free(walker);

					ReplaceList* newWalker = this->MRU:
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
				
				//-- if not in MRU head ----
				//??
				while(walker->next != NULL)
				{
				

					walker = walker->next;
				}
			}
			else//--- if previous is in LRU---
				//--- update old node, and move it to LRU tail ---
			{
			}
		}
	}
	else ///--- otherwise, love, add to LRU list
	{
	}

	return OK;
}



//--- LRU:pop head, MRU: pop tail ---
//---- input: frameId(arbitary value, changed after execution)---
Status BufMgr::replace(int& frameId)
{
	
}
