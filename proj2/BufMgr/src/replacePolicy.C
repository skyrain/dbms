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

					ReplaceList* newWalker = this->LRU:
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

					ReplaceList* newWalker = this->LRU:
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

 "Not enough memory to allocate queue node",
   20   "Poping an empty queue",
QMEMORYERROR, QEMPTY

//--- LRU:pop head, MRU: pop tail ---
//---- input: frameId(arbitary value, changed after execution)---
Status BufMgr::replace(int& frameId)
{
	//--- check MRU:hate list first --
	ReplaceList* walker = this->MRU;
	if(this->MRU != NULL)
	{

	}
	//--- check LRU:love list ----
	walker = this->LRU;
	if(this->LRU != NULL)
	{
		if(this->bufDescr[this->lRU->frameId].pinCount == 0)
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

	return MINIBASE_FIRST_ERROR(BUFMGR, QEMPTY);
}
