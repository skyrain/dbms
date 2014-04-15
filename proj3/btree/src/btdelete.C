	
{	
	Status status;
	status = deleteHelper(const void *key, const RID rid, PageId pageNo);
	if(status != OK)
		return MINIBASE_FIRST_ERROR(BTREE, DELETE_ERROR);
	
	return OK;
	

	// ************* //
	Status status;
	short type;
	Pege *tmpPage = NULL;
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
		status = tmpIPage->get_page_no(key, headerPage->key_type, tmpId);
		if(statis != OK)
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
		keyType = headerPage->key_type;	
	
		status = tmpLPage->get_first(tmpRid, &tmpEntry.key, tmpEntry.data.rid);
		if(status != OK)
			return MINIBASE_CHAIN_ERROR(BTREE, stauts);
	
		// Calling KeyCompare until find the entry that needed to be deleted
		int res = 0;
		res = KeyCompare(key, &tmpEntry.key, keyType);
		while(res >= 0){
			// find exactlt the data record, delete it
			if((tmpEntry.data.rid == rid) && res == 0)
			{
				status = tmpLPage->deleteRecord(tmpRid);
				if(status != OK)
					return MINIBASE_CHAIN_ERROR(BTREE, status)
				
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

}
