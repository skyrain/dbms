/*
	btindex_page.C
	Define the B tree index page and all the operations on a index page.

	- Overall description of your algorithms and data structures
	
	// ------------------- insertKey ------------------------
	// Inserts a <key, page pointer> value into the index node.
	// This is accomplished by a call to SortedPage::insertRecord()
	// This function sets up the recPtr field for the call to
	// SortedPage::insertRecord()
	Status BTIndexPage::insertKey (const void *key, AttrType key_type, PageId pageNo, RID& rid)

	// ------------------- deletekey ------------------------
	// Delete an index entry with a key, only deal with the push up in index.
	Status BTIndexPage::deleteKey (const void *key, AttrType key_type, RID& curRid)

	// ------------------ get_page_no -----------------------
	// This function encapsulates the search routine to search a
	// BTIndexPage. It uses the standard search routine as
	// described in the textbook, and returns the page_no of the
	// child to be searched next.
	Status BTIndexPage::get_page_no(const void *key, AttrType key_type, PageId & pageNo)

	// calls to HFPage::firstRecord() to get the first key pair    
	Status BTIndexPage::get_first(RID& rid, void *key,PageId & pageNo)

	// calls to HFPage::nextRecord() to get the next key pair
	Status get_next (RID& rid, void *key, PageId & pageNo);

	// ------------------- Iterators ------------------------
	// The two functions get_first and get_next provide an
	// iterator interface to the records on a specific BTIndexPage.
	// get_first returns the first <key, pageNo> pair from the page,
	// while get_next returns the next pair on the page.
	// These functions make calls to HFPage::firstRecord() and
	// HFPage::nextRecord(), and then split the flat record into its
	// two components: namely, the key and pageNo.
	// Should return NOMORERECS when there are no more pairs.
 	Status get_first_sp(RID& rid, void *key, PageId & pageNo, PageId spPageNo);
	Status get_next_sp (RID& rid, void *key, PageId & pageNo, PageId spPageNo);


	// ------------------- Left Link ------------------------
	// You will recall that index pages have a left-most
	// pointer that is followed whenever the search key value
	// is less than the least key value in the index node. The
	// previous page pointer is used to implement the left link.
	PageId getLeftLink(void) { return getPrevPage(); }
	void   setLeftLink(PageId left) { setPrevPage(left); }
	
	- Anything unusual in your implementation

	Design and implementation details are the same as what descriped in the course website.

	- What functionalities not supported well

	All functionalities are implemented, but not work properly so far, we will fix that soon.

*/

#include "btindex_page.h"

// Define your Error Messge here
const char* BTIndexErrorMsgs[] = {
	"Insert index failed",
	"Delete index failed",
	"Get Record error"
};

// define the error code
enum btIndexErrCodes{
	INDEXINSERTFAIL,
	INDEXDELFAIL,
	GETRECERROR,
};

static error_string_table btree_table(BTINDEXPAGE, BTIndexErrorMsgs);

// ------------------- insertKey ------------------------
// Inserts a <key, page pointer> value into the index node.
// This is accomplished by a call to SortedPage::insertRecord()
// This function sets up the recPtr field for the call to
// SortedPage::insertRecord()
Status BTIndexPage::insertKey (const void *key,
                               AttrType key_type,
                               PageId pageNo,
                               RID& rid)
{
	// the same process as in the lead page.
	// init the parameter for a data entry
	KeyDataEntry target;
	Datatype datatype;
	datatype.pageNo = pageNo;

	// Call Key::make_entry for this data record
	int entryLen;
	entryLen = 0;
	make_entry(&target, key_type, key, (nodetype)type, datatype, &entryLen);

	char* targetC = (char* )calloc(1, entryLen);
	memcpy(targetC, &target.key, entryLen - sizeof(PageId));
	memcpy(targetC + entryLen - sizeof(PageId), &target.data,
			sizeof(PageId));

	Status status;
	// Call SortedPage::insertRecord() to accomplish the insert.
	status = SortedPage::insertRecord(key_type, (char*)&target, entryLen, rid);
	
	//--- free 
	free(targetC);

	if(status != OK)
	{
		if(status == DONE)
			return DONE;
		else
			return MINIBASE_FIRST_ERROR(BTINDEXPAGE, INDEXINSERTFAIL);
	}
	else
		return OK;
}

// ------------------- deletekey ------------------------
// Delete an index entry with a key, only deal with the push up in index.
Status BTIndexPage::deleteKey (const void *key, AttrType key_type, RID& curRid)
{
	// In this case, delete an index entry only happen during push up.
	Status status;
	PageId tmpId;
	Keytype tmpKey;
	int found = 0;
	
	status = get_first(curRid, (void *)&tmpKey, tmpId);
	if(status != OK)
		return MINIBASE_FIRST_ERROR(BTINDEXPAGE, GETRECERROR);
	
	int res = 0;
	res = keyCompare(key, (void *)&tmpKey, key_type);
	
	// Fint the key that once larger than a certain tmpKey.
	while(res > 0){
		status = get_next(curRid, (void *)&tmpKey, tmpId);
		if(status != OK)
			break;
		res = keyCompare(key, (void *)&tmpKey, key_type);
	}
	
	// when break with the while loop, determine whether find the record exactly
	// otherwise, delete the previous key
	found = keyCompare(key, (void *)&tmpKey, key_type);
	if(found != 0)
		curRid.slotNo --;
	
	// Delete the record by calling deketeRecord in sorted page.
	status = deleteRecord(curRid);
	if(status != OK)
		return MINIBASE_FIRST_ERROR(BTINDEXPAGE, INDEXDELFAIL);
	
	return OK;
}

// ------------------ get_page_no -----------------------
// This function encapsulates the search routine to search a
// BTIndexPage. It uses the standard search routine as
// described in the textbook, and returns the page_no of the
// child to be searched next.
Status BTIndexPage::get_page_no(const void *key,
                                AttrType key_type,
                                PageId & pageNo)
{
	Status status;
	RID tmpRid;
	Keytype curKey;
	PageId curId;
	int res;

	res = 0;
	curId = INVALID_PAGE;	

	// if the key is NULL, return current page
	if(key == NULL)
	{
		pageNo = ((HFPage *)this)->page_no();
		return OK;
	}
	
	status = get_first(tmpRid, &curKey, curId);
	if(status != OK)
		return MINIBASE_FIRST_ERROR(BTINDEXPAGE, GETRECERROR);

	// if the key small than the first key, return left link of the index.
	res = keyCompare(key, &curKey, key_type);
	if(res < 0)
	{
		pageNo = getLeftLink();
		return OK;
	}
	
	// if is not in the left link, find a right index page to go.
	if(res >= 0)
	{
		bool larger_than_prev = false;
		bool smaller_than_next = false;
		PageId prevId;
		Keytype prevKey;
		int prev = 0;
		int next = 0;
		int numKeys = ((SortedPage*)this)->numberOfRecords();
	
		prevId = curId;
		prevKey = curKey;

		int i ;
		for(i = 0; i < numKeys; i++)
		{
			// go through all the records to find the page.	
			status = get_next(tmpRid, &curKey, curId);
			if(status != OK){	
				if(status == NOMORERECS)
					break;
				else
					return MINIBASE_FIRST_ERROR(BTINDEXPAGE, GETRECERROR);
			}

			// Compare the key with its previous and next key.
			prev = keyCompare(key, &prevKey, key_type);
			next = keyCompare(key, &curKey, key_type);
			if(prev >= 0)
				larger_than_prev = true;
			if(next < 0)
				smaller_than_next = true;
			
			// Find the exact pageNo, return OK.
			if(larger_than_prev && smaller_than_next)
			{
				pageNo = prevId;
				return OK;
			}else{
				// If it doesn't find, reset the flag and try again.
				larger_than_prev = false;
				smaller_than_next =false;
				prevId = curId;
				prevKey = curKey;
			}
		}
		
		// if didn't find the in the loop, the key must locate at the last index.
		pageNo = prevId;
		return OK;
	}
	return MINIBASE_FIRST_ERROR(BTINDEXPAGE, GETRECERROR);
}

// calls to HFPage::firstRecord() to get the first key pair    
Status BTIndexPage::get_first(RID& rid,
                              void *key,
                              PageId & pageNo)
{
	// get the first record by calling the HFPage function;
	Status status;
	status = ((HFPage*)this)->firstRecord(rid);
	if(status != OK)
	{
		// return DONE means no record.??
		if(status == DONE){
			pageNo = INVALID_PAGE;
			return NOMORERECS;
		}
		else
			return MINIBASE_FIRST_ERROR(BTINDEXPAGE, GETRECERROR);
	}
	// find the record page by calling the HFPage function;
	char *rec = NULL;
	int recLen = 0;
	
	status = ((HFPage*)this)->returnRecord(rid, rec, recLen);
	if(status != OK)
		return MINIBASE_FIRST_ERROR(BTINDEXPAGE, GETRECERROR);
	
	// get the first data key pair
	// cast the packed the pageNo.
	Datatype *tmpdt = new Datatype;
	tmpdt->pageNo = pageNo;
	
	get_key_data(key, tmpdt, (KeyDataEntry *)rec, recLen, (nodetype)type);
	// unpack the pageNo.
	pageNo = tmpdt->pageNo; 	

	return OK;
}

// calls to HFPage::nextRecord() to get the next key pair
Status BTIndexPage::get_next(RID& rid, void *key, PageId & pageNo)
{
	// get the next record by calling the HFPage function;
        Status status;
	RID nextRid;
        status = ((HFPage*)this)->nextRecord(rid, nextRid);
        if(status != OK)
        {
                // return DONE means no record. ??
                if(status == DONE){
                        pageNo = INVALID_PAGE;
                        return NOMORERECS;
                }
                else
                        return MINIBASE_FIRST_ERROR(BTINDEXPAGE, GETRECERROR);
        }

	 // find the record page by calling the HFPage function;
        char *rec = NULL;
        int recLen = 0;
        status = ((HFPage*)this)->returnRecord(nextRid, rec, recLen);
        if(status != OK)
                return MINIBASE_FIRST_ERROR(BTINDEXPAGE, GETRECERROR);
	
	// get the next data key pair
        // cast the packed the pageNo.
        Datatype *tmpdt = new Datatype;
        tmpdt->pageNo = pageNo;
        get_key_data(key, tmpdt, (KeyDataEntry *)rec, recLen, (nodetype)type);
        // unpack the pageNo.
        pageNo = tmpdt->pageNo;

	rid = nextRid;
	return OK;
}

// calls to HFPage::firstRecord() to get the first key pair    
Status BTIndexPage::get_first_sp(RID& rid,
                              void *key,
                              PageId & pageNo, PageId spPageNo)
{
	// get the first record by calling the HFPage function;
	Status status;
	HFPage* currPage;
	status = MINIBASE_BM->pinPage(spPageNo, (Page*&) currPage);
	if(status != OK)
		return MINIBASE_CHAIN_ERROR(BUFMGR, status);

	status = ((HFPage*)currPage)->firstRecord(rid);
	if(status != OK)
	{
		// return DONE means no record.??
		if(status == DONE){
			pageNo = INVALID_PAGE;
			return NOMORERECS;
		}
		else
			return MINIBASE_FIRST_ERROR(BTINDEXPAGE, GETRECERROR);
	}
	// find the record page by calling the HFPage function;
	char *rec = NULL;
	int recLen = 0;
	
	status = ((HFPage*)currPage)->returnRecord(rid, rec, recLen);
	if(status != OK)
		return MINIBASE_FIRST_ERROR(BTINDEXPAGE, GETRECERROR);
	
	// get the first data key pair
	// cast the packed the pageNo.
	Datatype *tmpdt = new Datatype;
	tmpdt->pageNo = pageNo;
	
	get_key_data(key, tmpdt, (KeyDataEntry *)rec, recLen, (nodetype)type);
	// unpack the pageNo.
	pageNo = tmpdt->pageNo; 

	status = MINIBASE_BM->unpinPage(spPageNo, true);
	if(status != OK)
		return MINIBASE_CHAIN_ERROR(BUFMGR, status);

	return OK;
}

// calls to HFPage::nextRecord() to get the next key pair
Status BTIndexPage::get_next_sp(RID& rid, void *key, PageId & pageNo, PageId spPageNo)
{
	// get the next record by calling the HFPage function;
        Status status;
		HFPage* currPage;
		status = MINIBASE_BM->pinPage(spPageNo, (Page*&)currPage);
		if(status != OK)
			return MINIBASE_CHAIN_ERROR(BUFMGR, status);


		RID nextRid;
        status = ((HFPage*)currPage)->nextRecord(rid, nextRid);
        if(status != OK)
        {
                // return DONE means no record. ??
                if(status == DONE){
                        pageNo = INVALID_PAGE;
                        return NOMORERECS;
                }
                else
                        return MINIBASE_FIRST_ERROR(BTINDEXPAGE, GETRECERROR);
        }

	 // find the record page by calling the HFPage function;
        char *rec = NULL;
        int recLen = 0;
        status = ((HFPage*)currPage)->returnRecord(nextRid, rec, recLen);
        if(status != OK)
                return MINIBASE_FIRST_ERROR(BTINDEXPAGE, GETRECERROR);
	
	// get the next data key pair
        // cast the packed the pageNo.
        Datatype *tmpdt = new Datatype;
        tmpdt->pageNo = pageNo;
        get_key_data(key, tmpdt, (KeyDataEntry *)rec, recLen, (nodetype)type);
        // unpack the pageNo.
        pageNo = tmpdt->pageNo;

	rid = nextRid;

	status = MINIBASE_BM->unpinPage(spPageNo, true);
	if(status != OK)
		return MINIBASE_CHAIN_ERROR(BUFMGR, status);

	return OK;
}
