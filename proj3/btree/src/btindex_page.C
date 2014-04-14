/*
 * btindex_page.C - implementation of class BTIndexPage
 *
 * Johannes Gehrke & Gideon Glass  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
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

        Status status;
        // Call SortedPage::insertRecord() to accomplish the insert.
        status = SortedPage::insertRecord(key_type, (char*)&target, entryLen, rid);
        if(status != OK)
		{
			if(minibase_errors.error_index() == NO_SPACE)
				return MINIBASE_CHAIN_ERROR(SORTEDPAGE, status);
			else
                return MINIBASE_FIRST_ERROR(BTINDEXPAGE, INDEXINSERTFAIL);
		}
        else
                return OK;
}

// ------------------- deletekey ------------------------
// Delete an index entry with a key ??
Status BTIndexPage::deleteKey (const void *key, AttrType key_type, RID& curRid)
{
	// delete, only marks the corresponding leaf entry as deleted???
	Status status;
	status = SortedPage::deleteRecord(curRid);
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
	// if the key is NULL, return current page
	if(key == NULL)
	{
		pageNo = ((HFPage *)this)->page_no();
		return OK;
	}

	// otherwise, compare the key with other index
	int i;
	for(i = 0; i <= slotCnt - 1; i++){
		// compare the key with each record in the hfpage
		void * keyc = (void *)(data + slot[i].offset);
		if(keyCompare(key, keyc, key_type) >= 0)
		{
			// if the key is larger then, get the key data.
			Datatype *tmpdt = NULL;
			tmpdt->pageNo = INVALID_PAGE;
			get_key_data(NULL, tmpdt, (KeyDataEntry *)keyc, slot[i].length, (nodetype)type);
			pageNo = tmpdt->pageNo;
			return OK;		
		}
	}
	
	// if not find in the curr page, return the left index page.
	pageNo = getLeftLink();
	return OK;
		
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
	Datatype *tmpdt = NULL;
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
        Datatype *tmpdt = NULL;
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
	status = MINIBASE_BM->pin(spPageNo, (Page*&) currPage);
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
	Datatype *tmpdt = NULL;
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
		status = MINIBASE_BM->pin(spPageNo, (Page*& currPage));
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
        Datatype *tmpdt = NULL;
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
