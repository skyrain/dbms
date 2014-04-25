/*
	btleaf_page.C
	Define the B tree leaf page and all the operations on a leaf page.

	- Overall description of your algorithms and data structures

	// Inserts a key, rid value into the leaf node. This is
	// accomplished by a call to SortedPage::insertRecord()
	// The function also sets up the recPtr field for the call
	// to SortedPage::insertRecord()
	Status insertRec(const void *key, AttrType key_type, RID dataRid, RID& rid);


	* This function performs a binary search to look for the
 	* rid of the data record. (dataRid contains the RID of
 	* the DATA record, NOT the rid of the data entry!)
	Status BTLeafPage::get_data_rid(void *key,
                                AttrType key_type,
                                RID & dataRid)

 	* These functions provide an
	* iterator interface to the records on a specific BTLeafPage.
 	* get_first returns the first key, RID from the page,
 	* while get_next returns the next key on the page.
	* These functions make calls to RecordPage::get_first() and
 	* RecordPage::get_next(), and break the flat record into its
 	* two components: namely, the key and datarid. 
	Status get_first(RID& rid, void *key, RID & dataRid);
   	Status get_next (RID& rid, void *key, RID & dataRid);
	Status get_first_sp(RID& rid, void *key, RID & dataRid, PageId spPageNo);
  	Status get_next_sp (RID& rid, void *key, RID & dataRid, PageId spPageNo);

	- Anything unusual in your implementation

	Design and implementation details are the same as what descriped in the course website.

	- What functionalities not supported well

	All functionalities are implemented, but not work properly so far, we will fix that soon.

*/

#include "btleaf_page.h"

const char* BTLeafErrorMsgs[] = {
	"Insert Record Failed",
	"Record not found",
	"No record",
	"Get Record error"
};

enum BLeafErrCodes{
	LEAFINSERTFAIL,
	RECNOFOUND,
	NOREC,
	GETRECERROR,
};

static error_string_table btree_table(BTLEAFPAGE, BTLeafErrorMsgs);
   
/*
 * Status BTLeafPage::insertRec(const void *key,
 *                             AttrType key_type,
 *                             RID dataRid,
 *                             RID& rid)
 *
 * Inserts a key, rid value into the leaf node. This is
 * accomplished by a call to SortedPage::insertRecord()
 * The function also sets up the recPtr field for the call
 * to SortedPage::insertRecord() 
 * 
 * Parameters:
 *   o key - the key value of the data record.
 *
 *   o key_type - the type of the key.
 * 
 *   o dataRid - the rid of the data record. This is
 *               stored on the leaf page along with the
 *               corresponding key value.
 *
 *   o rid - the rid of the inserted leaf record data entry.
 */
 
// * Inserts a key, rid value into the leaf node. This is
// * accomplished by a call to SortedPage::insertRecord()
// * The function also sets up the recPtr field for the call
// * to SortedPage::insertRecord()
Status BTLeafPage::insertRec(const void *key,
                              AttrType key_type,
                              RID dataRid,
                              RID& rid)
{
	// init the parameter for a data entry
	KeyDataEntry target;
	Datatype datatype;
	datatype.rid = dataRid;	
	int entryLen;	
	entryLen = 0;
	// Call Key::make_entry for this data record
	make_entry(&target, key_type, key, (nodetype)type, datatype, &entryLen);	
	
	char* targetC = (char*)calloc(1, entryLen);
	memcpy(targetC, &target.key, entryLen - sizeof(RID));
	memcpy(targetC + entryLen - sizeof(RID), &target.data, 
			sizeof(RID));

	Status status;
	// Call SortedPage::insertRecord() to accomplish the insert.
	status = SortedPage::insertRecord(key_type, targetC, entryLen, rid);
	
	//free 
	free(targetC);

	if(status != OK)
	{
		if(status == DONE)
			return DONE;

		return MINIBASE_FIRST_ERROR(BTLEAFPAGE,LEAFINSERTFAIL);
	}
	else
		return OK;
}

/*
 * This function performs a binary search to look for the
 * rid of the data record. (dataRid contains the RID of
 * the DATA record, NOT the rid of the data entry!)
 */
Status BTLeafPage::get_data_rid(void *key,
                                AttrType key_type,
                                RID & dataRid)
{
	// binary search for the the rid.
	int low;
	int med;
	int high;
	int Eq;
	
	// define the low and high
	low = 0;
	high = slotCnt - 1;
	Eq = 0;
	// binart search routine
	while(low <= high)
	{
		med = (low + high)/2;
		// find the result key by calling keyCompare
		// find the med of key by checking the slot. ??? tianyu
		void * keyc;
		//---slot_t * tmpSlot = (slot_t *)&(data[(med - 1) * sizeof(slot_t)]);
		//---keyc = (void *)(data + tmpSlot->offset);
		
		// we could access the number of ith slot by using the slot[i], Cross-boarder access
		keyc = (void *)(data + slot[med].offset);
		Eq = keyCompare(key, keyc, key_type);
		// find if the key is the desired one.
		if(Eq == 0){
			Keytype ktype;
			// if find the data, then call get_key_data
			get_key_data((void *)&ktype, (Datatype *)&dataRid,
		 	(KeyDataEntry*)keyc, slot[med].length, (nodetype)type); 
		
			// if find the key, return.
			// if not, check to find which side to go
			return OK;
		}else if(Eq > 0)
			high = med - 1;
		else
			low = med + 1;
	}

	return MINIBASE_FIRST_ERROR(BTLEAFPAGE,RECNOFOUND);
}

/* 
 * Status BTLeafPage::get_first (const void *key, RID & dataRid)
 * Status BTLeafPage::get_next (const void *key, RID & dataRid)
 * 
 * These functions provide an
 * iterator interface to the records on a BTLeafPage.
 * get_first returns the first key, RID from the page,
 * while get_next returns the next key on the page.

 * These functions make calls to RecordPage::get_first() and
 * RecordPage::get_next(), and break the flat record into its
 * two components: namely, the key and datarid. 
 */
Status BTLeafPage::get_first (RID& rid,
                              void *key,
                              RID & dataRid)
{
	// get the first record by calling the HFPage function;
	Status status;
	status = ((HFPage*)this)->firstRecord(rid);
	if(status != OK)
	{
		// return DONE means no record.??
		if(status == DONE){
			dataRid.pageNo = INVALID_PAGE;
			dataRid.slotNo = INVALID_SLOT;
			return NOMORERECS;
		}
		else
			return MINIBASE_FIRST_ERROR(BTLEAFPAGE, GETRECERROR);
	}
	// find the record page by calling the HFPage function;
	char *rec = NULL;
	int recLen = 0;
	status = ((HFPage*)this)->returnRecord(rid, rec, recLen);
	if(status != OK)
		return MINIBASE_FIRST_ERROR(BTLEAFPAGE, GETRECERROR);

	// get the first data key pair
	// pack the dataRid into Datatype
	Datatype *tmpdt = new Datatype;
	tmpdt->rid = dataRid;
	get_key_data(key, tmpdt, (KeyDataEntry *)rec, recLen, (nodetype)type);
	// unpack the dataRid
	dataRid = tmpdt->rid;
	
	return OK;
}

Status BTLeafPage::get_next (RID& rid,
                             void *key,
                             RID & dataRid)
{
	// get the next record by calling the HFPage function;
        Status status;
	RID nextRid;
        status = ((HFPage*)this)->nextRecord(rid, nextRid);
        if(status != OK)
        {
                // return DONE means no record. ??
                if(status == DONE){
                        dataRid.pageNo = INVALID_PAGE;
                        dataRid.slotNo = INVALID_SLOT;
                        return NOMORERECS;
                }
                else
                        return MINIBASE_FIRST_ERROR(BTLEAFPAGE, GETRECERROR);
        }
	
	 // find the record page by calling the HFPage function;
        char *rec = NULL;
        int recLen = 0;
        status = ((HFPage*)this)->returnRecord(nextRid, rec, recLen);
        if(status != OK)
                return MINIBASE_FIRST_ERROR(BTLEAFPAGE, GETRECERROR);
	
	// get the first data key pair
	// pack the dataRid into Datatype
	Datatype *tmpdt = new Datatype;
	tmpdt->rid = dataRid;
	tmpdt->pageNo = INVALID_PAGE;
	get_key_data(key, tmpdt, (KeyDataEntry *)rec, recLen, (nodetype)type);
	// unpack the dataRid
	dataRid = tmpdt->rid;

	rid = nextRid;
	return OK;
}


/* 
 * These functions provide an
 * iterator interface to the records on a specific BTLeafPage.
 * get_first returns the first key, RID from the page,
 * while get_next returns the next key on the page.
 * These functions make calls to RecordPage::get_first() and
 * RecordPage::get_next(), and break the flat record into its
 * two components: namely, the key and datarid. 
 */
Status BTLeafPage::get_first_sp (RID& rid,
                              void *key,
                              RID & dataRid,
							  PageId spPageNo)
{
	// get the first record by calling the HFPage function;
	Status status;
	HFPage* currPage;
	status = MINIBASE_BM->pinPage(spPageNo, (Page*&)currPage);
	if(status != OK)
		return MINIBASE_CHAIN_ERROR(BUFMGR, status);

	status = ((HFPage*)currPage)->firstRecord(rid);
	if(status != OK)
	{
		// return DONE means no record.??
		if(status == DONE){
			dataRid.pageNo = INVALID_PAGE;
			dataRid.slotNo = INVALID_SLOT;
			return NOMORERECS;
		}
		else
			return MINIBASE_FIRST_ERROR(BTLEAFPAGE, GETRECERROR);
	}
	// find the record page by calling the HFPage function;
	char *rec = NULL;
	int recLen = 0;
	status = ((HFPage*)currPage)->returnRecord(rid, rec, recLen);
	if(status != OK)
		return MINIBASE_FIRST_ERROR(BTLEAFPAGE, GETRECERROR);

	// get the first data key pair
	// pack the dataRid into Datatype
	Datatype *tmpdt = new Datatype;
	tmpdt->rid = dataRid;
	get_key_data(key, tmpdt, (KeyDataEntry *)rec, recLen, (nodetype)type);
	// unpack the dataRid
	dataRid = tmpdt->rid;

	status = MINIBASE_BM->unpinPage(spPageNo, true);
	if(status != OK)
		return MINIBASE_CHAIN_ERROR(BUFMGR, status);

	return OK;
}

Status BTLeafPage::get_next_sp (RID& rid,
                             void *key,
                             RID & dataRid,
			     PageId spPageNo)
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
                        dataRid.pageNo = INVALID_PAGE;
                        dataRid.slotNo = INVALID_SLOT;
                        return NOMORERECS;
                }
                else
                        return MINIBASE_FIRST_ERROR(BTLEAFPAGE, GETRECERROR);
        }
	
	 // find the record page by calling the HFPage function;
        char *rec = NULL;
        int recLen = 0;
        status = ((HFPage*)currPage)->returnRecord(nextRid, rec, recLen);
        if(status != OK)
                return MINIBASE_FIRST_ERROR(BTLEAFPAGE, GETRECERROR);
	
	// get the first data key pair
	// pack the dataRid into Datatype
	Datatype *tmpdt = new Datatype;
	tmpdt->rid = dataRid;
	tmpdt->pageNo = INVALID_PAGE;
	get_key_data(key, tmpdt, (KeyDataEntry *)rec, recLen, (nodetype)type);
	// unpack the dataRid
	dataRid = tmpdt->rid;

	rid = nextRid;

	status = MINIBASE_BM->unpinPage(spPageNo, true);
	if(status != OK)
		return MINIBASE_CHAIN_ERROR(BUFMGR, status);

	return OK;
}
