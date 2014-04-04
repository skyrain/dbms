/*
 * btleaf_page.C - implementation of class BTLeafPage
 *
 * Johannes Gehrke & Gideon Glass  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
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
	int entryLen;
	Datatype datatype;
	datatype.rid = dataRid;	
	// Call Key::make_entry for this data record
	make_entry(&target, key_type, key, LEAF, datatype, &entryLen);	

	Status status;
	// Call SortedPage::insertRecord() to accomplish the insert.
	status = SortedPage::insertRecord(key_type, (char*)&target, entryLen, rid);
	if(status != OK)
		return MINIBASE_FIRST_ERROR(BTLEAFPAGE,LEAFINSERTFAIL);
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
	int Eq = 0;
	
	// define the low and high
	low = 0;
	high = slotCnt - 1;
	
	// binart search routine
	while(low <= high)
	{
		med = (low + high)/2;
		// find the result key by calling keyCompare
		// find the med of key by checking the slot. ??? tianyu
		void * keyc;
		slot_t * tmpSlot = (slot_t *)&(data[(med - 1) * sizeof(slot_t)]);
		keyc = (void *)(data + tmpSlot->offset);
		Eq = keyCompare(key, keyc, key_type);
		// find if the key is the desired one.
		if(Eq == 0){
			Keytype ktype;

			// if find the data, then call get_key_data
			get_key_data((void *)&ktype, (Datatype *)&dataRid,
		 	(KeyDataEntry*)(data + tmpSlot->offset), tmpSlot->length, LEAF); 
		
			// if find the key, return.
			// if not, check to find which side to go
			return OK;
		}else if(Eq < 0)
			low = med + 1;
		else
			high = med + 1;
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
	get_key_data(key, (Datatype *)&dataRid, (KeyDataEntry *)rec, recLen, LEAF);

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
	
	// get the next data key pair.
	get_key_data(key, (Datatype *)&dataRid, (KeyDataEntry *)rec, recLen, LEAF);
	
	rid = nextRid;
	return OK;
}
