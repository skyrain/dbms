/*
 * sorted_page.C - implementation of class SortedPage
 *
 * Johannes Gehrke & Gideon Glass  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */
#include <stdlib.h>
#include <string.h>
#include "sorted_page.h"
#include "btindex_page.h"
#include "btleaf_page.h"

const char* SortedPage::Errors[SortedPage::NR_ERRORS] = {
	"OK",
	"Insert Record Failed (SortedPage::insertRecord)",
	"Delete Record Failed (SortedPage::deleteRecord)",
	"NOT attrString or attrInteger",
};

static error_string_table hfTable( SORTEDPAGE, SortedPage::Errors );


/*
 *  Status SortedPage::insertRecord(AttrType key_type, 
 *                                  char *recPtr,
 *                                    int recLen, RID& rid)
 *
 * Performs a sorted insertion of a record on an record page. The records are
 * sorted in increasing key order.
 * Only the  slot  directory is  rearranged.  The  data records remain in
 * the same positions on the  page.
 *  Parameters:
 *    o key is a void * pointer to a key.
 * 
 *    o key_type - the type of the key.
 *    o recPtr points to the actual record that will be placed on the page
 *            (So, recPtr is the combination of the key and the other data
 *       value(s)).
 *    o recLen is the length of the record to be inserted.
 *    o rid is the record id of the record inserted.
 */

//---- insertRecord method will hide the base method ---
Status SortedPage::insertRecord (AttrType key_type,
		char * recPtr,
		int recLen,
		RID& rid)
{
	// put your code here
	//--- check key_type ---
	if(key_type != attrString && key_type != attrInteger)
		return MINIBASE_FIRST_ERROR(SORTEDPAGE, KEY_TYPE_ERROR);

	//---1. call base insertRecord func to insert record ---
	Status status = HFPage::insertRecord(recPtr, recLen, rid);

	//--- check whether insert is good ---
	if(status != OK)
	{
		if(status == DONE)
			return DONE;
		else
			return MINIBASE_RESULTING_ERROR(SORTEDPAGE, status, INSERT_REC_FAILED);
	}	

	//---2. rearrange the slot dir ---

	//--- 2.1 ?? get the key value ---
	//--- accoridng to key_type to retrieve key value ---
	Keytype key;
	bool isStrKey = true;
	if(key_type == attrInteger)
	{
		int* keyMem = (int *)calloc(1, sizeof(int));
		memcpy(keyMem, recPtr, sizeof(int));
		
		key.intkey = *keyMem;
		free(keyMem);
		isStrKey = false;
	}
	else if(key_type == attrString)
	{
		strcpy(key.charkey, recPtr);
	}

	//--- 2.2 rearrange slot dir including the new slot ---
	//--- & change rid value ---

	//--- check new key from the first slot to the last slot, if --
	//--- new key be after &  smaller than the walker slot, swap & update ---
	//--- the new key as the walker slot value. Then check next walker slot---
	//--- if during this process, swap does happen, after walk to new key ---,    //--- slot stop check --
	//--- else  check from last slot to the new key slot  ---
	//--- if new key be before & bigger than the walekr slot, swap &update ---
	//--- the new key as the walker slot value ---
	bool swapFront = false;
	int newSlotNo = rid.slotNo;
	int j = this->slotCnt - 1;
	for(int i = 0; i < this->slotCnt; i++)
	{
		//--- check reach behind & swap happen in the front slot ---
		if( i >= newSlotNo && swapFront)
			break;

		if(i < newSlotNo)
		{
			//--- check if slot is empty ---
			if(slot[i].offset == INVALID_SLOT || slot[i].length == EMPTY_SLOT)
				continue;

			//--- retrieve walker slot's key ---
			char* tmpData = (char*)calloc(1, sizeof(char) * slot[i].length);
			memcpy(tmpData, &data[slot[i].offset], sizeof(char) * slot[i].length);
			if(isStrKey)
			{
				//--- extract key from data ---
				char* tmpKey = (char* )calloc(1, sizeof(char) * MAX_KEY_SIZE1);
				strcpy(tmpKey, tmpData);
				free(tmpData);
				//--- compare walker key and new key ---
				//--- check if new key smaller than walker key ---
				if(strcmp(key.charkey, tmpKey) < 0)
				{
					//--- first time swap: swap the new slot with others ---
					//--- since rid is the new slot's rid, need update it --
					if(!swapFront)
					{
						//--- update rid ---
						rid.slotNo = i;
					}
					swapFront = true;
					//--- swap slot ---	
					slot_t* tmpSlot = (slot_t *)calloc(1, sizeof(slot_t));
					memcpy(tmpSlot, &slot[i], sizeof(slot_t));
					memcpy(&slot[i], &slot[newSlotNo], sizeof(slot_t));
					memcpy(&slot[newSlotNo], tmpSlot, sizeof(slot_t));
					free(tmpSlot);
					//--- update slot[newSlotNo]'s key ---
					memset(key.charkey, 0, sizeof(char) * MAX_KEY_SIZE1);
					strcpy(key.charkey, tmpKey);
					free(tmpKey);
				}
			}
			else
			{
				//--- extract key from data ---
				int* keyMem = (int*)calloc(1, sizeof(int));
				memcpy(keyMem, tmpData, sizeof(int));

				int tmpKey = *keyMem;
				free(keyMem);
				free(tmpData);
				//--- compare walker key and new key ---
				//--- check if new key smaller than walker key ---
				if(key.intkey < tmpKey)
				{
					//--- first time swap: swap the new slot with others ---
					//--- since rid is the new slot's rid, need update it --
					if(!swapFront)
					{
						//--- update rid ---
						rid.slotNo = i;
					}
					swapFront = true;
					//--- swap slot ---	
					slot_t* tmpSlot = (slot_t* )calloc(1, sizeof(slot_t));
					memcpy(tmpSlot, &slot[i], sizeof(slot_t));
					memcpy(&slot[i], &slot[newSlotNo], sizeof(slot_t));
					memcpy(&slot[newSlotNo], tmpSlot, sizeof(slot_t));
					free(tmpSlot);
					//--- update slot[newSlotNo]'s key ---
					key.intkey = tmpKey;
				}
			}
		}
		else if(i > newSlotNo)
		{
			//--- check if slot is empty ---
			if(slot[j].offset == INVALID_SLOT || slot[j].length == EMPTY_SLOT)
				continue;

			//--- retrieve walker slot's key ---
			char* tmpData = (char* )calloc(1, sizeof(char) * slot[j].length);
			memcpy(tmpData, &data[slot[j].offset], sizeof(char) * slot[j].length);
			if(isStrKey)
			{
				//--- extract key from data ---
				char* tmpKey = (char* )calloc(1, sizeof(char) * MAX_KEY_SIZE1);
				strcpy(tmpKey, tmpData);
				free(tmpData);
				//--- compare walker key and new key ---
				//--- check if new key bigger than walker key ---
				if(strcmp(key.charkey, tmpKey) > 0)
				{
					//--- first time swap: swap the new slot with others ---
					//--- since rid is the new slot's rid, need update it --
					if(!swapFront)
					{
						//--- update rid ---
						rid.slotNo = j;
					}
					swapFront = true;
					//--- swap slot ---	
					slot_t* tmpSlot = (slot_t *)calloc(1, sizeof(slot_t));
					memcpy(tmpSlot, &slot[j], sizeof(slot_t));
					memcpy(&slot[j], &slot[newSlotNo], sizeof(slot_t));
					memcpy(&slot[newSlotNo], tmpSlot, sizeof(slot_t));
					free(tmpSlot);
					//--- update slot[newSlotNo]'s key ---
					memset(key.charkey, 0, sizeof(char) * MAX_KEY_SIZE1);
					strcpy(key.charkey, tmpKey);
					free(tmpKey);
				}
			}
			else
			{
				//--- extract key from data ---
				int* keyMem = (int*)calloc(1, sizeof(int));
				memcpy(keyMem, tmpData, sizeof(int));

				int tmpKey = (int)*keyMem;
				free(keyMem);
				free(tmpData);
				//--- compare walker key and new key ---
				//--- check if new key smaller than walker key ---
				if(key.intkey > tmpKey)
				{
					//--- first time swap: swap the new slot with others ---
					//--- since rid is the new slot's rid, need update it --
					if(!swapFront)
					{
						//--- update rid ---
						rid.slotNo = j;
					}
					swapFront = true;
					//--- swap slot ---	
					slot_t* tmpSlot = (slot_t* )calloc(1, sizeof(slot_t));
					memcpy(tmpSlot, &slot[j], sizeof(slot_t));
					memcpy(&slot[j], &slot[newSlotNo], sizeof(slot_t));
					memcpy(&slot[newSlotNo], tmpSlot, sizeof(slot_t));
					free(tmpSlot);
					//--- update slot[newSlotNo]'s key ---
					key.intkey = tmpKey;
				}
			}
			j--;

		}
	}

	return OK;
}


/*
 * Status SortedPage::deleteRecord (const RID& rid)
 *
 * Deletes a record from a sorted record page. It just calls
 * HFPage::deleteRecord().
 */

Status SortedPage::deleteRecord (const RID& rid)
{
	// put your code here
	Status status = HFPage::deleteRecord(rid);
	if(status != OK)
		return MINIBASE_RESULTING_ERROR(SORTEDPAGE, status, DELETE_REC_FAILED);

	return OK;
}

int SortedPage::numberOfRecords()
{
	// put your code here
    return	HFPage::getRecCnt();
}
