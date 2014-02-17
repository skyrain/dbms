#include <iostream>
#include <stdlib.h>
#include <memory.h>

#include "hfpage.h"
#include "heapfile.h"
#include "buf.h"
#include "db.h"

/*
// **********************************************
//---- assist methods definitions ---------------

//--- judge whehter already exist empty slot for new record ---
//--- return: -1 no empty slot, need to allocate new slot 
//--- return: non-position value - empty slot no 
int getEmptySlotNo();
void modifySlot(int slotNo, int recLen);
void addData(int slotNo, char* recPtr, int recLen);
int getRecordOffset(RID rid);
//--- clean slot and return the length of the record ----
int cleanSlot(RID rid);
//--- shrink slot directory ---
void shrinkSlotDir();
//--- delete record, relocate records beind the deleted record---
//--- update slot dir & update usedPtr, freeSpace ---
void deleteRec(int offset, int length);
//--- relocate records behind the records ---
void relocateRecord(int offset, int length);
//-- find the slot coresponding to back record ---
//-- input: record offset ---
HFPage::slot_t findBackRec(int offset);
// ***********************************************
*/
// **********************************************************
// page class constructor

void HFPage::init(PageId pageNo)
{
	// fill in the body
	this->slotCnt = 0;
	//--- points to the end for future record ----
	//--- and starts from 1 not 0 ---
	this->usedPtr = MAX_SPACE - DPFIXED;
	this->freeSpace = MAX_SPACE - DPFIXED;
	this->type = -1;
	this->prevPage = INVALID_PAGE;
	this->nextPage = INVALID_PAGE;
	this->curPage = pageNo;
	this->slot[0].offset = INVALID_SLOT;
	this->slot[0].length = EMPTY_SLOT;
}

// **********************************************************
// dump page utlity
void HFPage::dumpPage()
{
	int i;

	cout << "dumpPage, this: " << this << endl;
	cout << "curPage= " << curPage << ", nextPage=" << nextPage << endl;
	cout << "usedPtr=" << usedPtr << ",  freeSpace=" << freeSpace
		<< ", slotCnt=" << slotCnt << endl;

	for (i=0; i < slotCnt; i++) {
		cout << "slot["<< i <<"].offset=" << slot[i].offset
			<< ", slot["<< i << "].length=" << slot[i].length << endl;
	}
}

// **********************************************************
PageId HFPage::getPrevPage()
{
	// fill in the body
	return this->prevPage;
//	return 0;
}

// **********************************************************
void HFPage::setPrevPage(PageId pageNo)
{

	// fill in the body
	this->prevPage = pageNo;
}

// **********************************************************
PageId HFPage::getNextPage()
{
	// fill in the body
	return this->nextPage;
//	return 0;
}

// **********************************************************
void HFPage::setNextPage(PageId pageNo)
{
	// fill in the body
	this->nextPage = pageNo;
}

// **********************************************************
// Add a new record to the page. Returns OK if everything went OK
// otherwise, returns DONE if sufficient space does not exist
// RID of the new record is returned via rid parameter.
Status HFPage::insertRecord(char* recPtr, int recLen, RID& rid)
{
	// fill in the body
	//--- check if does not have enough free Space ----
	//--- get empty slot no ---
	int emptySlotNo = getEmptySlotNo();

	if((int)this->freeSpace < recLen + (int)sizeof(slot_t) && emptySlotNo == -1)
	{
		return DONE;	
	}
	else if((int)this->freeSpace < recLen && emptySlotNo != -1)
	{
		return DONE;
	}
	else
	{
		//--- check if is the first record on page---
		if(this->slotCnt == 0)
		{
			//--- modify RID value ----
			rid.pageNo = this->curPage;
			rid.slotNo = this->slotCnt;
			//--- add new slot ----
			modifySlot(0, recLen);
			//--- add the record data into page ----
			addData(0, recPtr, recLen);
			//--- update HFpage info ---
			this->slotCnt ++;
		}
		else
		{
			if(emptySlotNo == -1)
			{
				emptySlotNo = this->slotCnt;
				this->slotCnt ++;
			}
			//--- modify RID value ---
			rid.pageNo = this->curPage;
			rid.slotNo = emptySlotNo;
			//--- add new slot ----
			modifySlot(emptySlotNo, recLen);
			//--- add the record data into page ---
			addData(emptySlotNo, recPtr, recLen);
		}
		//--- update HFpage info ---
		this->usedPtr = this->usedPtr - recLen;
		this->freeSpace = this->freeSpace - recLen;
		return OK;
	}
}

// **********************************************************
// Delete a record from a page. Returns OK if everything went okay.
// Compacts remaining records but leaves a hole in the slot array.
// Use memmove() rather than memcpy() as space may overlap.
Status HFPage::deleteRecord(const RID& rid)
{
	// fill in the body
	//---?? 注意未处理delete不存在record得特殊情况----
	
	//--- get record offset ---
	int offset = getRecordOffset(rid);
	//--- clean corresponding slot & get record length---
	int len = cleanSlot(rid);
	//-- shrink slot directory ---
	shrinkSlotDir();
	//--- delete record & relocate behind records & slot dir ---
	//--- & update usedPtr, freeSpace -----
	deleteRec(offset, len);

	return OK;
}

// **********************************************************
// returns RID of first record on page
Status HFPage::firstRecord(RID& firstRid)
{
	// fill in the body
	//--- if no record ---
	if(this->slotCnt == 0)
		return DONE;
	
	//--- get the RID of first record ---
	if(this->slot[0].length != EMPTY_SLOT)
	{
		firstRid.slotNo = 0;
		firstRid.pageNo = this->curPage;
	}
	else
	{
		for(int i = 1; i < this->slotCnt; i++)
		{
			slot_t * tmpSlot = (slot_t *)&(this->data[(i - 1) * sizeof(slot_t)]);
			if(tmpSlot->length != EMPTY_SLOT)
			{
				firstRid.slotNo = i;
				firstRid.pageNo = this->curPage;
				break;
			}
		}
	}

	return OK;
}

// **********************************************************
// returns RID of next record on the page
// returns DONE if no more records exist on the page; otherwise OK
//--- ?? deal with error---
Status HFPage::nextRecord (RID curRid, RID& nextRid)
{
	// fill in the body
	bool noNext = true;
	for(int i = curRid.slotNo + 1; i < this->slotCnt; i++)
	{
		slot_t * tmpSlot = (slot_t *)&(this->data[(i - 1) * sizeof(slot_t)]);
		if(tmpSlot->length != EMPTY_SLOT)
		{
			nextRid.slotNo = i;
			nextRid.pageNo = this->curPage;
			noNext = false;
			break;
		}
	}
	if(noNext)
		return DONE;

	return OK;
}

// **********************************************************
// returns length and copies out record with RID rid
//--- deal with error ?? ---
Status HFPage::getRecord(RID rid, char* recPtr, int& recLen)
{
	// fill in the body
	if(rid.slotNo == 0)
	{
		//recPtr = (char *)calloc(1, this->slot[0].length);
		recLen = this->slot[0].length;
		memcpy(recPtr, &(this->data[this->slot[0].offset]), recLen);
	}
	else
	{
		slot_t * tmpSlot = (slot_t *)&(this->data[(rid.slotNo - 1) * sizeof(slot_t)]);
		//recPtr = (char *)calloc(1, tmpSlot->length);
		recLen = tmpSlot->length;
		memcpy(recPtr, &(this->data[tmpSlot->offset]), recLen);
	}
	return OK;
}

// **********************************************************
// returns length and pointer to record with RID rid.  The difference
// between this and getRecord is that getRecord copies out the record
// into recPtr, while this function returns a pointer to the record
// in recPtr.
//--- deal with error ?? ---
Status HFPage::returnRecord(RID rid, char*& recPtr, int& recLen)
{
	// fill in the body
	if(rid.slotNo == 0)
	{
		recPtr = &(this->data[this->slot[0].offset]);
		recLen = this->slot[0].length;
	}
	else
	{
		slot_t * tmpSlot = (slot_t *)&(this->data[(rid.slotNo - 1) * sizeof(slot_t)]);
		recPtr = &(this->data[tmpSlot->offset]);
		recLen = tmpSlot->length;
	}
	return OK;
}

// **********************************************************
// Returns the amount of available space on the heap file page
int HFPage::available_space(void)
{
	// fill in the body
	//--- check if all slots are full ---
	bool allSlotFull = true;
	if(this->slot[0].length == EMPTY_SLOT)
		allSlotFull = false;
	else
	{
		for(int i = 1; i < this->slotCnt; i++)
		{
			slot_t * tmpSlot = (slot_t *)&(this->data[(i - 1) * sizeof(slot_t)]);
			if(tmpSlot->length == EMPTY_SLOT)
			{
				allSlotFull = false;
				break;
			}
		}
	}

	if(allSlotFull)
		return this->freeSpace - sizeof(slot_t);

	return this->freeSpace;
}

// **********************************************************
// Returns 1 if the HFPage is empty, and 0 otherwise.
// It scans the slot directory looking for a non-empty slot.
bool HFPage::empty(void)
{
	// fill in the body
	if(this->slot[0].length != EMPTY_SLOT)
		return false;
	
	for(int i = 1; i < this->slotCnt; i++)
	{
		slot_t * tmpSlot = (slot_t *)&(this->data[(i - 1) * sizeof(slot_t)]);
		if(tmpSlot->length != EMPTY_SLOT)
			return false;
	}

	return true;
}


// ************** assist methods implementations ---------

int HFPage::getEmptySlotNo()
{
	//--- check if empty slot already exist ---
	int emptySlotNo = -1;
	if(this->slot[0].length == EMPTY_SLOT)
		emptySlotNo = 0;
	else
	{
		for(int i = 1; i < this->slotCnt; i++)
		{
			//--- cast correct?? ---
			slot_t *tmpSlot = (slot_t *)&(this->data[sizeof(slot_t) * (i - 1)]);
			if(tmpSlot->length == EMPTY_SLOT)
			{
				emptySlotNo = i;
				break;
			}
		}
	}
	return emptySlotNo;
}

void HFPage::modifySlot(int slotNo, int recLen)
{
	//--- add new slot ----
	slot_t newSlot;
	newSlot.offset = this->usedPtr - recLen + 1;
	newSlot.length = recLen;
	//--- insert new slot into HFpage ---
	if(slotNo == 0)
	{
		this->slot[0].offset = newSlot.offset;
		this->slot[0].length = newSlot.length;
	}
	else
	{
		memcpy(&(data[(slotNo - 1) * sizeof(slot_t)]),
				&newSlot, sizeof(slot_t));
	}
}


void HFPage::addData(int slotNo, char* recPtr, int recLen)
{
	if(slotNo == 0)
	{
		memcpy(&(this->data[this->slot[0].offset]), recPtr, recLen);
	}
	else
	{
		slot_t *tmpSlot = (slot_t *)&(data[(slotNo - 1) * sizeof(slot_t)]);
		memcpy(&(this->data[tmpSlot->offset]), recPtr, recLen);
	}
}

int HFPage::getRecordOffset(RID rid)
{
	int slotNo = rid.slotNo;
	if(slotNo == 0)
	{
		return this->slot[slotNo].offset;
	}
	else
	{
		slot_t *tmpSlot = (slot_t *)&(this->data[(slotNo - 1) * sizeof(slot_t)]);
		return (int)tmpSlot->offset;
	}
}

int HFPage::cleanSlot(RID rid)
{
	int recLen;
	//--- if rid.slotNo is in slot[0] ---
	if (rid.slotNo == 0)
	{
		recLen = this->slot[0].length;
		this->slot[0].length = EMPTY_SLOT;
		return recLen;
	}
	else
	{
		slot_t *tmpSlot = (slot_t *)&(this->data[(rid.slotNo - 1) * sizeof(slot_t)]);
		recLen = tmpSlot->length;
		tmpSlot->length = EMPTY_SLOT;
		return recLen;
	}
}

void HFPage::shrinkSlotDir()
{
	//--- check if only one slot ---
	if(this->slotCnt == 1)
	{
		if(this->slot[0].length == EMPTY_SLOT)
		{
			this->slot[0].offset = INVALID_SLOT;
			this->slotCnt--;
		}
	}
	else
	{
		slot_t *tmpSlot = (slot_t *)&(this->data[(this->slotCnt - 1) * sizeof(slot_t)]);
		if(tmpSlot->length == EMPTY_SLOT)
		{
			tmpSlot->offset = INVALID_SLOT;
			this->slotCnt--;
		}
	}
}

HFPage::slot_t HFPage::findBackRec(int offset)
{
	slot_t backRecSlot;
	backRecSlot.offset = INVALID_SLOT;
	backRecSlot.length = EMPTY_SLOT;

	if(this->slot[0].offset < offset)
	{
		 backRecSlot.offset = this->slot[0].offset;
		 backRecSlot.length = this->slot[0].length;
	}

	for(int i = 1; i < this->slotCnt; i++)
	{
		slot_t * tmpSlot = (slot_t *)&(this->data[(i - 1) * sizeof(slot_t)]);		
		if(tmpSlot->offset < offset && tmpSlot->offset > backRecSlot.offset)
		{
			backRecSlot.offset = tmpSlot->offset;
			backRecSlot.length = tmpSlot->length;
		}
	}
	return backRecSlot;
}

void HFPage::updateMovingSlot(slot_t mSlot, int offset)
{
	//--- identify the slot in HFpage corresponding to mSlot ----
	//--- then update the offset of it ---
	if(this->slot[0].offset == mSlot.offset)
		this->slot[0].offset = offset; 
	else
	{
		for(int i = 1; i < this->slotCnt; i++)
		{
			slot_t * tmpSlot = (slot_t *)&(data[(i - 1) * sizeof(slot_t)]);
			if(tmpSlot->offset == mSlot.offset)
			{
				tmpSlot->offset = offset;
				break;
			}
		}
	}
}

void HFPage::relocateRec(int offset, int length)
{		
	while(offset != this->usedPtr + 1)
	{
		//--- allocate the nearest back record's slot ---
		slot_t backSlot = findBackRec(offset);
		//--- move the nearest back record forward ----
		int dest = backSlot.offset + length;
		memmove(&(this->data[dest]), &(this->data[backSlot.offset]), backSlot.length);
		//--- update offset of the moving slot ---
		updateMovingSlot(backSlot, dest);
		
		//--- update loop condition ---
		offset = backSlot.offset;
	}
}


void HFPage::deleteRec(int offset, int length)
{
	//--- delete record data ---
	memset(&(this->data[offset]), 0, length);
	//--- move records behind the deleted record forward ---
	//--- & update the slot directory with their new offset value ---
	relocateRec(offset, length);
	//update usedPtr, freeSpace ---
	this->usedPtr = this->usedPtr + length;
	this->freeSpace = this->freeSpace + length;
}
