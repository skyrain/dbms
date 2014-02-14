#include <iostream>
#include <stdlib.h>
#include <memory.h>

#include "hfpage.h"
#include "heapfile.h"
#include "buf.h"
#include "db.h"

// **********************************************
//---- assist methods definitions ---------------

//--- judge whehter already exist empty slot for new record ---
//--- return: -1 no empty slot, need to allocate new slot 
//--- return: non-position value - empty slot no 
int getEmptySlotNo();
void modifySlot(int slotNo, int recLen);
void addData(int slotNo, char* recPtr, int recLen);
// ***********************************************

// **********************************************************
// page class constructor

void HFPage::init(PageId pageNo)
{
	// fill in the body
	this->slotCnt = 0;
	//--- ?? points to the end for future record ----
	this->usedPtr = MAX_SPACE - DPFIXED - 1;
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
	if(this->freeSpace < recLen + sizeof(slot_t))
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
			//--- modify exist slot ----
			this->slot[0].offset = this->usedPtr - recLen + 1;
			this->slot[0].length = recLen;
			//--- add the record data into page ----
			memcpy(this->data[this->slot[0].offset], recPtr, recLen);
			//--- update HFpage info ---
			this->slotCnt ++;
			this->usedPtr = this->usedPtr - recLen;
			this->freeSpace = this->freeSpace - recLen;
		}
		else
		{
			//--- get empty slot no ---
			int emptySlotNo = getEmptySlotNo();
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
			//--- update HFpage info ---
			this->usedPtr = this->usedPtr - recLen;
			this->freeSpace = this->freeSpace - recLen;
		}
	}
	return OK;
}

// **********************************************************
// Delete a record from a page. Returns OK if everything went okay.
// Compacts remaining records but leaves a hole in the slot array.
// Use memmove() rather than memcpy() as space may overlap.
Status HFPage::deleteRecord(const RID& rid)
{
	// fill in the body
	return OK;
}

// **********************************************************
// returns RID of first record on page
Status HFPage::firstRecord(RID& firstRid)
{
	// fill in the body
	return OK;
}

// **********************************************************
// returns RID of next record on the page
// returns DONE if no more records exist on the page; otherwise OK
Status HFPage::nextRecord (RID curRid, RID& nextRid)
{
	// fill in the body

	return OK;
}

// **********************************************************
// returns length and copies out record with RID rid
Status HFPage::getRecord(RID rid, char* recPtr, int& recLen)
{
	// fill in the body
	return OK;
}

// **********************************************************
// returns length and pointer to record with RID rid.  The difference
// between this and getRecord is that getRecord copies out the record
// into recPtr, while this function returns a pointer to the record
// in recPtr.
Status HFPage::returnRecord(RID rid, char*& recPtr, int& recLen)
{
	// fill in the body
	return OK;
}

// **********************************************************
// Returns the amount of available space on the heap file page
int HFPage::available_space(void)
{
	// fill in the body
	return 0;
}

// **********************************************************
// Returns 1 if the HFPage is empty, and 0 otherwise.
// It scans the slot directory looking for a non-empty slot.
bool HFPage::empty(void)
{
	// fill in the body
	return true;
}


// ************** assist methods implementations ---------

int getEmptySlotNo()
{
	//--- check if empty slot already exist ---
	int emptySlotNo = -1;
	if(this->slot[0].length == EMPTY_SLOT)
		emptySlotNo = 0;
	else
	{
		for(int i = 1; i < slotCnt; i++)
		{
			//--- cast correct?? ---
			slot_t *tmpSlot = (slot_t *)this->data[sizeof(slot_t) * (i - 1)];
			if(tmpSlot->length == EMPTY_SLOT)
			{
				emptySlotNo = i;
				break;
			}
		}
	}
	return emptySlotNo;
}

void modifySlot(int slotNo, int recLen)
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
		memcpy(data[(slotNo - 1) * sizeof(slot_t)],
				newSlot, sizeof(slot_t));
	}
}


void addData(int slotNo, char* recPtr, int recLen)
{
	if(slotNo == 0)
	{
		memcpy(this->data[slot[0].offset], recPtr, recLen);
	}
	else
	{
		int offset;
		slot_t *tmpSlot = (slot_t *)data[(slotNo - 1) * sizeof(slot_t)];
		memcpy(this->data[tmpSlot->offset], recPtr, recLen);
	}
}
