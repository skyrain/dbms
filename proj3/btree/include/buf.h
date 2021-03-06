///////////////////////////////////////////////////////////////////////////////
/////////////  The Header File for the Buffer Manager /////////////////////////
///////////////////////////////////////////////////////////////////////////////


#ifndef BUF_H
#define BUF_H

#include "db.h"
#include "page.h"
#include "new_error.h"

#define NUMBUF 1000
// Default number of frames, artifically small number for ease of debugging.

#define HTSIZE 7
// Hash Table size

#define HTA 1
#define HTB 1

//---- Buffer descriptor ---
typedef struct BufDescr
{
	PageId pageId;
	int pinCount;
	bool dirtyBit;
}BufDescr;

//--- hashtable bucket ----
typedef struct Bucket
{
	PageId pageId;
	int frameId;
	struct Bucket* next;
}Bucket;

//--- hash table ----------
typedef struct HashTable
{
	Bucket* directory[HTSIZE];	
}HashTable;

//--- replacement policy ---
typedef struct ReplaceList
{
	int frameId;
	bool hate;
	struct ReplaceList* next;
}ReplaceList;

/*******************ALL BELOW are purely local to buffer Manager********/

// You could add more enums for internal errors in the buffer manager.
enum bufErrCodes  {HASHMEMORY, HASHDUPLICATEINSERT, HASHREMOVEERROR, HASHNOTFOUND, QMEMORYERROR, QEMPTY, INTERNALERROR, 
	BUFFERFULL, BUFMGRMEMORYERROR, BUFFERPAGENOTFOUND, BUFFERPAGENOTPINNED, BUFFERPAGEPINNED};

class Replacer; // may not be necessary as described below in the constructor

class BufMgr {

	private: 
		unsigned int    numBuffers;
//		BufDescr bufDescr[NUMBUF];	
		HashTable *hashTable;
		ReplaceList* LRU;
		ReplaceList* MRU;
		
		//--- hash calculation ----------------
		//--- calculate the bucket should be --
		//--- according to pageId -------------
		int hash(PageId pageId);

		//--- delete hash bucket ---------------
		Status hashRemove(PageId pageId);

		//--- insert hash bucket ---------------
		Status hashPut(PageId pageId, int frameId);

		//--- get frame id ----------------------
		//--- input: pageId, frameId(arbitary value,---
		//--- changed to valid value after execution)--
		Status hashGetFrameId(PageId pageId, int& frameId);
	public:
BufDescr bufDescr[NUMBUF];// for test	
		Page* bufPool; // The actual buffer pool
		
		//--- initialize: 1. bufDescr[]; 2.hashtable; ---
		BufMgr (int numbuf, Replacer *replacer = 0); 
		// Initializes a buffer manager managing "numbuf" buffers.
		// Disregard the "replacer" parameter for now. In the full 
		// implementation of minibase, it is a pointer to an object
		// representing one of several buffer pool replacement schemes.


		~BufMgr();           // Flush all valid dirty pages to disk

		Status pinPage(PageId PageId_in_a_DB, Page*& page, int emptyPage);
		// Check if this page is in buffer pool, otherwise
		// find a frame for this page, read in and pin it.
		// also write out the old page if it's dirty before reading
		// if emptyPage==TRUE, then actually no read is done to bring
		// the page

		Status unpinPage(PageId globalPageId_in_a_DB, int dirty, int hate);
		// hate should be TRUE if the page is hated and FALSE otherwise
		// if pincount>0, decrement it and if it becomes zero,
		// put it in a group of replacement candidates.
		// if pincount=0 before this call, return error.

		Status newPage(PageId& firstPageId, Page*& firstpage, int howmany=1); 
		// call DB object to allocate a run of new pages and 
		// find a frame in the buffer pool for the first page
		// and pin it. If buffer is full, ask DB to deallocate 
		// all these pages and return error

		Status freePage(PageId globalPageId); 
		// User should call this method if it needs to delete a page
		// this routine will call DB to deallocate the page 

		Status flushPage(PageId pageid);
		// Used to flush a particular page of the buffer pool to disk
		// Should call the write_page method of the DB class

		Status flushAllPages();
		// Flush all pages of the buffer pool to disk, as per flushPage.

		/*** Methods for compatibility with project 1 ***/
		Status pinPage(PageId PageId_in_a_DB, Page*& page, int emptyPage=0, const char *filename=NULL);
		// Should be equivalent to the above pinPage()
		// Necessary for backward compatibility with project 1

		Status unpinPage(PageId globalPageId_in_a_DB, int dirty=FALSE, const char *filename=NULL);
		// Should be equivalent to the above unpinPage()
		// Necessary for backward compatibility with project 1

		unsigned int getNumBuffers() const { return numBuffers; }
		// Get number of buffers

		unsigned int getNumUnpinnedBuffers();
		// Get number of unpinned buffers

		//----- replacement policy ---------------------------
		
		//--- find the node previous location in MRU or LRU list-
		//--- if is a new node return NULL ----------------------
		//--- input node's next should be NULL -------------------
		ReplaceList* findList(ReplaceList* node);
		
		//---- every time unPin, add new node to LRU or MRU list ---
		//---- or modify the node along with its new access time & -
		//----- & its love/hate value(love conquers hate)        ---
		//--- input node's next should be NULL -------------------
		Status addReplaceList(ReplaceList* node);

		//--- replace ----
		//--- input: frameId(arbitary value, changed after execution)--
		Status replace(int& frameId);
};

#endif
