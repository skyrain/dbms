/*
scan.C:
Sequencial scan on the heapfile page.

- Overall description of your algorithms and data structures
Scan(HeapFile *hf, Status& status): Constructor, it will call the init() to init a heapfile
~Scan() : Deconstructor, every time when we finish with the scan, we will call the reset() to destory the heapfile
Status Scan::getNext(RID& rid, char *recPtr, int& recLen): Get the next record if there is record in that page, or get next page if no record in the current page.
Status Scan::init(HeapFile *hf): get the heapfile name and first data page of this file
Status Scan::reset(): set all the parameters in the scanner to unused state.
Status Scan::firstDataPage(): get the first dataPageId from the heapfile meta data, return OK if success, returon error code if not.
Status Scan::nextDataPage(): get the next data page, return OK if success, return DONE if there isn't any data page

- Anything unusual in your implementation
In inplementation of Scanner, there are several possible return value for the nextDataPage() function, if any of these
situations are not handled properly, the scan can not process. Because it's sequencial scan, only one doubly linked list is faster than the directory structure.

- What functionalities not supported well
All functionalities are fully supported.

*/

#include <stdio.h>   
#include <stdlib.h>   
   
#include "heapfile.h"   
#include "scan.h"   
#include "hfpage.h"   
#include "buf.h"   
#include "db.h"   

// *******************************************
// The constructor pins the first page in the file
// and initializes its private data members from the private data members from hf
Scan::Scan(HeapFile *hf, Status& status)   
{  
    // init do all the constructor work 
    status = init(hf);   
    if(status != OK)
        cout << "init fails" << endl;
}   

// *******************************************
// The deconstructor unpin all pages.
Scan::~Scan()   
{   
    // Reset everything and upin all pages
    Status status;
    status = reset();
    if(status != OK)
	cout << "reset fails" << endl;
}

// *******************************************
// Retrieve the next record in a sequential scan.
// Also returns the RID of the retrieved record.   
Status Scan::getNext(RID& rid, char *recPtr, int& recLen)   
{   
    if(nxtUserStatus != OK || dataPage == NULL){
        // No record in the current page, scan the next data page.
        if(nxtUserStatus != OK)   
            nextDataPage();   
        // no datapage in heapfile, return DONE.
        if(dataPage == NULL)   
            return DONE;   
    }

    // get the userRid from the current page, and get the data record by this rid. 
    // then set the nxtUserStatus to represent whether there is record in the current page.
    Status status;
    rid = userRid;        
    status  = dataPage->getRecord(rid, recPtr, recLen);   
    if(status != OK)   
        return  MINIBASE_CHAIN_ERROR(HEAPFILE, status);   
    nxtUserStatus = dataPage->nextRecord(rid, userRid);   
    
    return status;   
}

// *******************************************
// Do all the constructor work.
Status Scan::init(HeapFile *hf)   
{   
    // return the first page to the scanner
    _hf = hf;   
    return firstDataPage();   
}   

// *******************************************
// Reset everything and unpin all pages.   
Status Scan::reset()   
{   
    // if there is data page in the memory, them free it.
    if (dataPage != NULL)  
        MINIBASE_BM->unpinPage(dataPageId);   
      
    // set back the datapage and ID into unused state;
    dataPage = NULL;   
    dataPageId = 0;
    nxtUserStatus = OK;   
    return OK;   
}   

// *******************************************
// Copy data about first page in the file.   
Status Scan::firstDataPage()   
{   
    dataPage = NULL;   
    // Point the heapfile we are using to the hearder page at first.   
    dataPageId = _hf->firstDirPageId;  
    //assume there is data record in the first data page
    nxtUserStatus = OK;   
    Status status;
    status = nextDataPage();   
    
    // keep on reporting the error if the nextDataPage() return DONE; extually it happens.
    if(status != OK && status != DONE)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);   

    return OK;   
}   

// *******************************************
// Retrieve the next data page.   
Status Scan::nextDataPage()   
{   
    // make sure the file is not empty.  
    if(dataPage == NULL && dataPageId == INVALID_PAGE)      
        return DONE;
		
    Status status;
    if(dataPage == NULL){
    	RID rid;
	// get the first record in the data page file and the next page ID
	MINIBASE_BM->pinPage(dataPageId, (Page *&)dataPage);     
	// make sure if the new page has the record
	status = dataPage->firstRecord(rid);
	userRid = rid;
	if(status != DONE)
	    return OK;
    }
    
    // if neither the data page and page ID is empty, get the next page ID from the current page. 
    PageId nextPageId = INVALID_PAGE; 
    nextPageId = dataPage->getNextPage();   
    status = MINIBASE_BM->unpinPage(dataPageId);   
    if(status != OK)
	return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
    dataPage = NULL;   
    dataPageId = nextPageId;   
    // get the first record if the page is not a null page
    if (dataPageId == INVALID_PAGE)   
        return DONE;   
    // if there is next page, get the first record of the next page
    else{
    	RID rid;
        MINIBASE_BM->pinPage(dataPageId, (Page *&) dataPage);         
        nxtUserStatus = dataPage->firstRecord(rid);
        userRid = rid;
        if(nxtUserStatus != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
    }

    return OK;     
}   
   
   
