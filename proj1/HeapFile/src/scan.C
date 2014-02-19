#include <stdio.h>   
#include <stdlib.h>   
   
#include "heapfile.h"   
#include "scan.h"   
#include "hfpage.h"   
#include "buf.h"   
#include "db.h"   

Scan::Scan (HeapFile *hf, Status& status)   
{  
    // init do all the constructor work 
    status = init(hf);   
    if(status != OK)
        cout << "init fails" << endl;
}   
   
Scan::~Scan()   
{   
    // Reset everything and upin all pages
    Status status;
    status = reset();
    if(status != OK)
	cout << "reset fails" << endl;
}
   
Status Scan::getNext(RID& rid, char *recPtr, int& recLen)   
{   
    // No record in the current page, scan the next data page.
    if(nxtUserStatus != OK)   
        nextDataPage();   
    // no datapage in heapfile, return DONE.
    if(dataPage == NULL)   
        return DONE;   
    
    Status status;
    rid = userRid;        
    status  = dataPage->getRecord(rid, recPtr, recLen);   
    if(status != OK)   
        return  MINIBASE_CHAIN_ERROR(HEAPFILE, status);   
    nxtUserStatus = dataPage->nextRecord(rid, userRid);   
   
    return status;   
}

Status Scan::init(HeapFile *hf)   
{   
    // return the first page to the scanner
    _hf = hf;   
    return firstDataPage();   
}   
   
Status Scan::reset()   
{   
    if (dataPage != NULL)  
        MINIBASE_BM->unpinPage(dataPageId);   
      
    // set back the datapage and ID into unused state;
    dataPage = NULL;   
    dataPageId = 0;
    nxtUserStatus = OK;   
    return OK;   
}   
   
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
   
Status Scan::nextDataPage()   
{   
    // make sure the file is not empty.  
    if(dataPage == NULL && dataPageId == INVALID_PAGE)      
        return DONE;
		
    Status status;
    if(dataPage == NULL){
	// get the first record in the data page file and the next page ID
	MINIBASE_BM->pinPage(dataPageId, (Page *&) dataPage);     
	// make sure if the new page has the record
	status = dataPage->firstRecord(userRid);   
	if(status != DONE)
	    return OK;
    }
      
    PageId nextPageId = INVALID_PAGE; 
    nextPageId = dataPage->getNextPage();   
    MINIBASE_BM->unpinPage(dataPageId);   
    dataPage = NULL;   
    dataPageId = nextPageId;   
    if (dataPageId == INVALID_PAGE)   
        return DONE;   
    
    MINIBASE_BM->pinPage(dataPageId, (Page *&) dataPage);         
    nxtUserStatus = dataPage->firstRecord(userRid);    
    if(nxtUserStatus != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

    return OK;     
}   
   
   
