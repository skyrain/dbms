/*
 * implementation of class Scan for HeapFile project.
 * $Id: scan.C,v 1.1 1997/01/02 12:46:42 flisakow Exp $
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
    // ini do all the constructor work
    status = init(hf); 
    if(status != OK)
        cout << "init fails" << endl;
}   
   
// *******************************************
// The deconstructor unpin all pages.
Scan::~Scan()   
{   
    // Reset everything and unpin all pages.
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
    // determine if there still have page or record in page
    if (nxtUserStatus != OK)   
        nextDataPage();   
    if (dataPage == NULL)   
        return DONE;   
    
    // retrieve the record in the same page
    rid = userRid;        
    dataPage->getRecord(rid, recPtr, recLen);   
    nxtUserStatus = dataPage->nextRecord(rid, userRid);   
   
    return OK;   
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
    if(dataPage != NULL)   
    MINIBASE_BM->unpinPage(dataPageId);   
    
    // set back the datapage and ID into unused state.
    dataPage = NULL; 
    dataPageId = 0;    
    nxtUserStatus = OK;   
    return OK;	
}   
   
// *******************************************
// Copy data about first page in the file.
Status Scan::firstDataPage()   
{      
    Status status;
    dataPage = NULL;
    // Point the heapfile we are using to the hearder page at first.
    dataPageId = _hf->firstDirPageId;      
    // get the first data page
    status = nextDataPage(); 
	
    if(status != OK)
	cout << " error when get data page" << endl;
	
    //assume there is data record in the first data page
    nxtUserStatus = OK;   
    return OK;   
}   
   
// *******************************************   
// Retrieve the next data page   
Status Scan::nextDataPage()   
{    
    // make sure the file is not empty.  
    if(dataPage == NULL && dataPageId == INVALID_PAGE)      
        return DONE;
		
    Status status;
    if(dataPageId == INVALID_PAGE){
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
     
    return OK;   
}   
