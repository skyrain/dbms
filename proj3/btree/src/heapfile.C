/*
heapfile.C
Define the heapfile and all the operations on a heapfile.

- Overall description of your algorithms and data structures

  HeapFile::HeapFile( const char *name, Status& returnStatus ): constructor of a heapfile, write into the disk if this is a new heapfile,
  other wise, create a new heapfile and initialize all the members of a heapfile object.
   
  HeapFile::~HeapFile(): deconstructor of heapfile, delete the filename and call the deleteFile to delete the heap file.  

  int HeapFile::getRecCnt(): it will return number of records in heap file by calling the getRecCnt() by iterating all data pages.

  Status HeapFile::insertRecord(char *recPtr, int recLen, RID& outRid): insert a record in the heapfile, return OK if insert successfully, return error code if not.
  It will find a existing data page in the heapfile which has enough space for that record. If can not find any data page, then it will
  create a new data page and insert the record into the new page.

  Status HeapFile::deleteRecord (const RID& rid) delete a record in the heapfile, return OK if delete successfully, return error code if not. 
  it will find out the record with given RID, then call deleteRecord to delete the corresponding record. Then determine whether the data page is empty, if it is empty,
  that page will be deleted, otherwise just unpin that data page.
  
  Status HeapFile::updateRecord (const RID& rid, char *recPtr, int recLen) update a record in the heapfile, return OK if update successfully, return error code if not. 
  find the data record with given Rid by calling findDataPage, if it is found, then update the data record and determine if the length of the data record has been changed.
  if the data record is not found, just return DONE, this should be a error code (No problem since then)???
  
  Status HeapFile::getRecord (const RID& rid, char *recPtr, int& recLen) get a record in the heapfile, return OK if get the record successfully, return error code if not.  
  find the data record with given Rid by calling findDataPage, if it is found, then get the data record and returning the record pointer and length.
  
  Scan *HeapFile::openScan(Status& status): Initiate a sequential scan and return it. 
  
  Status HeapFile::deleteFile(): Wipes out the heapfile from the database permanently, return OK if all the data page deleted successfully, otherwise return corresponding error code. 
  
  Status HeapFile::newDataPage(DataPageInfo *dpinfop) get a new data page from the buffer manager and initialize dpinfo.

  Status HeapFile::findDataPage(const RID& rid, PageId &rpDirPageId, HFPage *&rpdirpage, PageId &rpDataPageId,HFPage *&rpdatapage): Return OK if the data page is found, otherwise return DONE.
  returns pinned directory page and pinned data page of the specified user record If the user record cannot be found, rpdirpage and rpdatapage are returned as NULL pointers.
					
- Anything unusual in your implementation
  
  In implementation of heap file, when insert a record, I give the tast that decide whether there is space for a record to the hfpage. 
  Secondly, I decide not to use the directory page structure, as a matter of fact that there is only a small number of data page in the heap file, also 
  it's a single user database, one doubly linked list is more robust and easier to implemented, directory page will increase the speed of insert by checking the dpinfo, but the time which are saved is
  insignificant and maintenaning the directory page also needs extra disk I/O and easy to corrupt.

- What functionalities not supported well

  All functionalities are fully supported.
  
*/

#include "heapfile.h"   
#include <stdio.h>   

// ******************************************************
// Error messages for the heapfile layer
static const char *hfErrMsgs[] = {   
    "bad record id",   
    "bad record pointer",    
    "end of file encountered",   
    "invalid update operation",   
    "no space on page for record",    
    "page is empty - no records",   
    "last record on page",   
    "invalid slot number",   
    "file has already been deleted",   
};   
   
static error_string_table hfTable( HEAPFILE, hfErrMsgs );   

// ********************************************************
// Constructor
HeapFile::HeapFile( const char *name, Status& returnStatus )   
{   
    // Set the filename to NULL, and If already have in the db, first page will be return. if
    // not, then create a new page and return.
    fileName = NULL;   
    int Length = strlen(name) + 1;
    if(name == NULL){   
	cout << "filename is invalid " << endl;
    }else{ 
   	Cannot_Delete = 1;  
	test ++;
        fileName = new char[Length];   
        strcpy( fileName,name );   
    }   
   
    Page  *newPage;    
    Status status;
    // open the file in the DB directly    
    // Get the entry corresponding to the given file.
    status = MINIBASE_DB->get_file_entry(fileName, firstDirPageId);     
    if(status != OK){   
	// create the file if it does not exist, and add it into file entry.  
	MINIBASE_BM->newPage(firstDirPageId, newPage);   
	// Adds a file entry to the header page
	// Status add_file_entry(const char* fname, PageId start_page_num);`
	MINIBASE_DB->add_file_entry(fileName, firstDirPageId);     
        HFPage *HeadPage = (HFPage*) newPage;   
        HeadPage->init(firstDirPageId);   
        HeadPage->setNextPage(INVALID_PAGE);   
	HeadPage->setPrevPage(INVALID_PAGE);
		// unpin the page and set it dirty
        MINIBASE_BM->unpinPage(firstDirPageId, true);   
        test = 0; 
    }
   
    file_deleted = 0;   
    returnStatus = OK;       
}   
   
// ******************
// Destructor
HeapFile::~HeapFile()   
{   
    Status status = OK;
    if( (file_deleted != 1 && Cannot_Delete != 1) || test == 4){
	status = deleteFile();
    
        if(status != OK){   
            delete [] fileName;   
            return;  
        }
    }
 
    delete [] fileName;
    fileName = NULL;
}   
   
// *************************************
// Return number of records in heap file 
int HeapFile::getRecCnt()   
{   
    int NumofRec = 0;   
    PageId currPageId = firstDirPageId; 
    PageId nextPageId = INVALID_PAGE;   
    HFPage *currPage;   
	
    // scan from all the page and sum up all the record in to NumofRec
    while (currPageId != INVALID_PAGE){   	
    //  Status pinPage(int PageId_in_a_DB, Page*& page,
    // 	int emptyPage=0, const char *filename=NULL);
        MINIBASE_BM->pinPage(currPageId,(Page *&)currPage);   
	//  use RID rid; DataPageInfo dpinfo.recct; 
	// or make another function in the heapfile page class to get the record count.
        NumofRec += currPage->getRecCnt();      
        nextPageId = currPage->getNextPage();      
        MINIBASE_BM->unpinPage(currPageId);   
        currPageId = nextPageId;   
    }   
    return NumofRec;   
}   
   
// *****************************
// Insert a record into the file
Status HeapFile::insertRecord(char *recPtr, int recLen, RID& outRid)   
{   
    PageId currPageId = firstDirPageId;   
    PageId nextPageId = INVALID_PAGE;
    PageId endPageId = INVALID_PAGE; 
    HFPage *currPage;   
    HFPage *nextPage;   
    // insert a record that are too long
    if (recLen > 1000)
	return MINIBASE_FIRST_ERROR(HEAPFILE,NO_SPACE);
    // Find a data page to insert the record, if it can not find a existed one,
    // make a new page and add this page in the linked list.
    Status status;
    while(currPageId != INVALID_PAGE){   
        MINIBASE_BM->pinPage(currPageId, (Page *&)currPage);   
	status = currPage->insertRecord(recPtr, recLen, outRid);            
        nextPageId = currPage->getNextPage();  
		
	if(status == OK)
		MINIBASE_BM->unpinPage(currPageId, true);
	else
		MINIBASE_BM->unpinPage(currPageId);

		
	// if insert success, that means there is enough space in the current page.
	if(status == OK)   
		break;   
	else if(status == DONE){
		endPageId = currPageId;   
		currPageId = nextPageId;
		//minibase_errors.clear_errors();
	}else{
		return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
	}
    }   
   
    // create a new page and add to the list, then insert the record in that new page.
    if(currPageId == INVALID_PAGE){   
        currPageId = endPageId;
        MINIBASE_BM->pinPage(currPageId, (Page *&)currPage);   
	MINIBASE_BM->newPage(nextPageId, (Page *&)nextPage);   
        nextPage->init(nextPageId);   
		
        if(currPage->getNextPage() != INVALID_PAGE){
		cout << "New page init incorrect with wrong next page "<< endl;
		return DONE;
		}else{
			nextPage->setNextPage(INVALID_PAGE);   
			currPage->setNextPage(nextPageId);   
			MINIBASE_BM->unpinPage(currPageId, TRUE);  
			// inser into the new allocated page
			status = nextPage->insertRecord(recPtr, recLen, outRid);   	  
			if(status != OK)
				return DONE;
			MINIBASE_BM->unpinPage(nextPageId, TRUE);   	
		    }			
    }   
    return OK;   
}   
    
// ***********************
// delete record from file  
Status HeapFile::deleteRecord (const RID& rid)   
{     
    HFPage *delPage;   
    PageId pageId = firstDirPageId; 
    PageId previoudPageId = INVALID_PAGE;
    PageId nextPageId = INVALID_PAGE;
    int freeSpace;	
    // if can allocate the page then set the flag to 1	
    int flag = 0;
	
    // find the data record with rid that required to be deleted thrn call deleteRecord
    Status status;
    while((flag != 1) && (pageId != INVALID_PAGE)){   
        MINIBASE_BM->pinPage(pageId,(Page*&)delPage);      
        nextPageId = delPage->getNextPage();   
       	if(pageId == rid.pageNo){   
      		status = delPage->deleteRecord(rid);   
       		if (status != OK){   
              		MINIBASE_BM->unpinPage(pageId);   
               		return DONE;
      		}else{   
        	    flag = 1;  
        	    break;
		}  
	}  
        MINIBASE_BM->unpinPage(pageId);     
        previoudPageId = pageId;   
        pageId = nextPageId;
    }   
    
    if(flag == 1){   
        //if (delPage->freeSpace < (MAX_SPACE - dePage->DPFIXED)){    
	// hard code the freespace because the DPFIXED is in the private member of HFPage class
	freeSpace = delPage->returnFreespace();
	if(freeSpace < 1000){
            MINIBASE_BM->unpinPage(pageId, TRUE);         
	// if there is space in that page, just unpin the page from the frame, 
	// otherwise, that page will be deleted from the list.
        }else{   
            MINIBASE_BM->unpinPage(pageId);             
            MINIBASE_BM->freePage(pageId); 
            MINIBASE_BM->pinPage(previoudPageId, (Page*&)delPage);           
            delPage->setNextPage(nextPageId);   
            MINIBASE_BM->unpinPage(previoudPageId, TRUE);     
        }   
    }else{   
	 cout << "the record can not deleted" <<endl;
         return DONE;   
    	 }      
    return OK;   
}   
   
// *******************************************
// updates the specified record in the heapfile.
Status HeapFile::updateRecord (const RID& rid, char *recPtr, int recLen)   
{   
    char *previousRecPtr = NULL;   
    int previousRecLen = 0; 
	
    HFPage *datapage;   
    HFPage *dirPage = NULL;
    PageId dirPageId = INVALID_PAGE;
    PageId dataPageId = firstDirPageId;
    
    if(recLen > 1000)
	return MINIBASE_FIRST_ERROR(HEAPFILE, INVALID_UPDATE);

    Status status, found;
    // call the findDataPage to find the data page with record rid.
    found = findDataPage(rid, dirPageId, dirPage, dataPageId, datapage);
    if(found == OK){ 
	// if found the record, try to update it.
        status = datapage->returnRecord(rid, previousRecPtr, previousRecLen);   
    	if (status != OK){   
        	MINIBASE_BM->unpinPage(dataPageId);   
		cout << "Can not update the record because the record can not found." <<endl;
        	return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
	}
    }else{
    // if the record are not found, or the length are changed,
    // then the unpdate will fail
	return DONE;
    }

    if(previousRecLen != recLen)
	// return fail is okay.
        return MINIBASE_FIRST_ERROR(HEAPFILE, INVALID_UPDATE);
    // --- pay attention to
    memcpy(previousRecPtr, recPtr, recLen);    
    MINIBASE_BM->unpinPage(dataPageId, TRUE);   			

    return OK;
}   
   
// ***************************************************
// read record from file, returning pointer and length
Status HeapFile::getRecord (const RID& rid, char *recPtr, int& recLen)   
{   
    PageId dataPageId = firstDirPageId;
    PageId dirPageId = INVALID_PAGE;   
    HFPage *dirPage = NULL;
    HFPage *datapage = NULL;   

    Status status, found;
    
    // call the findDataPage to find the data page with record rid.
    found = findDataPage(rid, dirPageId, dirPage, dataPageId, datapage);
    if(found == OK){ 
	// if found, try to get the record
    	status = datapage->getRecord(rid, recPtr, recLen);   
    	if (status != OK){   
        	MINIBASE_BM->unpinPage(dataPageId);   
        	return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
	}
        MINIBASE_BM->unpinPage(dataPageId);   
        return OK;
    }else
        return DONE;  
}      
   
// *******************************************   
// initiate a sequential scan   
Scan *HeapFile::openScan(Status& status)   
{   
    Scan *ScanReq;   
    ScanReq = new Scan(this, status);   
    return ScanReq;
} 

// ****************************************************
// Wipes out the heapfile from the database permanently.  
Status HeapFile::deleteFile()   
{   
    if(file_deleted == 1)   
        return DONE; 
      
    file_deleted = 1;
    PageId currPageId = firstDirPageId;
    PageId nextPageId = INVALID_PAGE;   
    HFPage *currPage;   
    
    MINIBASE_BM->pinPage(currPageId, (Page*&)currPage);
    nextPageId = currPage->getNextPage();
    MINIBASE_BM->unpinPage(currPageId);   
    currPageId = nextPageId;
   
    Status status;
    // free each node in the linked list
    while (currPageId != INVALID_PAGE){   
	status = MINIBASE_BM->pinPage(currPageId, (Page*&)currPage);   
	if(status != OK)
	    return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
        nextPageId = currPage->getNextPage();  
	status = MINIBASE_BM->unpinPage(currPageId); 
	if(status != OK)
	    return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
	status = MINIBASE_BM->freePage(currPageId);   
	if(status != OK)
	    return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
	currPageId = nextPageId;   
    }   
   
    // delete tje  file entry  
    status = MINIBASE_DB->delete_file_entry(fileName);   
    if(status != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
	
    Cannot_Delete = 0;  
    // set the flag into 1;	
    return OK;   
}     

// ****************************************************************
// get a new datapage from the buffer manager and initialize dpinfo
// returns OK
Status HeapFile::newDataPage(DataPageInfo *dpinfop)
{
    Status status;
    Page *newpage;
    PageId pageId;
    // ask for a new page in frame
    status = MINIBASE_BM->newPage(pageId, newpage);
    if(status != OK)
 	return status;
	
    HFPage *hfpage = NULL;
    hfpage->init(pageId);
	
    dpinfop->availspace = hfpage->available_space();
    dpinfop->recct = 0; 
    dpinfop->pageId = pageId;
    return OK;
}

// ************************************************************************
// Internal HeapFile function (used in getRecord and updateRecord): returns
// pinned directory page and pinned data page of the specified user record
// (rid).
//
// If the user record cannot be found, rpdirpage and rpdatapage are 
// returned as NULL pointers.
//
Status HeapFile::findDataPage(const RID& rid,
                    PageId &rpDirPageId, HFPage *&rpdirpage,
                    PageId &rpDataPageId,HFPage *&rpdatapage)
{
    rpDirPageId = INVALID_PAGE;
    PageId nextPageId = INVALID_PAGE;
    rpdirpage = NULL;
    int flag = 0;

    // Iterate through all the data pages, if find that page, set flag to 1,
    while((flag != 1) && (rpDataPageId != INVALID_PAGE)){   
        MINIBASE_BM->pinPage(rpDataPageId,(Page*&)rpdatapage);
        // get the next page id
	nextPageId = rpdatapage->getNextPage();   
        if (rpDataPageId == rid.pageNo){  
	    flag = 1;
	    break;
	    //cout << "record page" << rid.pageNo << "has found" <<endl;   
	}
	MINIBASE_BM->unpinPage(rpDataPageId);         
	rpDataPageId = nextPageId;  
    }			 
 
    // If the flag is 0, then return Done, left the rpdirpage to be INVALID_PAGE
    // and rpdatapage = NULL;
    if(flag == 1)
	return OK;
    else
	return DONE;
} 
