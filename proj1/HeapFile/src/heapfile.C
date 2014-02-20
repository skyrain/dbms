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
		// unpin the page and set it dirty
        MINIBASE_BM->unpinPage(firstDirPageId, true);    
    }
   
    file_deleted = 0;   
    returnStatus = OK;       
}   
   
// ******************
// Destructor
HeapFile::~HeapFile()   
{   
    Status status;
    if( file_deleted != 1) 
	status = deleteFile();
   
    if( status != OK ){   
        cout << " delete the file unsuccessfully" << endl;   
        delete [] fileName;   
        return;   
    }else{
        fileName = NULL;
        delete [] fileName;
    }
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
	// ??? use RID rid; DataPageInfo dpinfo.recct; 
	// or make another function in the heapfile page class to get the record count.
        NumofRec += currPage->slotCnt;      
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
	
    // Find a data page to insert the record, if it can not find a existed one,
    // make a new page and add this page in the linked list.
    Status status;
    while(currPageId != INVALID_PAGE){   
        MINIBASE_BM->pinPage(currPageId, (Page *&)currPage);   
	status = currPage->insertRecord(recPtr, recLen, outRid);            
        nextPageId = currPage->getNextPage();    
	MINIBASE_BM->unpinPage(currPageId,(status == OK));   
		
	// if insert success, that means there is enough space in the current page.
        if(status == OK)   
            break;   
	else if(minibase_error.error_index() == NO_SPACE){
		endPageId = currPageId;   
		currPageId = nextPageId;
		minibase_errors.clear_errors();
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
	PageId prevPageId = INVALID_PAGE;
	PageId nextPageId = INVALID_PAGE;
	
    	int flag = 0;
	Status status;
	
	// find the data record with rid that required to be deleted
    	while((flag != 0) && (pageId != INVALID_PAGE)){   
        	MINIBASE_BM->pinPage(pageId,(Page*&)delPage);      
        	nextPageId = delPage->getNextPage();   
        	if(pageId == rid.pageNo){   
            		status = delPage->deleteRecord(rid);   
            	if (status != OK){   
                	MINIBASE_BM->unpinPage(pageId);   
                	return DONE;
            	}   
            flag = 1;   
            break;   
        }  		
	MINIBASE_BM->unpinPage(pageId);     
        prevPageId = pageId;   
        pageId = nextPageId;   
  }   
    
    if(flag == 1){   
        //if (delPage->freeSpace < (MAX_SPACE - dePage->DPFIXED)){    
	// hard code the freespace because the DPFIXED is in the private member of HFPage class
	if(delPage->freeSpace < 1000){
            MINIBASE_BM->unpinPage(pageId, TRUE);         
	// if there is space in that page, just unpin the page from the frame, 
	// otherwise, that page will be deleted from the list.
        }else{   
            MINIBASE_BM->unpinPage(pageId);             
            MINIBASE_BM->freePage(pageId);                 
            MINIBASE_BM->pinPage(prevPageId, (Page*&)delPage);           
            delPage->setNextPage(nextPageId);   
            MINIBASE_BM->unpinPage(prevPageId, TRUE);     
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
    int flag = 0;
    char *PrevRecPtr = NULL;   
    int PrevRecLen = 0; 
	
    HFPage *datapage;   
    PageId dataPageId = firstDirPageId;
    PageId nextPageId;   
    
    Status status;
    while((flag != 1) && (dataPageId != INVALID_PAGE)){   
        MINIBASE_BM->pinPage(dataPageId,(Page*&)datapage);      
        if(dataPageId == rid.pageNo){   
	    // get the previous record info.
            status = datapage->returnRecord(rid, PrevRecPtr, PrevRecLen);   
            if(status != OK){   
                MINIBASE_BM->unpinPage(dataPageId);   
		cout << "Can not update the record because the record can not found." <<endl;
                return  DONE;   
            } 
	// set the flag indicates that the record is found.
            flag = 1;   
            break;   
        }   
		
		// if the record are not found, go to the next page.
        MINIBASE_BM->unpinPage(dataPageId);   
	nextPageId = datapage->getNextPage();
        dataPageId = nextPageId;   
    }   
   
    // if the record are not found, ore the length are change,
    // then the unpdate will fail
    if(flag != 1 && PrevRecLen != recLen){   
        cout<< "invalid undate request" << endl;
		return DONE;
	}else{	
		// ??? be careful on that
		memcpy(PrevRecPtr, recPtr, recLen);    
		MINIBASE_BM->unpinPage(dataPageId, TRUE);   
	}			
   
    return OK;   
}   
   
// ***************************************************
// read record from file, returning pointer and length
Status HeapFile::getRecord (const RID& rid, char *recPtr, int& recLen)   
{   
    PageId dataPageId = firstDirPageId;
    PageId nextPageId;   
    HFPage *datapage = NULL;   
    int flag = 0;   
   
    Status status; 
    while((flag != 0) && (dataPageId != INVALID_PAGE)){   
        MINIBASE_BM->pinPage(dataPageId,(Page*&)datapage);
        nextPageId = datapage->getNextPage();   
        if (dataPageId == rid.pageNo){   
            status = datapage->getRecord(rid, recPtr, recLen);   
            if (status != OK){   
                MINIBASE_BM->unpinPage(dataPageId);   
                return DONE;
            }else{   
		 flag = 1; 
		 cout << "record page" << rid.pageNo << "has found" <<endl;
		 }				
        }else{   
	     MINIBASE_BM->unpinPage(dataPageId);         
	     dataPageId = nextPageId;  
	     }			
    }    
    if(flag == 1)   
        return OK;   
       
    return DONE;   
}      
   
// *******************************************   
// initiate a sequential scan   
Scan *HeapFile::openScan(Status& status)   
{   
    Scan *ScanReq;   
    ScanReq = new Scan(this, status);   
   
    if(status == OK)   
        return ScanReq;   
    else{   
        delete ScanReq;   
        return NULL;   
        }     
} 

// ****************************************************
// Wipes out the heapfile from the database permanently.  
Status HeapFile::deleteFile()   
{   
    if(file_deleted == 1)   
        return DONE; 
      
    PageId currPageId = firstDirPageId;
    PageId nextPageId = INVALID_PAGE;   
    HFPage *currPage;   
    
    // free each node in the linked list
    while (currPageId != INVALID_PAGE){   
	MINIBASE_BM->pinPage(currPageId, (Page*&)currPage);   
        nextPageId = currPage->getNextPage();   
	MINIBASE_BM->freePage(currPageId);   
	currPageId = nextPageId;   
    }   
   
    // Deallocate the file entry and header page.   
    MINIBASE_DB->delete_file_entry(fileName);   
	
    // set the flag into 1;	
    file_deleted = 1;  
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
