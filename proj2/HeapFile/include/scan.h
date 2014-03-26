/*
 * class Scan
 * $Id: scan.h,v 1.1 1997/01/02 12:46:43 flisakow Exp $
 */

#ifndef _SCAN_H_
#define _SCAN_H_

#include "minirel.h"

// ***********************************************************
// A Scan object is created ONLY through the function openScan
// of a HeapFile. It supports the getNext interface which will
// simply retrieve the next record in the heapfile.
//
// An object of type scan will always have pinned one directory page
// of the heapfile.

class HeapFile;
class HFPage;

class Scan {

  public:
    // The constructor pins the first page in the file
    // and initializes its private data members from the private
    // data members from hf
    Scan(HeapFile *hf, Status& status);
   ~Scan();

    // Retrieve the next record in a sequential scan
    // Also returns the RID of the retrieved record.
    Status getNext(RID& rid, char *recPtr, int& recLen);

    // Position the scan cursor to the record with the given rid.
    // Returns OK if successful, non-OK otherwise.
    Status position(RID rid);

  private:
    /*
     * See heapfile.h for the overall description of a heapfile.
     * (Then see hfpage.h for HFPage ops.)
     */

    // The heapfile we are using.
    HeapFile  *_hf;

    // the actual PageId of the dir page with current position
    PageId dirPageId;

	PageId dataPageId;

	// the rid of the data page
	RID dataPageRid;

	HFPage *dirPage;

    // in-core copy (pinned) of the same
    HFPage *dataPage;

    // record ID of the current record (from the current data page)
    RID     userRid;
	int     scanIsDone;
	int     nxtUserStatus;

    // Do all the constructor work
    Status init(HeapFile *hf);

    // Reset everything and unpin all pages.
    Status reset();

    // Move over the data pages in the file.
    Status firstDataPage();
    Status nextDataPage();
	Status nextDirPage();

    Status peekNext(RID& rid) {
        rid = userRid;
        return OK;
    }

    // Move to the next record in a sequential scan.
    // Also returns the RID of the (new) current record.
    Status mvNext(RID& rid);
};

#endif  // _SCAN_H
