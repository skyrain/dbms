/*
 * btreefilescan.h
 *
 * sample header file
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 *
 */
 
#ifndef _BTREEFILESCAN_H
#define _BTREEFILESCAN_H

#include "btfile.h"

// errors from this class should be defined in btfile.h

class BTreeFileScan : public IndexFileScan {
public:
    friend class BTreeFile;

    // get the next record
    Status get_next(RID & rid, void* keyptr);

    // delete the record currently scanned
    Status delete_current();

    int keysize(); // size of the key

    // destructor
    ~BTreeFileScan();
private:
	// range of the scan.
	const void *lo_key;
	const void *hi_key;
	
	// Meta data of the scanning btree.
	int keySize;
	RID curRid;	
	Page *curPage;
	AttrType keyType;
	bool curDeleted;
	PageId rootPageId;
	
	// Constructor with scan parameters.
	BTreeFileScan(const void *l, const void *h, AttrType keytype, int keysize, PageId rootid);
	
	// Supporting function for the constructor, it will find the left most node of the tree.
	Status fromLeftMostPage(PageId pageid);
	
	// Supporting function for the constructor, it will find the low key node of the tree.
	Status fromLowPage(PageId pageid);
};

#endif
