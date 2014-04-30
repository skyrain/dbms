/*
	btreefilescan.C
	Define all the operations on a B tree file scan.

	- Overall description of your algorithms and data structures

	// Constructor of BTreeFileScan, input will be the headerPage infor, and hikey and lokey.
	BTreeFileScan::BTreeFileScan(const void *l, const void *h, AttrType keytype, int keysize, PageId rootid)

	// Deconstructor of BTreeFileScan.
	BTreeFileScan::~BTreeFileScan()

	// get the next record
	Status BTreeFileScan::get_next(RID & rid, void* keyptr)

	// delete the record currently scanned
	Status BTreeFileScan::delete_current()

	// size of the key
	int BTreeFileScan::keysize() 


	// Supporting function for the constructor, it will find the left most node of the tree.
	// Dealing the following cases:
   	//      (1) lo_key = NULL, hi_key = NULL
   	//              scan the whole index
   	//      (2) lo_key = NULL, hi_key!= NULL
  	//              range scan from min to the hi_key
	Status BTreeFileScan::fromLeftMostPage(PageId pageid)
		

	// Supporting function for the constructor, it will find the low key node of the tree.
	// Dealing the following cases:
   	//      (3) lo_key!= NULL, hi_key = NULL
   	//              range scan from the lo_key to max
   	//      (4) lo_key!= NULL, hi_key!= NULL, lo_key = hi_key
   	//              exact match ( might not unique)
   	//      (5) lo_key!= NULL, hi_key!= NULL, lo_key < hi_key
   	//              range scan from lo_key to hi_key
	Status BTreeFileScan::fromLowPage(PageId pageid)

	- Anything unusual in your implementation

	Design and implementation details are the same as what descriped in the course website.

	- What functionalities not supported well

	All functionalities are implemented, but not work properly so far, we will fix that soon.

*/

#include "minirel.h"
#include "buf.h"
#include "db.h"
#include "new_error.h"
#include "btfile.h"
#include "btreefilescan.h"

/*
 * Note: BTreeFileScan uses the same errors as BTREE since its code basically 
 * BTREE things (traversing trees).
 */

// Constructor of BTreeFileScan, input will be the headerPage infor, and hikey and lokey.
BTreeFileScan::BTreeFileScan(const void *l, const void *h, AttrType keytype, int keysize, PageId rootid)
{
	// Pass the value to the file.
	this->keyType = keytype;
	this->keySize = keysize;
	this->curDeleted = true;
	this->lo_key = l;
	this->hi_key = h;
	this->rootPageId = rootid;
	
	curRid.pageNo = INVALID_PAGE;
	curRid.slotNo = INVALID_SLOT;
	
	
	Status status;
	// if assign lo_key = NULL, start from the left most page.
	if(lo_key == NULL)
	{	
		status = fromLeftMostPage(rootPageId);
		if(status != OK)
			curRid.pageNo = INVALID_PAGE;
	}else{	
		// start from the given low key.
		status = fromLowPage(rootPageId);
		if(status != OK)
			curRid.pageNo = INVALID_PAGE;	
	}
}

// Deconstructor of BTreeFileScan.
BTreeFileScan::~BTreeFileScan()
{
	Status status;
	// unpin the current page.
	if(curPage != NULL){
		PageId curPageId = curRid.pageNo;
		status = MINIBASE_BM->unpinPage(curPageId, true);
		if(status != OK)
			MINIBASE_FIRST_ERROR(BTREE, CANT_UNPIN_PAGE);
	}
}

// get the next record
Status BTreeFileScan::get_next(RID & rid, void* keyptr)
{
	Status status;
	curDeleted = false;
	PageId prevPageId = INVALID_PAGE;
	BTLeafPage *lPage = NULL;
	lPage = (BTLeafPage *)curPage;

	// no page, scan finish.
	if(curRid.pageNo == INVALID_PAGE)
		return DONE;

	// if this is the first rec in the page, get the first record.
	if(curRid.slotNo == INVALID_SLOT)
	{
		status = lPage->get_first(curRid, keyptr, rid);
		if(status != OK)
			return MINIBASE_CHAIN_ERROR(BTREE, status);
	}
	else
	{
		// recursively call get next until return status is DONE.
		status = lPage->get_next(curRid, keyptr, rid);
		if(status == NOMORERECS){
			prevPageId = curRid.pageNo;
			curRid.pageNo = lPage->getNextPage();
			curRid.slotNo = INVALID_SLOT;
			// unpin the previous page because scanner has move the next page.
			status = MINIBASE_BM->unpinPage(prevPageId, true);
			if( status != OK)
				return MINIBASE_FIRST_ERROR(BTREE, DELETE_TREE_ERROR);

			// if there is next Page, then pin this page into buffer pool.
			if(curRid.pageNo != INVALID_PAGE){
				status = MINIBASE_BM->pinPage(curRid.pageNo, curPage);
				if(status != OK)
					return MINIBASE_FIRST_ERROR(BTREE, DELETE_TREE_ERROR);
			}

			return get_next(rid, keyptr);
		}

		/* Implementation of Skipping from continues duplicate key*/
		// Skip the duplicate record.
		int contRes;
		RID contRid = curRid;
		RID contERid = rid;
		void* contKey = malloc(sizeof(keyptr));
		//contKey = keyptr;
		Page *tmpPage;
		BTLeafPage *contPage = NULL;

		status = lPage->get_next(contRid, contKey, contERid);
		// 1. if the continues record is in the next page.
		if(status == NOMORERECS){
			prevPageId = contRid.pageNo;
			contRid.pageNo = lPage->getNextPage();
			contRid.slotNo = INVALID_SLOT;

			// if there is next Page, then pin this page into buffer pool.
			if(contRid.pageNo != INVALID_PAGE){
				status = MINIBASE_BM->pinPage(contRid.pageNo, tmpPage);
				contPage = (BTLeafPage *)tmpPage;
				if(status != OK)
					return MINIBASE_FIRST_ERROR(BTREE, DELETE_TREE_ERROR);
			}

			// isn't continues record, so just need to compare the current key with hi_key.
			if(contRid.pageNo == INVALID_PAGE)
			{	
				// determine whether the key beyond the hi_key key at last.
				int resh = 0;
				if(hi_key != NULL){
					resh = keyCompare(keyptr, hi_key, keyType);
				}
				// if hi_key is not max and there is key larger than hikey, then return DONE to finish the scan.
				if(resh > 0){
					status = MINIBASE_BM->unpinPage(curRid.pageNo, true);
					if(status != OK)
						return MINIBASE_FIRST_ERROR(BTREE, CANT_UNPIN_PAGE);
					curRid.pageNo = INVALID_PAGE;			
					return DONE;
				}

				status = MINIBASE_BM->unpinPage(curRid.pageNo, true);
				if(status != OK)
					return MINIBASE_FIRST_ERROR(BTREE, CANT_UNPIN_PAGE);
				curRid.pageNo = INVALID_PAGE;

				// both case, scan is done.
				return OK;
			}

			// if this is the first rec in the page, get the first record.
			if(contRid.slotNo == INVALID_SLOT)
			{
				status = contPage->get_first(contRid, contKey, contERid);
				if(status != OK)
					return MINIBASE_CHAIN_ERROR(BTREE, status);
			}

			// compare to find a duplicate key in next page, if it is a duplicate key, skip to next key
			contRes = keyCompare(keyptr, contKey, keyType);
			if(contRes == 0)
			{
				//curRid = contRid;
				//rid = contERid;
				// same key, needn't to assign the value;
				// keyptr = contKey;
				// unpin the previous page because scanner has move the next page.
				status = MINIBASE_BM->unpinPage(prevPageId, true);
				if( status != OK)
					return MINIBASE_FIRST_ERROR(BTREE, DELETE_TREE_ERROR);

				// change the next page to the next page.
				curPage = tmpPage;
				return get_next(contERid, contKey);
			}	
			// else 
			// unpin the next page because scanner has to stay.
			status = MINIBASE_BM->unpinPage(contRid.pageNo, true);
			if( status != OK)   	       
				return MINIBASE_FIRST_ERROR(BTREE, DELETE_TREE_ERROR);
		}

		// 2. if the continues record in current page, compare it.

		// compare to find a duplicate key in current page, if it is a duplicate key, skip to next key
		contRes = keyCompare(keyptr, contKey, keyType);
		if(contRes == 0)
			return get_next(contERid, contKey);		
		/* End of the implementation of skipping continues key*/
	}		

	// determine whether the key beyond the hi_key key at last.
	int resh = 0;
	if(hi_key != NULL){
		resh = keyCompare(keyptr, hi_key, keyType);
	}
	// if hi_key is not max and there is key larger than hikey, then return DONE to finish the scan.
	if(resh > 0){
		status = MINIBASE_BM->unpinPage(curRid.pageNo, true);
		if(status != OK)
			return MINIBASE_FIRST_ERROR(BTREE, CANT_UNPIN_PAGE);
		curRid.pageNo = INVALID_PAGE;
		return DONE;
	}

	return OK;
}
// delete the record currently scanned
Status BTreeFileScan::delete_current()
{
	Status status;
	BTLeafPage *lPage = NULL;
	lPage = (BTLeafPage *)curPage;
	PageId prevPageId = INVALID_PAGE;
	int numofRec = 0;
	
	// curDeleted should be false now, and then set it true.
	if(curDeleted == true)
		return MINIBASE_FIRST_ERROR(BTREE, DELETE_TREE_ERROR);
	curDeleted = true;
	
	// call deleteRecord to delete
	status = lPage->deleteRecord(curRid);
	if(status != OK)
		return MINIBASE_CHAIN_ERROR(BTREE, status);
	
	// if No record on the current page, move to the next page.
	numofRec = lPage->numberOfRecords();
	if(numofRec == 0){
		prevPageId = curRid.pageNo;
		curRid.pageNo = lPage->getNextPage();
		curRid.slotNo = INVALID_SLOT;
		// unpin the previous page because scanner has move the next page.
		status = MINIBASE_BM->unpinPage(prevPageId, true);
		if( status != OK)
			return MINIBASE_FIRST_ERROR(BTREE, DELETE_TREE_ERROR);

		// if there is next Page, then pin this page into buffer pool.
		if(curRid.pageNo != INVALID_PAGE){
			status = MINIBASE_BM->pinPage(curRid.pageNo, curPage);
			if(status != OK)
				return MINIBASE_FIRST_ERROR(BTREE, DELETE_TREE_ERROR);
		}
	}
	else{
		curRid.slotNo--;	
	}
		
	return OK;
}

// size of the key
int BTreeFileScan::keysize() 
{
	int keysize;
	// keySize equal to headerPage->key_size;
	keysize = this->keySize;
	return keysize;
}

// Supporting function for the constructor, it will find the left most node of the tree.
// Dealing the following cases:
    //      (1) lo_key = NULL, hi_key = NULL
    //              scan the whole index
    //      (2) lo_key = NULL, hi_key!= NULL
    //              range scan from min to the hi_key
Status BTreeFileScan::fromLeftMostPage(PageId pageid)
{
	Status status;
	Page *tmpPage = NULL;
	SortedPage *tmpSPage = NULL;
	BTIndexPage *tmpIPage = NULL;
	short type;	

	status = MINIBASE_BM->pinPage(pageid, tmpPage);
	if(status != OK)
		return MINIBASE_FIRST_ERROR(BTREE, CANT_PIN_PAGE);
	
	// if the type is still the index page, the recursively call this funciton to get to the leaf.
	tmpSPage = (SortedPage *)tmpPage;
	tmpIPage = (BTIndexPage *)tmpPage;
	type = tmpSPage->get_type();
	if(type == INDEX){
		// call fromLeftMostPage recursively.
		status = fromLeftMostPage(tmpIPage->getLeftLink());
		if(status != OK)
			return MINIBASE_FIRST_ERROR(BTREE, NO_LEAF_NODE);
		
		// unpin the current page;
		status = MINIBASE_BM->unpinPage(pageid, true);
		if(status != OK)
			return MINIBASE_FIRST_ERROR(BTREE, CANT_UNPIN_PAGE);
	} 
	
	// if the node is not a INDEX oage, then we fine it!
	if(type == LEAF){
		curPage = tmpPage;
		curRid.pageNo = pageid;
	}

	return OK;
}

// Supporting function for the constructor, it will find the low key node of the tree.
// Dealing the following cases:
    //      (3) lo_key!= NULL, hi_key = NULL
    //              range scan from the lo_key to max
    //      (4) lo_key!= NULL, hi_key!= NULL, lo_key = hi_key
    //              exact match ( might not unique)
    //      (5) lo_key!= NULL, hi_key!= NULL, lo_key < hi_key
    //              range scan from lo_key to hi_key
Status BTreeFileScan::fromLowPage(PageId pageid)
{
	Status status;
        PageId tmpId = INVALID_PAGE;
	Page *tmpPage = NULL;
        SortedPage *tmpSPage = NULL;
        BTIndexPage *tmpIPage = NULL;
	BTLeafPage *tmpLPage = NULL;	

	status = MINIBASE_BM->pinPage(pageid, tmpPage);
	if(status != OK)
		return MINIBASE_FIRST_ERROR(BTREE, CANT_PIN_PAGE);
	
	// if the type is still the index page, the recursively call this funciton to get to the leaf.
        short type;
	tmpSPage = (SortedPage *)tmpPage;
        tmpIPage = (BTIndexPage *)tmpPage;
        type = tmpSPage->get_type();
	if(type == INDEX){
		// if this is a index, them find the child node of given low key by calling get_page_no.
		tmpIPage->get_page_no(lo_key, keyType, tmpId);
		status = MINIBASE_BM->unpinPage(pageid, true);
		if(status != OK)
			return MINIBASE_FIRST_ERROR(BTREE, CANT_UNPIN_PAGE);
		// recursively call fromLowPage if it is still an index.
		status = fromLowPage(tmpId);
		if(status != OK)	
			return MINIBASE_FIRST_ERROR(BTREE, NO_LEAF_NODE);
	}
	
	// if this is the leaf node, then find the low key record, to start with.
	if(type == LEAF){
		KeyDataEntry dataEntry;
		RID tmpRid;
		curPage = tmpPage;
		tmpLPage = (BTLeafPage *)tmpPage;
		status = tmpLPage->get_first(curRid, &dataEntry.key, tmpRid);
		if(status != OK)	
			return MINIBASE_CHAIN_ERROR(BTREE, status);
		
		// Calling KeyCompare until find the start point to scan.
		int res = 0;
		res = keyCompare(&dataEntry.key, lo_key, keyType);
		while(res < 0){
			status = tmpLPage->get_next(curRid, &dataEntry.key, tmpRid);
			if(status != OK){
				if(status == NOMORERECS){
			 		tmpId = curRid.pageNo;
		                        curRid.pageNo = tmpLPage->getNextPage();
        		                curRid.slotNo = INVALID_SLOT;
                		        // unpin the previous page because scanner has move the next page.
                        		status = MINIBASE_BM->unpinPage(tmpId, true);
	                        	if( status != OK)
        	                        	return MINIBASE_FIRST_ERROR(BTREE, DELETE_TREE_ERROR);
	
	        	                // if there is next Page, then pin this page into buffer pool.
        	        	        if(curRid.pageNo != INVALID_PAGE){
                	        	        status = MINIBASE_BM->pinPage(curRid.pageNo, curPage);
                        	        	if(status != OK)
                                	        	return MINIBASE_FIRST_ERROR(BTREE, DELETE_TREE_ERROR);
                        		}
				}else
						return MINIBASE_CHAIN_ERROR(BTREE, status);
			}
			res = keyCompare(&dataEntry.key, lo_key, keyType);
		}
		curRid.slotNo --;
	}
	
	return OK;
}
