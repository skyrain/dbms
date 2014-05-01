/*
sortedMerge.C:
The sortMerge constructor joins two relations R and S, using the sort-merge join algorithm.

- Overall description of your algorithms and data structures

SortMerge::sortMerge(
		char*           filename1,      // Name of heapfile for relation R
		int             len_in1,        // # of columns in R.
		AttrType        in1[],          // Array containing field types of R.
		short           t1_str_sizes[], // Array containing size of columns in R
		int             join_col_in1,   // The join column of R 

		char*           filename2,      // Name of heapfile for relation S
		int             len_in2,        // # of columns in S.
		AttrType        in2[],          // Array containing field types of S.
		short           t2_str_sizes[], // Array containing size of columns in S
		int             join_col_in2,   // The join column of S

		char*           filename3,      // Name of heapfile for merged results
		int             amt_of_mem,     // Number of pages available
		TupleOrder      order,          // Sorting order: Ascending or Descending
		Status&         s               // Status of constructor
		)

The sortMerge constructor joins two relations R and S, represented by the heapfiles filename1 and filename2,respectively, using the sort-merge join algorithm. If the record in S and R are equal, merge and insert into output file filename3, the merged record will do a Cartesian product on the equal record on R and S.

sortMerge::~sortMerge(): SortMerge destructor, it does nothing, all pointers and memory are reclaimed in the sortMerge function..

- Anything unusual in your implementation
Because it's sequencial scan, we use only one doubly linked list and it is faster than the directory structure.

- What functionalities not supported well
All functionalities are fully supported.

*/

#include <string.h>
#include <assert.h>
#include "sortMerge.h"

// Error Protocall:

static const char* ErrMsgs[] = 	{
	"Error: Sort Failed.",
	"Error: HeapFile Failed."
};

static error_string_table ErrTable( JOINS, ErrMsgs );

sortMerge::sortMerge(
		char*           filename1,      // Name of heapfile for relation R
		int             len_in1,        // # of columns in R.
		AttrType        in1[],          // Array containing field types of R.
		short           t1_str_sizes[], // Array containing size of columns in R
		int             join_col_in1,   // The join column of R 

		char*           filename2,      // Name of heapfile for relation S
		int             len_in2,        // # of columns in S.
		AttrType        in2[],          // Array containing field types of S.
		short           t2_str_sizes[], // Array containing size of columns in S
		int             join_col_in2,   // The join column of S

		char*           filename3,      // Name of heapfile for merged results
		int             amt_of_mem,     // Number of pages available
		TupleOrder      order,          // Sorting order: Ascending or Descending
		Status&         s               // Status of constructor
		){

		// Sorted file name.
		char *sorted1 = "out1";
		char *sorted2 = "out2";

		// Record length of relation R and S
		int recLenR = 0;
		int recLenS = 0;
		int recLenM = 0;
		// calculate the record length by, adding each of column in R and S for every array containing size of columns in R and S.
		int i;
		for(i = 0; i < len_in1; i++)
			recLenS += t1_str_sizes[i];
		for(i = 0; i < len_in2; i++)
			recLenR += t2_str_sizes[i];
		// Length of merged record is the lenth of record R + S.
		recLenM = recLenS + recLenR;
		
		// Call sort to sort the relation filename1 and filename2,  save the sorted file.
		Sort(filename1, sorted1, len_in1, in1,  t1_str_sizes,  join_col_in1,  order,  amt_of_mem, s);
		Sort(filename2, sorted2, len_in2, in2,  t2_str_sizes,  join_col_in2,  order,  amt_of_mem, s);
		if(s != OK)
			MINIBASE_FIRST_ERROR(JOINS,  SORT_FAILED);
		
		// Build up three heapfile, two of them are relation S.
		HeapFile* heapfileR = new HeapFile(sorted1, s);
		HeapFile* heapfileS1 = new HeapFile(sorted2, s);
		HeapFile* heapfileS2 = new HeapFile(sorted2, s);
		HeapFile* mergedfile = new HeapFile(filename3, s);
		if(s != OK)
			MINIBASE_FIRST_ERROR(JOINS,  HEAPFILE_FAILED);
		
		// Open three scan for heapfile R and S.
		Status statusR, statusS1, statusS2;
		Scan* scanR = heapfileR->openScan(statusR);
		Scan* scanS1 = heapfileS1->openScan(statusS1);
		Scan* scanS2 = heapfileS2->openScan(statusS2);

		// Allocate the memory of record pointer.
		char* recR_ptr = (char*)malloc(sizeof(recLenS));
		char* recS1_ptr = (char*)malloc(sizeof(recLenR));
		char* recS2_ptr = (char*)malloc(sizeof(recLenR));
		
		// get the first data record in each heapfile.
		RID ridR, ridS1, rid_S2;
		int len_R, len_S1, len_S2;	
		statusR = scanR->getNext(ridR, recR_ptr, len_R);
		statusS1 = scanS1->getNext(ridS1, recS1_ptr, len_S1);
		statusS2 = scanS2->getNext(rid_S2, recS2_ptr, len_S2);

		RID mergeRid;
		
		// the outer loop will determine whehter there is record in R or S.
		// End the merge when one of the relation is scaned.
		// Inner loop will do a Cartesian product on the tuple that R and S have the same value.
		// Since all the tuples are sorted in R and S, so only check the adjacent tuple.
		while( statusR == OK && statusS2 == OK)
		{
			int res = 0;
			res = tupleCmp(recR_ptr, recS2_ptr);
			// if record in R smaller than S,  then move pointer ahead in R.
			while(res < 0)
			{
				statusR = scanR->getNext(ridR, recR_ptr, len_R);
				res = tupleCmp(recR_ptr, recS2_ptr);
				// if R has no more record.
				if(statusR != OK)
					break;
			}

			// if record in R larger than S,  then move pointer ahead in S.
			while(res > 0)
			{
				statusS2 = scanS2->getNext(rid_S2, recS2_ptr, len_S2);
				res = tupleCmp(recR_ptr, recS2_ptr);
				if(statusS2!=OK)
					break;
			}

			// if we find the position that record S = R,  then user another scan S1 to call postion() find the position
			// that record s = record r.
			scanS1 -> position(rid_S2);
			statusS1 = scanS1 -> getNext(ridS1, recS1_ptr, len_S1);
			
			// if the record in S and R are equal,  merge and insert into output file
			// the merged record will do a Cartesian product on the equal record on R and S.
			while(res == 0)
			{
				while(res == 0)
				{
					// memcpy the equal record in R and S into mergedRec for insertion.
					char* mergedRec;
					mergedRec = (char*)malloc(recLenM);
					memcpy(mergedRec, recR_ptr, len_R);
					memcpy(mergedRec+len_R, recS1_ptr, len_S1);
					// insert the merged record in merged record.
					mergedfile->insertRecord(mergedRec, len_R +len_S1, mergeRid);
					// Move the pointer on S ahead.
					statusS1 = scanS1 ->getNext(ridS1, recS1_ptr, len_S1);
					if(statusS1 != OK)
						break;
					res = tupleCmp(recR_ptr, recS1_ptr);
				}
				
				// Move the pointer on R ahead and return to the first position that make S and R equal.
				statusR = scanR -> getNext(ridR, recR_ptr, len_R);
				if(statusR != OK)
					break;
				scanS1 -> position(rid_S2);
				
				// move the pointer ahead in relation S, if record in S and R still equal. the merge will continue.
				statusS1 = scanS1 -> getNext(ridS1, recS1_ptr, len_S1);	
				if(statusS1 != OK)
					break;
				res = tupleCmp(recR_ptr, recS2_ptr);
			}
			
			// If break our of the inner loop, move to a new record in S to restart the scanning
			// and record the postion in scanS2.
			scanS2 -> position(ridS1);
		}	
		
		// reclaim the memory.
		delete scanR;
		heapfileR->deleteFile();
		delete heapfileR;

		delete scanS1;
		heapfileS1->deleteFile();
		delete heapfileS1;

		delete scanS2;
		//heapfileS2->deleteFile();
		delete heapfileS2;	
}	

// sortMerge destructor
sortMerge::~sortMerge()
{
	
}
