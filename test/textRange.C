#include <iostream>
#include <stdlib.h>

#include "textRange.h"

bool TextRange::testRange(char* s)
{
	//for each subset in the text Range, we just check it ---
	Range* walker = this->textRange;
	bool inSubset = false;

	while(walker != NULL)
	{
		bool isIn = true;
		//--- check whether s is before start of subset ---
		if((strcmp(walker->start, s) > 0 && walker->startIncluded) ||
			(strcmp(walker->start, s) >= 0 && !walker->startIncluded))
			isIn = false;
		//--- check whether s is after end of subset ---
		if((strcmp(walker->end, s) < 0 && walker->endIncluded) ||
			(strcmp(walker->end, s) <= 0 && !walker->endIncluded))
			isIn = false;
		
		if(isIn)
		{
			inSubSet = true;
			break;
		}
		walker = walker->next;
	}

	return inSubset;
}

//--- idea: use a linkedlist to record two textRanges' boundary
//--- infomation ---
//--- then according to the linkedlist to rebuild the result text range ---
void TextRange::addition(TextRange* add)
{
	Range* addWalker = add->textRange;
	Range* walker = this->textRange;	
	//-- build a linkedList to record the boundaries ---
	Node* list = NULL;
	Node* listWalker = list;
	while(walker != NULL)
	{
		Node* bStart = (Node*)calloc(1, sizeof(Node));
		bStart->boundary = walker->start;
		bStart->included = walker->startIncluded;
		bStart->isStart = true;
		bStart->next = NULL;
		
		if(list == NULL)
		{
			list = bStart;
			listWalker = list;
		}
		else
		{
			listWalker->next = bStart;
			listWalker = listWalker->next;
		}
		
		Node* bEnd = (Node*)calloc(1, sizeof(Node));
		bEnd->boundary = walker->end;
		bEnd->included = walker->endIncluded;
		bEnd->isStart = false;
		bEnd->next = NULL;
		
		listWalker->next = bEnd;
		listWalker = listWalker->next;


		//--- check next subset ---
		walker = walker->next;
	}

	//--- insert addWalker info into the linkedlist for boudnary ---
	while(addWalker != NULL)
	{
		Node* bStart = (Node*)calloc(1, sizeof(Node));
		bStart->boundary = addWalker->start;
		bStart->included = addWalker->startIncluded;
		bStart->isStart = true;
		bStart->next = NULL;

		if(list == NULL)
		{
			list = bStart;
			listWalker = list;
		}
		else
		{
			listWalker->next = bStart;
			listWalker = listWalker->next;
		}

		Node* bEnd = (Node*)calloc(1, sizeof(Node));
		bEnd->boundary = addWalker->end;
		bEnd->included = addWalker->endIncluded;
		bEnd->isStart = false;
		bEnd->next = NULL;

		listWalker->next = bEnd;
		listWalker = listWalker->next;


		//--- check next subset ---
		addWalker = addWalker->next;				
	}

	//--- use the boundary info to rebuild the added resut range ---
	listWalker = list;
	while(listWalker != NULL)
	{
		//---1. find min start ---
		
		//---2. find min end ---

		//---3. build subset ---

		//---4. each time find min start need to check whether
		//--- it is in the previous subset,if so merge ---
	}
}
