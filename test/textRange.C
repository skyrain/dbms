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
			inSubset = true;
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
	
	//--- clear current range info ---
	walker = this->textRange;
	while(walker != NULL)
	{
		Range* tmp = walker->next;
		free(walker);
		walker = tmp;
	}

	//--- rebuild ---
	walker = this->textRange;
	listWalker = list;
	while(listWalker != NULL)
	{
		char* tmpStart = NULL;
		char* tmpEnd = NULL;
		bool startIn, endIn;
		//---1. find min start ---
		bool findStart = findNode(list, true, tmpStart, 
				startIn);			

		if(!findStart)
			break;
		//---2. find min end ---
		bool findEnd = findNode(list, false, tmpStart, 
				endIn);		

		if(!findEnd)
			break;

		//---3. build subset ---
		Range* tmp = (Range*)calloc(1, sizeof(Range));
		tmp->start = tmpStart;
		tmp->startIncluded = startIn;
		tmp->end = tmpEnd;
		tmp->endIncluded = endIn;
		tmp->next = NULL;
	
		walker = tmp;
		walker = walker->next;

		//-- check next ---
		listWalker = listWalker->next;
	}

	//--- merge overlap subset ---
	walker = this->textRange;
	Range* nextWalker = NULL;
	
	while(walker != NULL)
	{
		nextWalker = walker->next;

		//--- merge all the overlaps of later subsets ---
		//--- by updating end ---
		bool extend = true;
		while(extend && nextWalker!= NULL)
		{
			if(strcmp(nextWalker->start, walker->end) < 0 || 
				(strcmp(nextWalker->start, walker->end) == 0 &&
				 (nextWalker->startIncluded || walker->endIncluded)))
			{
				walker->end = nextWalker->end;
				walker->endIncluded = nextWalker->endIncluded;

				walker->next = nextWalker->next;
				free(nextWalker);
			}
			else
				extend = false;

			nextWalker = walker->next;
		}

		walker = walker->next;
	}
}

bool TextRange::findNode(Node* list, bool isStartNode, char* bound, 
		bool& included)
{
	Node* listWalker = list;
	char* boundary = NULL;
	bool findNode = false;

	while(listWalker != NULL)
	{
		if(listWalker->isStart == isStartNode)
		{
			if(boundary == NULL)
			{
				boundary = (char*)calloc(1, strlen(listWalker->boundary) 
						+ 1) ;
				strcpy(boundary, listWalker->boundary);
				included = listWalker->included;
				findNode = true;
			}
			else if((strcmp(listWalker->boundary, boundary) < 0) ||
					(strcmp(listWalker->boundary, boundary) == 0 &&
					listWalker->included))
			{
				free(boundary);
				boundary = (char*)calloc(1, strlen(listWalker->boundary) 
						+ 1) ;
				strcpy(boundary, listWalker->boundary);
				included = listWalker->included;
			}
		}

		listWalker = listWalker->next;
	}
	
	bound = boundary;

	//--- delete this boundary info from linkedlist ---
	listWalker = list;

	while(listWalker->next != NULL)
	{
		if(listWalker->next->isStart == isStartNode)
		{
			if(strcmp(listWalker->next->boundary, bound) == 0
					&& included == listWalker->next->included)
			{
				Node* freeNode = listWalker->next;
				free(freeNode);
				listWalker->next = listWalker->next->next;
			}
		}

		listWalker = listWalker->next;
	}
	
	if(strcmp(list->boundary, bound) == 0
			&& included == list->included)
	{
		Node* freeNode = list;
		list = list->next;
		free(freeNode);
	}

	return findNode;
}

//--- idea: similar as addition method ---
//--- 1. scan the range subsets of the two to build linked list --
//--- for boundaries info after the scan ---
//--- 2. use boundary info linked list to rebuild this text Range ---
void TextRange::deletion(TextRange* del)
{
}
