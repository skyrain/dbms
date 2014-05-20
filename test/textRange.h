#ifndef _TEXTRANGE_H
#define _TEXTRANGE_H

#ifndef False
#define False 0
#endif

#ifndef True
#define True !False
#endif

#ifndef TRUE
#define TRUE true
#endif

#ifndef FALSE
#define FALSE False
#endif

#ifndef NULL
#define NULL 0
#endif

#include <string.h>

//--- text range information holder ---
typedef struct Range{
	char* start;
	bool startIncluded;
	char* end;
	bool endIncluded;
	struct Range* next;

}Range;

//--- boundary information holder---
typedef struct Node
{
	char* boundary;
	bool included;
	bool isStart;
	bool isDelete;
	struct Node* next;
}Node;

class TextRange{

	public:
	void addition(TextRange* add);
	void deletion(TextRange* del);
	bool testRange(char* s);

	//--- info to hold textRanges ---
	Range* textRange;

	//--- assist functions ---
	bool findNode(Node* list, bool isStartNode, char* bound,
			bool& included);
};

#endif
