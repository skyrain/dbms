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

typedef struct Range{
	char* start;
	bool startIncluded;
	char* end;
	bool endIncluded;
	struct Range* next;

}Range;

typedef struct Node
{
	char* boundary;
	bool included;
	bool isStart;
	struct Node* next;
}Node;

public class TextRange{

	public:
	void addition(TextRange* add);
	void deletion(TextRange* del);
	bool testRange(char* s);

	Range* textRange;
}

#endif
