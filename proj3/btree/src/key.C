/*
 * key.C - implementation of <key,data> abstraction for BT*Page and 
 *         BTreeFile code.
 *
 * Gideon Glass & Johannes Gehrke  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include <string.h>
#include <assert.h>

#include "bt.h"

/*
 * See bt.h for more comments on the functions defined below.
 */

/*
 * Reminder: keyCompare compares two keys, key1 and key2
 * Return values:
 *   - key1  < key2 : negative
 *   - key1 == key2 : 0
 *   - key1  > key2 : positive
 */
int keyCompare(const void *key1, const void *key2, AttrType t)
{
        // when we need to compare two keys, we actually compare.
        // the integer key number in a keytype.

        // At first, cast the key1 and key2 to keytype.
        Keytype tmpkey1 = NULL;
        Keytype tmpkey2 = NULL;

        tmpkey1 = (Keytype *)key1;
        tmpkey2 = (Keytype *)key2;

	int result;
	result = 0;

	// different type with different comparing method.
        if(t == attrInteger){
		// minus the int key number.
		result = tmpkey1->intkey - tmpkey2->intkey;
                return result;
	}else if(t == attrString){
		// use string compare method to calculate the result.
		result = strncmp(tmpkey1->charkey, tmpkey2->charkey, MAX_KEY_SIZE1);
		return result;
	}else
		return result;
}

/*
 * make_entry: write a <key,data> pair to a blob of memory (*target) big
 * enough to hold it.  Return length of data in the blob via *pentry_len.
 *
 e Ensures that <data> part begins at an offset which is an even 
 * multiple of sizeof(PageNo) for alignment purposes.
 */
void make_entry(KeyDataEntry *target,
                AttrType key_type, const void *key,
                nodetype ndtype, Datatype data,
                int *pentry_len)
{

}


/*
 * get_key_data: unpack a <key,data> pair into pointers to respective parts.
 * Needs a) memory chunk holding the pair (*psource) and, b) the length
 * of the data chunk (to calculate data start of the <data> part).
 */
void get_key_data(void *targetkey, Datatype *targetdata,
                  KeyDataEntry *psource, int entry_len, nodetype ndtype)
{
   // put your code here
   return;
}

/*
 * get_key_length: return key length in given key_type
 */
int get_key_length(const void *key, const AttrType key_type)
{
	int result = 0;
        // different type with different method.
        if(key_type == attrInteger){
                result = sizeof(int);
                return result;
        }else if(key_type == attrString){
		// size of string key need a \0 in the end
                result = strlen((char *)key) + 1;
                return result;
        }else
                return result;
}
 
/*
 * get_key_data_length: return (key+data) length in given key_type
 */   
int get_key_data_length(const void *key, const AttrType key_type, 
                        const nodetype ndtype)
{
	int result = 0;
        // different type with different method.
        if(ndtype == LEAF){
                result = sizeof(RID);
                return result;
        }else if(ndtype == INDEX){
                // size of string key need a \0 in the end
                result = sizeof(PageId);
                return result;
        }else
                return result;
}
