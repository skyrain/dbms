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
	Keytype *tmpkey1 = NULL;
	Keytype *tmpkey2 = NULL;

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
	}
	// other type ???
	
	// non of this type:
	cout << "AttrType unknown: " << t << endl;
	return 0;
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
	// entry_len =  key_len + data_len;
	int key_len = 0;
	int data_len = 0;
	// Final length of the entry.
	*pentry_len = get_key_data_length(key, key_type, ndtype);
	key_len = get_key_length(key, key_type);
	data_len = *pentry_len - key_len;

	// write a key into memory chunk of target.
	if(key_type == attrInteger)
		memcpy(target, key, key_len);
	else if(key_type == attrString)
		// memcpy ???
		strcpy((char *)target, (char *)key);
	else //other type ???
		cout << " KeyType unknown: " << key_type << endl;

	// write data into memory chunk of targe
	memcpy(target + key_len, &data, data_len);
}


/*
 * get_key_data: unpack a <key,data> pair into pointers to respective parts.
 * Needs a) memory chunk holding the pair (*psource) and, b) the length
 * of the data chunk (to calculate data start of the <data> part).
 */
void get_key_data(void *targetkey, Datatype *targetdata,
                  KeyDataEntry *psource, int entry_len, nodetype ndtype)
{
	// We need to slit the KeyDataEntry, and give the value to targetKey
	// and targetdata based on the nodetype and the given entry_length.
	
	if(targetkey == NULL && targetdata == NULL)
		return;
	
	// entry_len =  key_len + data_len;
	int key_len = 0;
	int data_len = 0;
	
	if(ndtype == LEAF){
		data_len = sizeof(RID);
		key_len = entry_len - data_len;
	}
	else if(ndtype == INDEX){
		data_len = sizeof(PageId);
		key_len = entry_len - data_len;
	}
	// other type ???
	
	// unpack the data into memory chunk.
	// save the key into targetKey
	if(targetkey != NULL)
		memcpy(targetkey, psource, key_len);
	
	// sava the data into targetdata
	if(targetdata != NULL)
		memcpy(targetkey, psource + key_len, data_len);
}

/*
 * get_key_length: return key length in given key_type
 */
int get_key_length(const void *key, const AttrType key_type)
{
	int key_len = 0;
        // different type with different method.
        if(key_type == attrInteger){
                key_len = sizeof(int);
                return key_len;
        }else if(key_type == attrString){
		// size of string key need a \0 in the end
                key_len = strlen((char *)key) + 1;
                return key_len;
	}
	// other type ???
	return key_len;
}
 
/*
 * get_key_data_length: return (key+data) length in given key_type
 */   
int get_key_data_length(const void *key, const AttrType key_type, 
                        const nodetype ndtype)
{
	int key_len = 0;
	int data_len = 0;
	int key_data_len = 0;

        // different type with different method.
        if(ndtype == LEAF)
                data_len = sizeof(RID);
        else if(ndtype == INDEX)
                data_len = sizeof(PageId);
	// other type ???
	
	// Call get_key_length to get the length of the key.
	key_len = get_key_length(key, key_type);
	key_data_len = key_len + data_len;
	
	return key_data_len;	
}
