//----------------------------------------
// Team members:
//      Jiaqi Cheng (9079840592), Skylar Hou (9084989582), Ziqi Liao (9084667097)
//
// This insert.C implements the function of inserting a tuple with 
// the given attribute values (in attrList) in relation.
//----------------------------------------

#include "catalog.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

/**
 * Insert the record saved in attrlist[] with attrCnt of attributes
 * to the relation.
 * @param relation  name of relation
 * @param attrCnt attribute amount of the record to insert
 * @param attrList save the attribute value
 * @return  status of the QU_Insert function
 */
const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{
	Status status;
	Record rec;
	RID outRid;
	unsigned int reclen = 0;

	// open current table (to be scanned) as a HeapFileScan object
	InsertFileScan resultRel(relation, status);
    if (status != OK) { return status; }	

	// get all attribute info in relation
	int relattrCnt;
	AttrDesc *attrDesc;
    status = attrCat->getRelInfo(relation, relattrCnt, attrDesc);
    if (status != OK){ return status; }

	// Make sure that attrCnt corresponds the relation attribute count
	if (attrCnt != relattrCnt) return OK;
	
	for (int i = 0; i < attrCnt; i ++){
		reclen += attrDesc[i].attrLen;
	}
	rec.data = (void *)malloc(reclen * attrCnt);
	rec.length = 0;

	// copy attrList to rec.data
	int dataOffset = 0;
	for (int i = 0; i < relattrCnt; i++){
		for (int j = 0; j < attrCnt; j++){
			if (attrList[j].attrValue == NULL)
				return BADCATPARM;	//TODO

			// find the same attribute, add to rec.data
			if (strcmp(attrDesc[i].attrName, attrList[j].attrName) == 0){
				
				int type = attrList[j].attrType;
				const char *attrValue_2;
				int new_val_1;
				float new_val_2;
				
				switch(type) {
				case INTEGER:{
					new_val_1 = atoi((char*)attrList[j].attrValue);
					attrValue_2 = (char *)&new_val_1;
					break;
				}
				case FLOAT:{
					new_val_2 = atof((char*)attrList[j].attrValue);
					attrValue_2 = (char *)&new_val_2;
					break;
				}
				case STRING:{
					attrValue_2 = (char*)attrList[j].attrValue;
					break;
				}
				default:
					break;
				}
				memcpy((char*)rec.data + attrDesc[i].attrOffset, attrValue_2, attrDesc[i].attrLen);
				rec.length += attrDesc[i].attrLen;
				break;
			}
		}
	}
	// insert the record to relation
	resultRel.insertRecord(rec, outRid);
	free(rec.data);
	return OK;
}