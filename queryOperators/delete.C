//----------------------------------------
// Team members:
//      Jiaqi Cheng (9079840592), Skylar Hou (9084989582), Ziqi Liao (9084667097)
//
// This delete.C function will delete all tuples in relation 
// satisfying the predicate specified by attrName, op, and the constant attrValue. type 
// denotes the type of the attribute. 
//----------------------------------------

#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */


/**
 * This function will delete all tuples in relation satisfying the 
 * predicate specified by attrName, op, and the constant attrValue. 
 * type denotes the type of the attribute. Locate all the qualifying 
 * tuples using a filtered HeapFileScan.
 * @param relation  relation name
 * @param type type of attribute
 * @param attrName, op, attrValue condition componets
 * @return  status of the QU_Delete function
 */
const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const   char *attrValue)
{	
	Status status;
	// open current table (to be scanned) as a HeapFileScan object
	HeapFileScan Scan(relation, status);
    if (status != OK) { return status; }

	const char *attrValue_2;
	int new_val_1;
	float new_val_2;
	// check if an unconditional scan is required
	if (attrName.empty()){
		status = Scan.startScan(0,
                            0,
                            STRING,
                            NULL,
                            EQ);
    	if (status != OK) { return status; }
	}
	else{
		AttrDesc *attr = new AttrDesc();
    	status = attrCat->getInfo(relation, attrName, *attr);
    	if (status != OK){ return status; }
		
		switch(type) {
		case INTEGER:{
			new_val_1 = atoi(attrValue);
			attrValue_2 = (char *)&new_val_1;
			break;
		}
		case FLOAT:{
			new_val_2 = atof(attrValue);
			attrValue_2 = (char *)&new_val_2;
			break;
		}
		case STRING:{
			attrValue_2 = attrValue;
			break;
		}
		default:
			break;
		}

		status = Scan.startScan(attr->attrOffset,
                                attr->attrLen,
                                (Datatype) attr->attrType,
                                attrValue_2,
                                op);
        if (status != OK) { return status; }
	}
	// scan the current table
	RID recRID;
    Record Rec;
	while (Scan.scanNext(recRID) == OK)
    {	
		status = Scan.deleteRecord();
		if (status != OK) { return status; }
    } // end scan
	// part 6
	return OK;
}