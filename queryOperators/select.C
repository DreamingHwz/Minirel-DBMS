//----------------------------------------
// Team members:
//      Jiaqi Cheng (9079840592), Skylar Hou (9084989582), Ziqi Liao (9084667097)
//
// This select.C file implements the select funtion.
// A selection is implemented using a filtered HeapFileScan.  
// The result of the selection is stored in the result relation called result.
//----------------------------------------

#include "catalog.h"
#include "query.h"


// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

/**
 * Select records satisfied with specific conditions.
 * @param result  a  heapfile with this name will be created 
 * by the parser before QU_Select() is called
 * @param projCnt amount of columns
 * @param projNames attribute information list
 * @param attr, op, attrValue condition
 * @return  status of the QU_Select function
 */
const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[],
		       const attrInfo *attr, 
		       const Operator op, 
		       const char *attrValue)
{
   // Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;
	Status status;
	
	int reclen = 0;
	AttrDesc projNamesArray[projCnt];
    for (int i = 0; i < projCnt; i++)
    {
        status = attrCat->getInfo(projNames[i].relName,
                                         projNames[i].attrName,
                                         projNamesArray[i]);
        if (status != OK){ return status;}

		reclen += projNamesArray[i].attrLen;
    }
	AttrDesc *attrDesc = new AttrDesc();
    if (attr != NULL){
    status = attrCat->getInfo(attr->relName,
                                     attr->attrName,
                                     *attrDesc);
    if (status != OK){ return status; }
    }
    
    int type = attrDesc->attrType;

    const char *attrValue_2;
    int new_val_1;
	float new_val_2;

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

	
	return ScanSelect(result, projCnt, projNamesArray, attrDesc, op, attrValue_2, reclen);//////
}


/**
 * Select records satisfied with specific conditions.
 * @param similar to above function
 */
const Status ScanSelect(const string & result, 
#include "stdio.h"
#include "stdlib.h"
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;
	Status status;
	int resultTupCnt = 0;

	// have a temporary record for output table
	char outputData[reclen];
    Record outputRec;
    outputRec.data = (void *) outputData;
    outputRec.length = reclen;

	// open "result" as an InsertFileScan object
	InsertFileScan resultRel(result, status);
    if (status != OK) { return status; }	

	// open current table (to be scanned) as a HeapFileScan object
	HeapFileScan Scan(string(projNames->relName), status);
    if (status != OK) { return status; }

	// check if an unconditional scan is required
	if (attrDesc == NULL){
		status = Scan.startScan(0,
                            0,
                            STRING,
                            NULL,
                            EQ);
    	if (status != OK) { return status; }
	}
	else{
		status = Scan.startScan(attrDesc->attrOffset,
                                attrDesc->attrLen,
                                (Datatype) attrDesc->attrType,
                                filter,
                                op);
        if (status != OK) { return status; }
	}

	// scan the current table
	RID recRID;
    Record Rec;
	while (Scan.scanNext(recRID) == OK)
    {
        status = Scan.getRecord(Rec);
        ASSERT(status == OK);

        // we have a match, copy data into the output record
        int outputOffset = 0;
        for (int i = 0; i < projCnt; i++)
        {
            // if find a record, then copy stuff over to the temporary record (memcpy)
            memcpy(outputData + outputOffset, (char *)Rec.data + projNames[i].attrOffset,
                           projNames[i].attrLen);
            outputOffset += projNames[i].attrLen;
        } 
        // insert into the output table
        RID outRID;
        status = resultRel.insertRecord(outputRec, outRID);
        ASSERT(status == OK);
        resultTupCnt++;
    } // end scan
    // printf("tuple selection produced %d result tuples \n", resultTupCnt);
    status = Scan.endScan();
    return status;
}


