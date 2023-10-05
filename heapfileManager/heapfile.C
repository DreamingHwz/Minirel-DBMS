//----------------------------------------
// Team members:
//      Jiaqi Cheng (9079840592), Skylar Hou (9084989582), Ziqi Liao (9084667097)
//
// This heapfile.c implement a File Manager for Heap Files
// it also provides a scan mechanism to search a heap file for records that satisfy a search predicate called a filter
//----------------------------------------
#include "heapfile.h"
#include "error.h"
/**
 * The HeapFile layer implements three main classes: HeapFile, HeapFileScan, InsertFileScan
 */
// routine to create a heapfile
/**
 * creates an empty heap file
 * @param fileName  name of file
 * @return  status of the createHeapFile function
 */
const Status createHeapFile(const string fileName)
{
    File* 		file;
    Status 		status;
    FileHdrPage*	hdrPage;
    int			hdrPageNo;
    int			newPageNo;
    Page*		newPage;

    // try to open the file. This should return an error
    status = db.openFile(fileName, file);
    if (status != OK)
    {
		// file doesn't exist. First create it and allocate
		// an empty header page and data page.
		
        //create a file named fileName and open the file
        if ((status = db.createFile(fileName)) != OK)
            return status;
        if ((status = db.openFile(fileName, file)) != OK)
            return status;

        //initialize the header page
        if ((status = bufMgr->allocPage(file, hdrPageNo, newPage)) != OK)
            return status;
        hdrPage = (FileHdrPage *) newPage;

        int i;
        for (i = 0; fileName[i] != '\0'; i++){
            hdrPage->fileName[i] = fileName[i];
        }
        hdrPage->fileName[i] = '\0';

        //initialize the first data page
        if ((status = bufMgr->allocPage(file, newPageNo, newPage)) != OK)
            return status;
        newPage->init(newPageNo);

        //change the header page after alloc first page
        hdrPage->firstPage = newPageNo;
        hdrPage->lastPage = newPageNo;
        hdrPage->pageCnt = 1;
        hdrPage->recCnt = 0;

        //unpin and flush
        if ((status = bufMgr->unPinPage(file, hdrPageNo, true)) != OK)
            return status;
        if ((status = bufMgr->unPinPage(file, newPageNo, true)) != OK)
            return status;
        if ((status = bufMgr->flushFile(file)) != OK)
            return status;

        //close the file
        if ((status = db.closeFile(file)) != OK)
            return status;

        return status;
    }
    return (FILEEXISTS);
}
/**
 * closed all instances of the file before calling this function.
 * @param fileName  name of file
 * @return status of the function
 */
// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
	return (db.destroyFile (fileName));
}

/**
 * This is the constructor of the HeapFile.
 * @param fileName  name of file
 * @param returnStatus store the return status
 */
// constructor opens the underlying file
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status 	status;
    Page*	pagePtr;

    cout << "opening file " << fileName << endl;

    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {
        //read and pin the header page
        if ((status = filePtr->getFirstPage(headerPageNo)) != OK){
            returnStatus = status;
            return;
        }
        if ((status = bufMgr->readPage(filePtr, headerPageNo, pagePtr)) != OK){
            returnStatus = status;
            return;
        }
        //Take the Page* pointer returned from allocPage() and cast it to a FileHdrPage*
        headerPage = (FileHdrPage *) pagePtr;
        hdrDirtyFlag = false;

        //read and pin the first data page
        curPageNo = headerPage->firstPage;
        if ((status = bufMgr->readPage(filePtr, curPageNo, curPage)) != OK){
            returnStatus = status;
            return;
        }
        curDirtyFlag = false;
        curRec = NULLRID;
        returnStatus = status;
    }
    else
    {
    	cerr << "open of heap file failed\n";
		returnStatus = status;
		return;
    }
}

// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it 
    if (curPage != NULL)
    {
    	status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
		curPage = NULL;
		curPageNo = 0;
		curDirtyFlag = false;
		if (status != OK) cerr << "error in unpin of date page\n";
    }
	
	 // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK) cerr << "error in unpin of header page\n";
	
	// status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
	// if (status != OK) cerr << "error in flushFile call\n";
	// before close the file
	status = db.closeFile(filePtr);
    if (status != OK)
    {
		cerr << "error in closefile call\n";
		Error e;
		e.print (status);
    }
}


/**
 * get the number of records currently in the file
 * @return Returns the number of records currently in the file
 */
const int HeapFile::getRecCnt() const
{
  return headerPage->recCnt;
}

// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter
/**
 *  This method returns a record (via the rec structure) given the RID of the record.
 * @param rid rid parameter indicate a record
 * @param rec returns the length and a pointer to the record with RID rid.
 * @return return the status of getRecord function
 */
const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
    Status status;

    // cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;
    
    //curPage is the right page of the record
    if (curPage != NULL) {
        if (curPageNo == rid.pageNo) {
            status = curPage->getRecord(rid, rec);
            return status;
        }
    }

    //curPage == NULL or not the right page
    if ((status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag)) != OK)
        return status;
    curPageNo = rid.pageNo;
    if ((status = bufMgr->readPage(filePtr, curPageNo, curPage)) != OK)
        return status;
    curDirtyFlag = false;
    status = curPage->getRecord(rid, rec);
    return status;
}
/**
 * This HeapFileScan class (derived from the HeapFile class) is for scanning a heap file.
 * @param name
 * @param status
 */
HeapFileScan::HeapFileScan(const string & name,
			   Status & status) : HeapFile(name, status)
{
    filter = NULL;
}

/**
 * This method initiates a scan over a file.
 * @param offset_ byte offset of filter attribute
 * @param length_ length of filter attribute
 * @param type_ datatype of filter attribute
 * @param filter_ comparison value of filter
 * @param op_ comparison operator of filter
 * @return
 */
const Status HeapFileScan::startScan(const int offset_,
				     const int length_,
				     const Datatype type_, 
				     const char* filter_,
				     const Operator op_)
{
    if (!filter_) {                        // no filtering requested
        filter = NULL;
        return OK;
    }
    
    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int)
         || type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}

/**
 * This method terminates a scan over a file but does not delete the scan object.
 * @return  status or OK message
 */
const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
		curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}
/**
 * Resets the position of the scan to the position when the scan was last marked.
 * @return status
 */
const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo) 
    {
		if (curPage != NULL)
		{
			status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
			if (status != OK) return status;
		}
		// restore curPageNo and curRec values
		curPageNo = markedPageNo;
		curRec = markedRec;
		// then read the page
		status = bufMgr->readPage(filePtr, curPageNo, curPage);
		if (status != OK) return status;
		curDirtyFlag = false; // it will be clean
    }
    else curRec = markedRec;
    return OK;
}

/**
 * Scan the file one page at a time, in each page get the next record one by one.
 * if the current page is null or it is the last record of this page, move to the next page.
 * Repeat this until reach the end of the file.
 * @param outRid the RID of the record that satisfies the scan predicate.
 * @return the status of the function scanNext
 */
const Status HeapFileScan::scanNext(RID& outRid)
{
    Status 	status = OK;
    RID		nextRid;
    RID		tmpRid;
    int 	nextPageNo;
    Record      rec;


    while (true){
        //if curPage == NULL, check the first page's first record
        if (curPage == NULL){   
            curPageNo = headerPage->firstPage; 
            if ((status = bufMgr->readPage(filePtr, curPageNo, curPage)) != OK)
                return status;
            if ((status = curPage->firstRecord(curRec)) != OK){
                //no records on the page
                bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
                return FILEEOF;
            } 
            if ((status = curPage->getRecord(curRec, rec)) != OK)
                return status;            
        }
        //check the next record
        else {
            if ((status = curPage->nextRecord(curRec, nextRid)) != OK) {
                //reach the end of the page
                curPage->getNextPage(nextPageNo);

                //if it is the last page of the file
                if (nextPageNo == -1){
                    bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
                    return FILEEOF;
                }
                
                //if the page is blank and it is not the last page, get the next page
                do {
                    if ((status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag)) != OK)
                        ;//return status;
                    curPageNo = nextPageNo;
                    curDirtyFlag = false;
                    if ((status = bufMgr->readPage(filePtr, curPageNo, curPage)) != OK)
                        return status;
                    curPage->getNextPage(nextPageNo);
                } while ((status = curPage->firstRecord(curRec)) != OK && nextPageNo != -1);

                //if it is the last page and it is blank
                if (nextPageNo == -1 && (status = curPage->firstRecord(curRec)) != OK){
                    bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
                    return FILEEOF;
                }
                
            }
            else curRec = nextRid;

            if ((status = curPage->getRecord(curRec, rec)) != OK) 
                return status;
        }

        if (matchRec(rec) == true){
            outRid = curRec;
            return status;
        }
    }	
	
}


// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page 

const Status HeapFileScan::getRecord(Record & rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete record from file. 
const Status HeapFileScan::deleteRecord()
{
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true; 
    return status;
}


// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record & rec) const
{
    // no filtering requested
    if (!filter) return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length -1 ) >= rec.length)
	return false;

    float diff = 0;                       // < 0 if attr < fltr
    switch(type) {

    case INTEGER:
        int iattr, ifltr;                 // word-alignment problem possible
        memcpy(&iattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ifltr,
               filter,
               length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr;               // word-alignment problem possible
        memcpy(&fattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ffltr,
               filter,
               length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset,
                       filter,
                       length);
        break;
    }

    switch(op) {
    case LT:  if (diff < 0.0) return true; break;
    case LTE: if (diff <= 0.0) return true; break;
    case EQ:  if (diff == 0.0) return true; break;
    case GTE: if (diff >= 0.0) return true; break;
    case GT:  if (diff > 0.0) return true; break;
    case NE:  if (diff != 0.0) return true; break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string & name,
                               Status & status) : HeapFile(name, status)
{
  //Do nothing. Heapfile constructor will bread the header page and the first
  // data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK) cerr << "error in unpin of data page\n";
    }
}

/**
 * Insert a record into the file
 * @param rec a record (rec) of the page
 * @param outRid a return parameter to store the RID of the inserted record
 * @return the status of the insertRecord function
 */
const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
    Page* newPage;
    int  newPageNo;
    Status status, unpinstatus;
    RID  rid;

    // check for very large records
    if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
    {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }
    if (curPage == NULL){
        //find the last page
        curPageNo = headerPage->lastPage;
        bufMgr->readPage(filePtr, curPageNo, curPage);
        curDirtyFlag = false;
    }
    if ((status = curPage->insertRecord(rec, rid)) != OK){
        //curPage is full, allocate new one
        if ((status = bufMgr->allocPage(filePtr, newPageNo, newPage)) != OK)
            return status;
        newPage->init(newPageNo);
        newPage->setNextPage(-1);
        
        //modify the header page
        headerPage->lastPage = newPageNo;
        headerPage->pageCnt++;
        hdrDirtyFlag = true;
        
        //link up new page
        curPage->setNextPage(newPageNo);
        
        //make curPage the new one
        if ((unpinstatus = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag)) != OK)
            return unpinstatus;
        curPageNo = newPageNo;
        curPage = newPage;

        //insert the record
        if ((status = curPage->insertRecord(rec, rid)) != OK)
            return status;
    }
    curDirtyFlag = true;
    headerPage->recCnt++;
    outRid = rid;
    return OK;
}
