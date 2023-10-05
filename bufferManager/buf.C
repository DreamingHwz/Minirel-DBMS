//----------------------------------------
// Team members: 
//      Ziqi Liao (9084667097), Jiaqi Cheng (9079840592), Skylar Hou (9084989582)
// 
// This buf.c implements of method of a buffer manager. 
// Allocates an array for the buffer pool with bufs page frames and a corresponding BufDesc table. 
// The way things are set up all frames will be in the clear state when the buffer pool is allocated.
//----------------------------------------

#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}

/**
 * @brief Allocates a free frame using the clock algorithm; if necessary, writing a dirty page back to disk.  
 * 
 * @param frame  used to hold page that have been read from disk into memory
 * @return Returns BUFFEREXCEEDED if all buffer frames are pinned, UNIXERR if the call to the I/O layer returned an error 
 * when a dirty page was being written to disk and OK otherwise. 
 */
const Status BufMgr::allocBuf(int & frame) 
{   
    Status status = OK;
    int start = clockHand;
    int round = 0;
    //Find a free frame using the clock algorithm
    while(1){
        advanceClock();     
        if (clockHand == start){
            round ++;
            if (round == 2)
                return BUFFEREXCEEDED;
        } 
        // the frame don't have page 
        if (bufTable[clockHand].valid == false){
            break;
        }
        //unset the refbit
        if (bufTable[clockHand].refbit == true){
            bufTable[clockHand].refbit = false;
            continue;
        }
        //replace the current page
        if (bufTable[clockHand].pinCnt == 0){
            if (bufTable[clockHand].dirty == true){  
                if ((status = bufTable[clockHand].file->writePage(bufTable[clockHand].pageNo,   //write the new page
	    		    		    &(bufPool[clockHand]))) != OK)
	                return status;
            }
            if ((status = hashTable->remove(bufTable[clockHand].file, bufTable[clockHand].pageNo)) != OK)  //flush the page
                return status;
            bufTable[clockHand].Clear();
            break;
        }  
    }
    frame = clockHand;
    return status;
}

/**
 * @brief Reads page pageNo from disk into the memory address specified by pagePtr. 
 * 
 * @param file pointer a file object
 * @param PageNo page number within a file
 * @param page page within file
 * @return * const Status Returns OK if no errors occurred, 
 * BADPAGENO if pageNo is not a valid page, BADPAGEPTR if pagePtr is not a valid address and UNIXERR if there is a Unix error.
 */
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    //Check whether the page is already in the buffer pool.
    Status status = OK;
    int frameNo = 0;

    //Invoking the lookup() method on the hashtable to get a frame number
    status = hashTable->lookup(file, PageNo, frameNo);

    //If page is in the buffer pool
    if (status ==  OK){
        bufTable[frameNo].refbit = true;
        bufTable[frameNo].pinCnt ++;
        page = &(bufPool[frameNo]);
    }
    //Page is not in the buffer pool
    else{
        if ((status = allocBuf(frameNo)) != OK)
            return status;
        if ((status = file->readPage(PageNo, &(bufPool[frameNo]))) != OK)
            return status;
        if ((status = hashTable->insert(file, PageNo, frameNo)) != OK)
            return status;
    
        bufTable[frameNo].Set(file, PageNo);
        page = &(bufPool[frameNo]);
    }
    return status;
}

/**
 * @brief Decrements the pinCnt of the frame containing (file, PageNo).
 * 
 * @param file pointer a file object
 * @param PageNo page number within a file
 * @param dirty true if dirty;  false otherwise
 * @return const Status Returns OK if no errors occurred, 
 * HASHNOTFOUND if the page is not in the buffer pool hash table, 
 * PAGENOTPINNED if the pin count is already 0.
 */
const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{
    Status status = OK;
    int frameNo = 0;

    //find the frameNo in HashTable
    status = hashTable->lookup(file, PageNo, frameNo);
    if (status ==  OK){
        if (bufTable[frameNo].pinCnt == 0)
            return PAGENOTPINNED;
        bufTable[frameNo].pinCnt --;

        // if dirty == true, sets the dirty bit. 
        if (dirty == true)
            bufTable[frameNo].dirty = true;
    }
    return status;
}

/**
 * @brief Allocate an empty page in the specified file by invoking the file->allocatePage() method
 * 
 * @param file pointer a file object
 * @param pageNo page number within a file
 * @param page page within file
 * @return const Status Returns OK if no errors occurred, UNIXERR if a Unix error occurred, 
 * BUFFEREXCEEDED if all buffer frames are pinned and HASHTBLERROR if a hash table error occurred. 
 */
const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
    Status status = OK;
    int frameNo;
    
    //allocatePage fail
    if ((status = file->allocatePage(pageNo)) != OK)
        return status;
    
    //allocBuf fail
    if ((status = allocBuf(frameNo)) != OK)
        return status;

    //hash table error occur
    if ((status = hashTable->insert(file, pageNo, frameNo)) != OK)
        return status;

    //an entry is inserted into the hashTable
    bufTable[frameNo].Set(file, pageNo);

    page = &(bufPool[frameNo]);
    return status;
}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	    return status;

	    tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


