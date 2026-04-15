#include "heapfile.h"
#include "error.h"

static RID makeNullRid()
{
    RID rid;
    rid.pageNo = -1;
    rid.slotNo = -1;
    return rid;
}

// routine to create a heapfile
const Status createHeapFile(const string fileName)
{
    File* 		file;
    Status 		status;
    FileHdrPage*	hdrPage;
    int			hdrPageNo;
    int			newPageNo;
    Page*		newPage;
    Page*           pagePtr;

    // try to open the file. This should return an error
    status = db.openFile(fileName, file);
    if (status == OK)
    {
        db.closeFile(file);
        return FILEEXISTS;
    }

    // file doesn't exist. First create it and allocate
    // an empty header page and data page.
    status = db.createFile(fileName);
    if (status != OK) return status;

    status = db.openFile(fileName, file);
    if (status != OK) return status;

    status = bufMgr->allocPage(file, hdrPageNo, pagePtr);
    if (status != OK)
    {
        db.closeFile(file);
        return status;
    }

    hdrPage = (FileHdrPage*) pagePtr;
    strncpy(hdrPage->fileName, fileName.c_str(), MAXNAMESIZE - 1);
    hdrPage->fileName[MAXNAMESIZE - 1] = '\0';
    hdrPage->pageCnt = 1;
    hdrPage->recCnt = 0;

    status = bufMgr->allocPage(file, newPageNo, newPage);
    if (status != OK)
    {
        bufMgr->unPinPage(file, hdrPageNo, true);
        db.closeFile(file);
        return status;
    }

    newPage->init(newPageNo);
    hdrPage->firstPage = newPageNo;
    hdrPage->lastPage = newPageNo;

    status = bufMgr->unPinPage(file, newPageNo, true);
    if (status != OK)
    {
        bufMgr->unPinPage(file, hdrPageNo, true);
        db.closeFile(file);
        return status;
    }

    status = bufMgr->unPinPage(file, hdrPageNo, true);
    if (status != OK)
    {
        db.closeFile(file);
        return status;
    }

    return db.closeFile(file);
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
	return (db.destroyFile (fileName));
}

// constructor opens the underlying file
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status 	status;
    Page*	pagePtr;

    filePtr = NULL;
    headerPage = NULL;
    headerPageNo = -1;
    hdrDirtyFlag = false;
    curPage = NULL;
    curPageNo = -1;
    curDirtyFlag = false;
    curRec = makeNullRid();

    cout << "opening file " << fileName << endl;

    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {
        status = filePtr->getFirstPage(headerPageNo);
        if (status != OK)
        {
            returnStatus = status;
            db.closeFile(filePtr);
            filePtr = NULL;
            return;
        }

        status = bufMgr->readPage(filePtr, headerPageNo, pagePtr);
        if (status != OK)
        {
            returnStatus = status;
            db.closeFile(filePtr);
            filePtr = NULL;
            return;
        }

        headerPage = (FileHdrPage*) pagePtr;
        hdrDirtyFlag = false;

        curPageNo = headerPage->firstPage;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK)
        {
            bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
            headerPage = NULL;
            returnStatus = status;
            db.closeFile(filePtr);
            filePtr = NULL;
            return;
        }
        curDirtyFlag = false;
        curRec = makeNullRid();
        returnStatus = OK;
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

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
  return headerPage->recCnt;
}

// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter

const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
    Status status;

    // cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;
    if (rid.pageNo < 0 || rid.slotNo < 0) return BADRID;

    if (curPage == NULL || curPageNo != rid.pageNo)
    {
        if (curPage != NULL)
        {
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK) return status;
        }

        status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
        if (status != OK)
        {
            curPage = NULL;
            curPageNo = -1;
            curDirtyFlag = false;
            return status;
        }
        curPageNo = rid.pageNo;
        curDirtyFlag = false;
    }

    status = curPage->getRecord(rid, rec);
    if (status == OK) curRec = rid;
    return status;
}

// constructor for HeapFileScan
HeapFileScan::HeapFileScan(const string & name,
			   Status & status) : HeapFile(name, status)
{
    filter = NULL;
    markedPageNo = curPageNo;
    markedRec = curRec;
}

// start a scan
const Status HeapFileScan::startScan(const int offset_,
				     const int length_,
				     const Datatype type_, 
				     const char* filter_,
				     const Operator op_)
{
    Status status;

    if (!filter_) {                        // no filtering requested
        filter = NULL;
        curRec = makeNullRid();
        if (curPage == NULL)
        {
            curPageNo = headerPage->firstPage;
            status = bufMgr->readPage(filePtr, curPageNo, curPage);
            if (status != OK) return status;
            curDirtyFlag = false;
        }
        else if (curPageNo != headerPage->firstPage)
        {
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK) return status;
            curPageNo = headerPage->firstPage;
            status = bufMgr->readPage(filePtr, curPageNo, curPage);
            if (status != OK) return status;
            curDirtyFlag = false;
        }
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
    curRec = makeNullRid();

    if (curPage == NULL)
    {
        curPageNo = headerPage->firstPage;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) return status;
        curDirtyFlag = false;
    }
    else if (curPageNo != headerPage->firstPage)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK) return status;
        curPageNo = headerPage->firstPage;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) return status;
        curDirtyFlag = false;
    }

    return OK;
}

// end a scan
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

// destructor for HeapFileScan
HeapFileScan::~HeapFileScan()
{
    endScan();
}

// save current position of scan
const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

// reset scan to last marked location
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
        if (curPageNo <= 0)
        {
            curPage = NULL;
            curDirtyFlag = false;
            return OK;
        }
		// then read the page
		status = bufMgr->readPage(filePtr, curPageNo, curPage);
		if (status != OK) return status;
		curDirtyFlag = false; // it will be clean
    }
    else curRec = markedRec;
    return OK;
}

// return RID of next record satisfying scan
const Status HeapFileScan::scanNext(RID& outRid)
{
    Status 	status = OK;
    RID		nextRid;
    RID		tmpRid;
    int 	nextPageNo;
    Record      rec;

    if (curPage == NULL)
    {
        curPageNo = headerPage->firstPage;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) return status;
        curDirtyFlag = false;
        curRec = makeNullRid();
    }

    while (true)
    {
        if (curRec.pageNo != curPageNo || curRec.slotNo < 0)
            status = curPage->firstRecord(tmpRid);
        else
            status = curPage->nextRecord(curRec, tmpRid);

        while (status == OK)
        {
            status = curPage->getRecord(tmpRid, rec);
            if (status != OK) return status;
            if (matchRec(rec))
            {
                curRec = tmpRid;
                outRid = curRec;
                return OK;
            }
            status = curPage->nextRecord(tmpRid, nextRid);
            tmpRid = nextRid;
        }

        if (status != ENDOFPAGE && status != NORECORDS)
            return status;

        status = curPage->getNextPage(nextPageNo);
        if (status != OK) return status;
        if (nextPageNo == -1)
            return FILEEOF;

        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK) return status;

        curPageNo = nextPageNo;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK)
        {
            curPage = NULL;
            curDirtyFlag = false;
            return status;
        }
        curDirtyFlag = false;
        curRec = makeNullRid();
    }
}


// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page 

const Status HeapFileScan::getRecord(Record & rec)
{
    if (curPage == NULL || curRec.pageNo != curPageNo) return BADPAGENO;
    return curPage->getRecord(curRec, rec);
}

// delete record from file. 
const Status HeapFileScan::deleteRecord()
{
    Status status;

    if (curPage == NULL || curRec.pageNo != curPageNo) return BADPAGENO;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    if (status != OK) return status;
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

// check if record matches the scan filter
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

// destructor for InsertFileScan
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

// Insert a record into the file
const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
    Page*	newPage;
    int		newPageNo;
    Status	status, unpinstatus;
    RID		rid;

    // check for very large records
    if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
    {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }

    if (curPage == NULL)
    {
        curPageNo = headerPage->lastPage;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) return status;
        curDirtyFlag = false;
    }

    status = curPage->insertRecord(rec, rid);
    if (status == NOSPACE)
    {
        status = bufMgr->allocPage(filePtr, newPageNo, newPage);
        if (status != OK) return status;

        newPage->init(newPageNo);
        status = curPage->setNextPage(newPageNo);
        if (status != OK)
        {
            bufMgr->unPinPage(filePtr, newPageNo, true);
            return status;
        }
        curDirtyFlag = true;

        unpinstatus = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (unpinstatus != OK)
        {
            bufMgr->unPinPage(filePtr, newPageNo, true);
            return unpinstatus;
        }

        curPage = newPage;
        curPageNo = newPageNo;
        curDirtyFlag = true;
        headerPage->lastPage = newPageNo;
        headerPage->pageCnt++;
        hdrDirtyFlag = true;

        status = curPage->insertRecord(rec, rid);
    }

    if (status != OK) return status;

    headerPage->recCnt++;
    hdrDirtyFlag = true;
    curDirtyFlag = true;
    outRid = rid;
    curRec = rid;
    return OK;
}
