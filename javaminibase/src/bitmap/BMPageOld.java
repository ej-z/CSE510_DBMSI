/* File BMPage.java */

package bitmap;

import diskmgr.Page;
import global.Convert;
import global.GlobalConst;
import global.PageId;
import global.RID;
import heap.InvalidSlotNumberException;
import heap.Tuple;

import java.io.IOException;

/**
 * Class bitmap file page.
 * The design assumes that records are kept compacted when
 * deletions are performed.
 */

interface ConstSlot {
    int INVALID_SLOT = -1;
    int EMPTY_SLOT = -1;
}

public class BMPageOld extends Page
        implements ConstSlot, GlobalConst {


    public static final int SIZE_OF_SLOT = 4;
    public static final int DPFIXED = 4 * 2 + 3 * 4;

    public static final int SLOT_CNT = 0;
    public static final int USED_PTR = 2;
    public static final int FREE_SPACE = 4;
    public static final int TYPE = 6;
    public static final int PREV_PAGE = 8;
    public static final int NEXT_PAGE = 12;
    public static final int CUR_PAGE = 16;
    /**
     * page number of this page
     */
    protected PageId curPage = new PageId();
    /**
     * number of slots in use
     */
    private short slotCnt;
    /**
     * offset of first used byte by data records in data[]
     */
    private short usedPtr;
    /**
     * number of bytes free in data[]
     */
    private short freeSpace;
    /**
     * backward pointer to data page
     */
    private PageId prevPage = new PageId();
    /**
     * forward pointer to data page
     */
    private PageId nextPage = new PageId();

    /**
     * Default constructor
     */

    public BMPageOld() {
    }

    /**
     * Constructor of class HFPage
     * open a HFPage and make this HFpage piont to the given page
     *
     * @param page the given page in Page type
     */

    public BMPageOld(Page page) {
        data = page.getpage();
    }

    /**
     * returns the amount of available space on the page.
     *
     * @return the amount of available space on the page
     * @throws IOException I/O errors
     */
    public int available_space()
            throws IOException {
        freeSpace = Convert.getShortValue(FREE_SPACE, data);
        return (freeSpace - SIZE_OF_SLOT);
    }

    /**
     * Dump contents of a page
     *
     * @throws IOException I/O errors
     */
    public void dumpPage()
            throws IOException {
        int i, n;
        int length, offset;

        curPage.pid = Convert.getIntValue(CUR_PAGE, data);
        nextPage.pid = Convert.getIntValue(NEXT_PAGE, data);
        usedPtr = Convert.getShortValue(USED_PTR, data);
        freeSpace = Convert.getShortValue(FREE_SPACE, data);
        slotCnt = Convert.getShortValue(SLOT_CNT, data);

        System.out.println("dumpPage");
        System.out.println("curPage= " + curPage.pid);
        System.out.println("nextPage= " + nextPage.pid);
        System.out.println("usedPtr= " + usedPtr);
        System.out.println("freeSpace= " + freeSpace);
        System.out.println("slotCnt= " + slotCnt);

        for (i = 0, n = DPFIXED; i < slotCnt; n += SIZE_OF_SLOT, i++) {
            length = Convert.getShortValue(n, data);
            offset = Convert.getShortValue(n + 2, data);
            System.out.println("slotNo " + i + " offset= " + offset);
            System.out.println("slotNo " + i + " length= " + length);
        }

    }


    /**
     * Determining if the page is empty
     *
     * @return true if the HFPage is has no records in it, false otherwise
     * @throws IOException I/O errors
     */
    public boolean empty() throws IOException {
        int i;
        short length;
        // look for an empty slot
        slotCnt = Convert.getShortValue(SLOT_CNT, data);

        for (i = 0; i < slotCnt; i++) {
            length = getSlotLength(i);
            if (length != EMPTY_SLOT)
                return false;
        }

        return true;
    }


    /**
     * Constructor of class HFPage
     * initialize a new page
     *
     * @param pageNo the page number of a new page to be initialized
     * @param apage  the Page to be initialized
     * @throws IOException I/O errors
     * @see Page
     */
    public void init(PageId pageNo, Page apage) throws IOException {
        data = apage.getpage();

        slotCnt = 0;                // no slots in use
        Convert.setShortValue(slotCnt, SLOT_CNT, data);

        curPage.pid = pageNo.pid;
        Convert.setIntValue(curPage.pid, CUR_PAGE, data);

        nextPage.pid = prevPage.pid = INVALID_PAGE;
        Convert.setIntValue(prevPage.pid, PREV_PAGE, data);
        Convert.setIntValue(nextPage.pid, NEXT_PAGE, data);

        usedPtr = (short) MAX_SPACE;  // offset in data array (grow backwards)
        Convert.setShortValue(usedPtr, USED_PTR, data);

        freeSpace = (short) (MAX_SPACE - DPFIXED);    // amount of space available
        Convert.setShortValue(freeSpace, FREE_SPACE, data);
    }


    /**
     * Constructor of class BMPage
     * open a existed bmpage
     *
     * @param apage a page in buffer pool
     */

    public void openBMpage(Page apage) {
        data = apage.getpage();
    }


    /**
     * @return page number of current page
     * @throws IOException I/O errors
     */
    public PageId getCurPage()
            throws IOException {
        curPage.pid = Convert.getIntValue(CUR_PAGE, data);
        return curPage;
    }

    /**
     * sets value of curPage to pageNo
     *
     * @param pageNo page number for current page
     * @throws IOException I/O errors
     */
    public void setCurPage(PageId pageNo)
            throws IOException {
        curPage.pid = pageNo.pid;
        Convert.setIntValue(curPage.pid, CUR_PAGE, data);
    }

    /**
     * @return page number of next page
     * @throws IOException I/O errors
     */
    public PageId getNextPage()
            throws IOException {
        nextPage.pid = Convert.getIntValue(NEXT_PAGE, data);
        return nextPage;
    }

    /**
     * sets value of nextPage to pageNo
     *
     * @param pageNo page number for next page
     * @throws IOException I/O errors
     */
    public void setNextPage(PageId pageNo)
            throws IOException {
        nextPage.pid = pageNo.pid;
        Convert.setIntValue(nextPage.pid, NEXT_PAGE, data);
    }

    /**
     * @return PageId of previous page
     * @throws IOException I/O errors
     */
    public PageId getPrevPage()
            throws IOException {
        prevPage.pid = Convert.getIntValue(PREV_PAGE, data);
        return prevPage;
    }

    /**
     * sets value of prevPage to pageNo
     *
     * @param pageNo page number for previous page
     * @throws IOException I/O errors
     */
    public void setPrevPage(PageId pageNo)
            throws IOException {
        prevPage.pid = pageNo.pid;
        Convert.setIntValue(prevPage.pid, PREV_PAGE, data);
    }

    public byte[] getBMpageArray() {
        return data;
    }

    void writeBMPageArray(byte[] givenData) throws IOException {
        data = givenData;
    }


    /**
     * @param slotno slot number
     * @return the length of record the given slot contains
     * @throws IOException I/O errors
     */
    public short getSlotLength(int slotno)
            throws IOException {
        int position = DPFIXED + slotno * SIZE_OF_SLOT;
        short val = Convert.getShortValue(position, data);
        return val;
    }

    public RID insertRecord(byte[] record)
            throws IOException {
        RID rid = new RID();

        int recLen = record.length;
        int spaceNeeded = recLen + SIZE_OF_SLOT;

        // Start by checking if sufficient space exists.
        // This is an upper bound check. May not actually need a slot
        // if we can find an empty one.

        freeSpace = Convert.getShortValue(FREE_SPACE, data);
        if (spaceNeeded > freeSpace) {
            return null;

        } else {

            // look for an empty slot
            slotCnt = Convert.getShortValue(SLOT_CNT, data);
            int i;
            short length;
            for (i = 0; i < slotCnt; i++) {
                length = getSlotLength(i);
                if (length == EMPTY_SLOT)
                    break;
            }

            if (i == slotCnt)   //use a new slot
            {
                // adjust free space
                freeSpace -= spaceNeeded;
                Convert.setShortValue(freeSpace, FREE_SPACE, data);

                slotCnt++;
                Convert.setShortValue(slotCnt, SLOT_CNT, data);

            } else {
                // reusing an existing slot
                freeSpace -= recLen;
                Convert.setShortValue(freeSpace, FREE_SPACE, data);
            }

            usedPtr = Convert.getShortValue(USED_PTR, data);
            usedPtr -= recLen;    // adjust usedPtr
            Convert.setShortValue(usedPtr, USED_PTR, data);

            //insert the slot info onto the data page
            setSlot(i, recLen, usedPtr);

            // insert data onto the data page
            System.arraycopy(record, 0, data, usedPtr, recLen);
            curPage.pid = Convert.getIntValue(CUR_PAGE, data);
            rid.pageNo.pid = curPage.pid;
            rid.slotNo = i;
            return rid;
        }
    }

    /**
     * delete the record with the specified rid
     *
     * @param rid the record ID
     * @throws IOException                I/O errors
     *                                    in C++ Status deleteRecord(const RID& rid)
     * @throws InvalidSlotNumberException Invalid slot number
     */
    public void deleteRecord(RID rid)
            throws IOException,
            InvalidSlotNumberException {
        int slotNo = rid.slotNo;
        short recLen = getSlotLength(slotNo);
        slotCnt = Convert.getShortValue(SLOT_CNT, data);

        // first check if the record being deleted is actually valid
        if ((slotNo >= 0) && (slotNo < slotCnt) && (recLen > 0)) {
            // The records always need to be compacted, as they are
            // not necessarily stored on the page in the order that
            // they are listed in the slot index.

            // offset of record being deleted
            int offset = getSlotOffset(slotNo);
            usedPtr = Convert.getShortValue(USED_PTR, data);
            int newSpot = usedPtr + recLen;
            int size = offset - usedPtr;

            // shift bytes to the right
            System.arraycopy(data, usedPtr, data, newSpot, size);

            // now need to adjust offsets of all valid slots that refer
            // to the left of the record being removed. (by the size of the hole)

            int i, n, chkoffset;
            for (i = 0, n = DPFIXED; i < slotCnt; n += SIZE_OF_SLOT, i++) {
                if ((getSlotLength(i) >= 0)) {
                    chkoffset = getSlotOffset(i);
                    if (chkoffset < offset) {
                        chkoffset += recLen;
                        Convert.setShortValue((short) chkoffset, n + 2, data);
                    }
                }
            }

            // move used Ptr forwar
            usedPtr += recLen;
            Convert.setShortValue(usedPtr, USED_PTR, data);

            // increase freespace by size of hole
            freeSpace = Convert.getShortValue(FREE_SPACE, data);
            freeSpace += recLen;
            Convert.setShortValue(freeSpace, FREE_SPACE, data);

            setSlot(slotNo, EMPTY_SLOT, 0);  // mark slot free
        } else {
            throw new InvalidSlotNumberException(null, "HEAPFILE: INVALID_SLOTNO");
        }
    }

    /**
     * @return RID of first record on page, null if page contains no records.
     * @throws IOException I/O errors
     *                     in C++ Status firstRecord(RID& firstRid)
     */
    public RID firstRecord()
            throws IOException {
        RID rid = new RID();
        // find the first non-empty slot


        slotCnt = Convert.getShortValue(SLOT_CNT, data);

        int i;
        short length;
        for (i = 0; i < slotCnt; i++) {
            length = getSlotLength(i);
            if (length != EMPTY_SLOT)
                break;
        }

        if (i == slotCnt)
            return null;

        // found a non-empty slot

        rid.slotNo = i;
        curPage.pid = Convert.getIntValue(CUR_PAGE, data);
        rid.pageNo.pid = curPage.pid;

        return rid;
    }

    /**
     * @param curRid current record ID
     * @return RID of next record on the page, null if no more
     * records exist on the page
     * @throws IOException I/O errors
     *                     in C++ Status nextRecord (RID curRid, RID& nextRid)
     */
    public RID nextRecord(RID curRid)
            throws IOException {
        RID rid = new RID();
        slotCnt = Convert.getShortValue(SLOT_CNT, data);

        int i = curRid.slotNo;
        short length;

        // find the next non-empty slot
        for (i++; i < slotCnt; i++) {
            length = getSlotLength(i);
            if (length != EMPTY_SLOT)
                break;
        }

        if (i >= slotCnt)
            return null;

        // found a non-empty slot

        rid.slotNo = i;
        curPage.pid = Convert.getIntValue(CUR_PAGE, data);
        rid.pageNo.pid = curPage.pid;

        return rid;
    }

    /**
     * copies out record with RID rid into record pointer.
     * <br>
     * Status getRecord(RID rid, char *recPtr, int& recLen)
     *
     * @param rid the record ID
     * @return a tuple contains the record
     * @throws InvalidSlotNumberException Invalid slot number
     * @throws IOException                I/O errors
     * @see Tuple
     */
    public Tuple getRecord(RID rid)
            throws IOException,
            InvalidSlotNumberException {
        short recLen;
        short offset;
        byte[] record;
        PageId pageNo = new PageId();
        pageNo.pid = rid.pageNo.pid;
        curPage.pid = Convert.getIntValue(CUR_PAGE, data);
        int slotNo = rid.slotNo;

        // length of record being returned
        recLen = getSlotLength(slotNo);
        slotCnt = Convert.getShortValue(SLOT_CNT, data);
        if ((slotNo >= 0) && (slotNo < slotCnt) && (recLen > 0)
                && (pageNo.pid == curPage.pid)) {
            offset = getSlotOffset(slotNo);
            record = new byte[recLen];
            System.arraycopy(data, offset, record, 0, recLen);
            Tuple tuple = new Tuple(record, 0, recLen);
            return tuple;
        } else {
            throw new InvalidSlotNumberException(null, "HEAPFILE: INVALID_SLOTNO");
        }


    }

    /**
     * returns a tuple in a byte array[pageSize] with given RID rid.
     * <br>
     * in C++	Status returnRecord(RID rid, char*& recPtr, int& recLen)
     *
     * @param rid the record ID
     * @return a tuple  with its length and offset in the byte array
     * @throws InvalidSlotNumberException Invalid slot number
     * @throws IOException                I/O errors
     * @see Tuple
     */
    public Tuple returnRecord(RID rid)
            throws IOException,
            InvalidSlotNumberException {
        short recLen;
        short offset;
        PageId pageNo = new PageId();
        pageNo.pid = rid.pageNo.pid;

        curPage.pid = Convert.getIntValue(CUR_PAGE, data);
        int slotNo = rid.slotNo;

        // length of record being returned
        recLen = getSlotLength(slotNo);
        slotCnt = Convert.getShortValue(SLOT_CNT, data);

        if ((slotNo >= 0) && (slotNo < slotCnt) && (recLen > 0)
                && (pageNo.pid == curPage.pid)) {

            offset = getSlotOffset(slotNo);
            Tuple tuple = new Tuple(data, offset, recLen);
            return tuple;
        } else {
            throw new InvalidSlotNumberException(null, "HEAPFILE: INVALID_SLOTNO");
        }

    }

    /**
     * @param slotno slot number
     * @return the offset of record the given slot contains
     * @throws IOException I/O errors
     */
    public short getSlotOffset(int slotno)
            throws IOException {
        int position = DPFIXED + slotno * SIZE_OF_SLOT;
        short val = Convert.getShortValue(position + 2, data);
        return val;
    }

    /**
     * sets slot contents
     *
     * @param slotno the slot number
     * @param length length of record the slot contains
     * @param offset offset of record
     * @throws IOException I/O errors
     */
    public void setSlot(int slotno, int length, int offset)
            throws IOException {
        int position = DPFIXED + slotno * SIZE_OF_SLOT;
        Convert.setShortValue((short) length, position, data);
        Convert.setShortValue((short) offset, position + 2, data);
    }

}
