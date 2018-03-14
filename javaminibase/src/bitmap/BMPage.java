/* File BMPage.java */

package bitmap;

import diskmgr.Page;
import global.Convert;
import global.GlobalConst;
import global.PageId;

import java.io.IOException;

/**
 * Class bitmap file page.
 * The design assumes that records are kept compacted when
 * deletions are performed.
 */
// TODO: Add void setCurPage_forGivenPosition(int Position) method
interface ConstSlot {
    int INVALID_SLOT = -1;
    int EMPTY_SLOT = -1;
}

public class BMPage extends Page
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

    public BMPage() {
    }

    /**
     * Constructor of class HFPage
     * open a HFPage and make this HFpage piont to the given page
     *
     * @param page the given page in Page type
     */

    public BMPage(Page page) {
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
     * @throws IOException I/O errors
     * @param    pageNo    the page number of a new page to be initialized
     * @param    apage    the Page to be initialized
     * @see        Page
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
     * Constructor of class HFPage
     * open a existed hfpage
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
     * @throws IOException I/O errors
     * @param    pageNo    page number for current page
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
     * @throws IOException I/O errors
     * @param    pageNo    page number for next page
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


    void writeBMPageArray(byte[] data) {

    }


    /**
     * @throws IOException I/O errors
     * @param    slotno    slot number
     * @return the length of record the given slot contains
     */
    public short getSlotLength(int slotno)
            throws IOException {
        int position = DPFIXED + slotno * SIZE_OF_SLOT;
        short val = Convert.getShortValue(position, data);
        return val;
    }

}
