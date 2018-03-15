package bitmap;

import btree.ConstructPageException;
import diskmgr.Page;
import global.AttrType;
import global.Convert;
import global.PageId;
import global.SystemDefs;

import java.io.IOException;

public class BitMapHeaderPage extends BMPage {

    public static final int DPFIXED = 4 * 2 + 3 * 4;
    public static final int COLUMN_NUMBER_SIZE = 2;
    public static final int ATTR_TYPE_SIZE = 2;
    public static final int COLUMNNAR_FILE_NAME_SIZE = 200;
    public static final int VALUE_SIZE = 400;

    public static final int COLUMN_NUMBER_POSITION = DPFIXED;
    public static final int ATTR_TYPE_POSITION = COLUMN_NUMBER_POSITION + COLUMN_NUMBER_SIZE;
    public static final int COLUMNNAR_FILE_NAME_POSITION = ATTR_TYPE_POSITION + ATTR_TYPE_SIZE;
    public static final int VALUE_POSITION = COLUMNNAR_FILE_NAME_POSITION + COLUMNNAR_FILE_NAME_SIZE;

    public void setColumnNumber(int columnNumber) throws Exception {
        Convert.setShortValue((short) columnNumber, COLUMN_NUMBER_POSITION, data);
    }

    public void setAttrType(AttrType attrType) throws Exception {
        Convert.setShortValue((short) attrType.attrType, ATTR_TYPE_POSITION, data);
    }

    public void setColumnnarFileName(String columnnarFileName) throws Exception {
        Convert.setStrValue(columnnarFileName, COLUMNNAR_FILE_NAME_POSITION, data);
    }

    public void setValue(String value) throws Exception {
        Convert.setStrValue(value, VALUE_POSITION, data);
    }

    public Integer getColumnNumber() throws Exception {
        short val = Convert.getShortValue(COLUMN_NUMBER_POSITION, data);
        return (int) val;
    }

    public AttrType getAttrType() throws Exception {
        short val = Convert.getShortValue(ATTR_TYPE_POSITION, data);
        return new AttrType(val);
    }

    public String getColumnnarFileName() throws Exception {
        String val = Convert.getStrValue(COLUMNNAR_FILE_NAME_POSITION, data, COLUMNNAR_FILE_NAME_SIZE);
        return val.trim();
    }

    public String getValue() throws Exception {
        String val = Convert.getStrValue(VALUE_POSITION, data, VALUE_SIZE);
        return val.trim();
    }

    /**
     * pin the page with pageno, and get the corresponding SortedPage
     */
    public BitMapHeaderPage(PageId pageno)
            throws ConstructPageException {
        super();
        try {

            SystemDefs.JavabaseBM.pinPage(pageno, this, false/*Rdisk*/);
        } catch (Exception e) {
            throw new ConstructPageException(e, "pinpage failed");
        }
    }

    /**
     * associate the SortedPage instance with the Page instance
     */
    public BitMapHeaderPage(Page page) {
        super(page);
    }

    /**
     * new a page, and associate the SortedPage instance with the Page instance
     */
    public BitMapHeaderPage() throws ConstructPageException {
        super();
        try {
            Page apage = new Page();
            PageId pageId = SystemDefs.JavabaseBM.newPage(apage, 1);
            if (pageId == null)
                throw new ConstructPageException(null, "new page failed");
            this.init(pageId, apage);

        } catch (Exception e) {
            throw new ConstructPageException(e, "construct header page failed");
        }
    }

    PageId getPageId() throws IOException {
        return getCurPage();
    }

    void setPageId(PageId pageno)
            throws IOException {
        setCurPage(pageno);
    }

    /**
     * get the rootId
     */
    PageId get_rootId()
            throws IOException {
        return getNextPage();
    }

    /**
     * set the rootId
     */
    void set_rootId(PageId rootID)
            throws IOException {
        setNextPage(rootID);
    }

    void setColumnarFileName(String fileName) {

    }

//    /**
//     * get the magic0
//     */
//    int get_magic0()
//            throws IOException {
//        return getPrevPage().pid;
//    }
//
//    /**
//     * set the magic0
//     *
//     * @param magic magic0 will be set to be equal to magic
//     */
//    void set_magic0(int magic)
//            throws IOException {
//        setPrevPage(new PageId(magic));
//    }

//    /**
//     * get the key type
//     */
//    short get_keyType()
//            throws IOException {
//        return (short) getSlotLength(3);
//    }
//
//    /**
//     * set the max keysize
//     */
//    int get_maxKeySize()
//            throws IOException {
//        return getSlotLength(1);
//    }
//
//
//    /**
//     * get the delete fashion
//     */
//    int get_deleteFashion()
//            throws IOException {
//        return getSlotLength(2);
//    }

    /* get the max keysize
     */
//    void set_maxKeySize(int key_size)
//            throws IOException {
//        setSlot(1, key_size, 0);
//    }
//
//    /**
//     * set the key type
//     */
//    void set_keyType(short key_type)
//            throws IOException {
//        setSlot(3, (int) key_type, 0);
//    }
//
//    /**
//     * set the delete fashion
//     */
//    void set_deleteFashion(int fashion)
//            throws IOException {
//        setSlot(2, fashion, 0);
//    }
}
