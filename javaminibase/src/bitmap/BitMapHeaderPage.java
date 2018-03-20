package bitmap;

import btree.ConstructPageException;
import diskmgr.Page;
import global.AttrType;
import global.Convert;
import global.PageId;
import global.SystemDefs;
import heap.HFPage;

import java.io.IOException;

public class BitMapHeaderPage extends HFPage {

    public static final int DPFIXED = 4 * 2 + 3 * 4;
    public static final int COLUMN_NUMBER_SIZE = 2;
    public static final int ATTR_TYPE_SIZE = 2;
    public static final int COLUMNNAR_FILE_NAME_SIZE = 200;
    public static final int VALUE_SIZE = 400;

    public static final int COLUMN_NUMBER_POSITION = DPFIXED;
    public static final int ATTR_TYPE_POSITION = COLUMN_NUMBER_POSITION + COLUMN_NUMBER_SIZE;
    public static final int COLUMNNAR_FILE_NAME_POSITION = ATTR_TYPE_POSITION + ATTR_TYPE_SIZE;
    public static final int VALUE_POSITION = COLUMNNAR_FILE_NAME_POSITION + COLUMNNAR_FILE_NAME_SIZE;

    public void dumpHeaderPage() throws Exception {
        System.out.println("Dump Header Page");
        System.out.println("Colmnnar File Name= " + getColumnarFileName());
        System.out.println("Column Number= " + getColumnNumber().toString());
        System.out.println("Attribute Type= " + getAttrType().toString());
        System.out.println("Value= " + getValue());
    }

    public void setColumnNumber(int columnNumber) throws Exception {
        Convert.setShortValue((short) columnNumber, COLUMN_NUMBER_POSITION, data);
    }

    public void setAttrType(AttrType attrType) throws Exception {
        Convert.setShortValue((short) attrType.attrType, ATTR_TYPE_POSITION, data);
    }

    public void setColumnarFileName(String columnnarFileName) throws Exception {
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

    public String getColumnarFileName() throws Exception {
        String val = Convert.getStrValue(COLUMNNAR_FILE_NAME_POSITION, data, COLUMNNAR_FILE_NAME_SIZE);
        return val.trim();
    }

    public String getValue() throws Exception {
        String val = Convert.getStrValue(VALUE_POSITION, data, VALUE_SIZE);
        return val.trim();
    }

    public BitMapHeaderPage(PageId pageno)
            throws ConstructPageException {
        super();
        try {

            SystemDefs.JavabaseBM.pinPage(pageno, this, false/*Rdisk*/);
        } catch (Exception e) {
            throw new ConstructPageException(e, "pinpage failed");
        }
    }

    public BitMapHeaderPage(Page page) {
        super(page);
    }

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

    public PageId getPageId() throws IOException {
        return getCurPage();
    }

    void setPageId(PageId pageno)
            throws IOException {
        setCurPage(pageno);
    }

    public PageId get_rootId()
            throws IOException {
        return getNextPage();
    }

    void set_rootId(PageId rootID)
            throws IOException {
        setNextPage(rootID);
    }
}
