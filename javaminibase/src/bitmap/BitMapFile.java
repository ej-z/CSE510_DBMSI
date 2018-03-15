package bitmap;

import btree.*;
import columnar.Columnarfile;
import columnar.ValueClass;
import columnar.ValueInt;
import columnar.ValueString;
import diskmgr.Page;
import global.AttrType;
import global.GlobalConst;
import global.PageId;
import global.SystemDefs;
import heap.HFBufMgrException;

// TODO: Add void setCurPage_forGivenPosition(int Position) method
public class BitMapFile implements GlobalConst {

    private String fileName;
    private BitMapHeaderPage headerPage;
    private PageId headerPageId;
    private String columnarFileName;
    private Integer columnNumber;
    private AttrType attrType;
    private ValueClass value;

    public Integer getColumnNumber() {
        return columnNumber;
    }

    public void setColumnNumber(Integer columnNumber) {
        this.columnNumber = columnNumber;
    }

    public ValueClass getValue() {
        return value;
    }

    public void setValue(ValueClass value) {
        this.value = value;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public BitMapHeaderPage getHeaderPage() {
        return headerPage;
    }

    public void setHeaderPage(BitMapHeaderPage headerPage) {
        this.headerPage = headerPage;
    }

    public BitMapFile(String fileName) throws Exception {
        this.fileName = fileName;
        headerPageId = get_file_entry(fileName);
        if (headerPageId == null) {
            throw new Exception("This index file (" + fileName + ") doesn't exist");
        }
        headerPage = new BitMapHeaderPage(headerPageId);

        columnarFileName = headerPage.getColumnarFileName();
        columnNumber = headerPage.getColumnNumber();
        attrType = headerPage.getAttrType();
        if (attrType.attrType == AttrType.attrString) {
            value = new ValueString(headerPage.getValue());
        } else {
            value = new ValueInt(Integer.parseInt(headerPage.getValue()));
        }
    }

    public BitMapFile(String filename, Columnarfile columnarFile, Integer columnNo, ValueClass value) throws Exception {
        headerPageId = get_file_entry(filename);
        if (headerPageId == null) //file not exist
        {
            headerPage = new BitMapHeaderPage();
            headerPageId = headerPage.getPageId();
            add_file_entry(filename, headerPageId);
            headerPage.set_rootId(new PageId(INVALID_PAGE));
            headerPage.setColumnarFileName(columnarFile.getColumnarFileName());
            headerPage.setColumnNumber(columnNo);
            if (value instanceof ValueInt) {
                headerPage.setValue(((ValueInt) value).getValue().toString());
                headerPage.setAttrType(new AttrType(AttrType.attrInteger));
            } else {
                headerPage.setValue(((ValueString) value).getValue());
                headerPage.setAttrType(new AttrType(AttrType.attrString));
            }
        } else {
            headerPage = new BitMapHeaderPage(headerPageId);
        }
    }

    public void close() throws Exception {
        if (headerPage != null) {
            SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
            headerPage = null;
        }
    }

    public void destroyBitMapFile() throws Exception {
        if (headerPage != null) {
            PageId pgId = headerPage.get_rootId();
            BMPage bmPage;
            while (pgId.pid != INVALID_PAGE) {
                Page page = pinPage(pgId);
                bmPage = new BMPage(page);
                pgId = bmPage.getNextPage();
                unpinPage(pgId);
                freePage(pgId);
            }
            unpinPage(headerPageId);
            freePage(headerPageId);
            delete_file_entry(fileName);
            headerPage = null;
        }
    }

    public Boolean delete(int position) throws Exception {
        if (headerPage.get_rootId().pid != INVALID_PAGE) {
            int pageCounter = 1;
            while (position > BMPage.MAX_POSITION_IN_A_PAGE) {
                pageCounter++;
                position -= BMPage.MAX_POSITION_IN_A_PAGE;
            }
            PageId bmPageId = headerPage.get_rootId();
            Page page = pinPage(bmPageId);
            BMPage bmPage = new BMPage(page);
            for (int i = 1; i < pageCounter; i++) {
                bmPageId = bmPage.getNextPage();
                if (bmPageId.pid == BMPage.INVALID_PAGE) {
                    throw new Exception("Non existent position passed as input");
                }
                page = pinPage(bmPageId);
                bmPage = new BMPage(page);
            }
            byte[] currentPageData = bmPage.getBMpageArray();
            currentPageData[position] = 0;
            bmPage.writeBMPageArray(currentPageData);
            if (bmPage.getCounter() < position) {
                bmPage.updateCounter((short) position);
            }

            return Boolean.TRUE;
        }

        return Boolean.FALSE;
    }


    public Boolean insert(int position) throws Exception {
        if (headerPage.get_rootId().pid != INVALID_PAGE) {
            int pageCounter = 1;
            while (position > BMPage.MAX_POSITION_IN_A_PAGE) {
                pageCounter++;
                position -= BMPage.MAX_POSITION_IN_A_PAGE;
            }
            PageId bmPageId = headerPage.get_rootId();
            Page page = pinPage(bmPageId);
            BMPage bmPage = new BMPage(page);
            for (int i = 1; i < pageCounter; i++) {
                bmPageId = bmPage.getNextPage();
                if (bmPageId.pid == BMPage.INVALID_PAGE) {
                    PageId newPageId = getNewBMPage();
                    bmPage.setNextPage(newPageId);
                    bmPageId = newPageId;
                }
                page = pinPage(bmPageId);
                bmPage = new BMPage(page);
            }
            byte[] currentPageData = bmPage.getBMpageArray();
            currentPageData[position] = 1;
            bmPage.writeBMPageArray(currentPageData);
            if (bmPage.getCounter() < position) {
                bmPage.updateCounter((short) position);
            }
        } else {
            PageId newPageId = getNewBMPage();
            headerPage.set_rootId(newPageId);
            insert(position);
        }

        return Boolean.TRUE;
    }

    public PageId getNewBMPage() throws Exception {
        Page apage = new Page();
        PageId pageId = newPage(apage, 1);
        BMPage bmPage = new BMPage();
        bmPage.init(pageId, apage);

        return pageId;
    }

    private PageId get_file_entry(String filename)
            throws GetFileEntryException {
        try {
            return SystemDefs.JavabaseDB.get_file_entry(filename);
        } catch (Exception e) {
            e.printStackTrace();
            throw new GetFileEntryException(e, "");
        }
    }

    private void add_file_entry(String fileName, PageId pageno)
            throws AddFileEntryException {
        try {
            SystemDefs.JavabaseDB.add_file_entry(fileName, pageno);
        } catch (Exception e) {
            e.printStackTrace();
            throw new AddFileEntryException(e, "");
        }
    }

    private void delete_file_entry(String filename)
            throws DeleteFileEntryException {
        try {
            SystemDefs.JavabaseDB.delete_file_entry(filename);
        } catch (Exception e) {
            e.printStackTrace();
            throw new DeleteFileEntryException(e, "");
        }
    }

    private void unpinPage(PageId pageno)
            throws UnpinPageException {
        try {
            SystemDefs.JavabaseBM.unpinPage(pageno, false /* = not DIRTY */);
        } catch (Exception e) {
            e.printStackTrace();
            throw new UnpinPageException(e, "");
        }
    }

    private void unpinPage(PageId pageno, boolean dirty)
            throws UnpinPageException {
        try {
            SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
        } catch (Exception e) {
            e.printStackTrace();
            throw new UnpinPageException(e, "");
        }
    }

    private void freePage(PageId pageno)
            throws FreePageException {
        try {
            SystemDefs.JavabaseBM.freePage(pageno);
        } catch (Exception e) {
            e.printStackTrace();
            throw new FreePageException(e, "");
        }

    }

    private Page pinPage(PageId pageno)
            throws PinPageException {
        try {
            Page page = new Page();
            SystemDefs.JavabaseBM.pinPage(pageno, page, false/*Rdisk*/);
            return page;
        } catch (Exception e) {
            e.printStackTrace();
            throw new PinPageException(e, "");
        }
    }

    private void updateHeader(PageId firstPage) throws Exception {
        BitMapHeaderPage header = new BitMapHeaderPage(pinPage(headerPageId));
        header.set_rootId(firstPage);
        unpinPage(headerPageId, true /* = DIRTY */);
    }

    private PageId newPage(Page page, int num)
            throws HFBufMgrException {

        PageId tmpId = new PageId();

        try {
            tmpId = SystemDefs.JavabaseBM.newPage(page, num);
        } catch (Exception e) {
            throw new HFBufMgrException(e, "Heapfile.java: newPage() failed");
        }

        return tmpId;

    } // end of newPage

}
