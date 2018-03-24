package bitmap;

import btree.GetFileEntryException;
import btree.PinPageException;
import columnar.ValueClass;
import diskmgr.Page;
import global.GlobalConst;
import global.PageId;
import global.SystemDefs;

public class BM implements GlobalConst {

    /**
     * Default constructor
     */
    public BM() {
    }

    /**
     * Pretty print the bitmap
     *
     * @param header
     * @throws Exception
     */
    public void printBitMap(BitMapHeaderPage header) throws Exception {
        if (header == null) {
            System.out.println("\n Empty Header!!!");
        } else {
            PageId bmPageId = header.get_rootId();
            if (bmPageId.pid == INVALID_PAGE) {
                System.out.println("Empty Bitmap File");
                return;
            }
            System.out.println("Columnar File Name: " + header.getColumnarFileName());
            System.out.println("Column Number: " + header.getColumnNumber());
            System.out.println("Attribute Type: " + header.getAttrType());
            System.out.println("Attribute Value: " + header.getValue());
            Page page = pinPage(bmPageId);
            BMPage bmPage = new BMPage(page);
            int position = 1;
            while (Boolean.TRUE) {
                int count = bmPage.getCounter();
                byte[] currentPageByteArray = bmPage.getBMpageArray();
                for (int i = 0; i < count; i++) {
                    System.out.println("Position: " + position + "   Value: " + currentPageByteArray[i]);
                    position++;
                }
                if (bmPage.getNextPage().pid == INVALID_PAGE) {
                    break;
                } else {
                    page = pinPage(bmPage.getNextPage());
                    bmPage.openBMpage(page);
                }
            }
        }
    }

    /***
     * checks if a bitmap file exists or not
     * @param columnnarFileName
     * @param columnPosition
     * @param value
     * @return Boolean
     * @throws Exception
     */
    public Boolean checkIfBitMapFileExists(String columnnarFileName, Integer columnPosition, ValueClass value) throws Exception {
        String bitmapFileName = columnnarFileName + "-" + columnPosition.toString() + "-" + value.toString();
        PageId pageId = get_file_entry(bitmapFileName);
        if (pageId == null) {
            return Boolean.FALSE;
        } else {
            return Boolean.TRUE;
        }
    }

    /***
     * Get the header page Id of the bitmap
     * @param columnnarFileName
     * @param columnPosition
     * @param value
     * @return PageId
     * @throws Exception
     */
    public PageId getBitMapHeaderPage(String columnnarFileName, Integer columnPosition, ValueClass value) throws Exception {
        String bitmapFileName = columnnarFileName + "-" + columnPosition.toString() + "-" + value.toString();
        PageId pageId = get_file_entry(bitmapFileName);

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

    private Page pinPage(PageId pageno) throws PinPageException {
        try {
            Page page = new Page();
            SystemDefs.JavabaseBM.pinPage(pageno, page, false/*Rdisk*/);
            return page;
        } catch (Exception e) {
            e.printStackTrace();
            throw new PinPageException(e, "");
        }
    }

}
