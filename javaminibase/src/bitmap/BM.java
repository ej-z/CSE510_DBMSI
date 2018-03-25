package bitmap;

import btree.PinPageException;
import btree.UnpinPageException;
import diskmgr.Page;
import global.GlobalConst;
import global.PageId;
import global.SystemDefs;

import java.util.ArrayList;
import java.util.List;

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
    public static void printBitMap(BitMapHeaderPage header) throws Exception {
        if (header == null) {
            System.out.println("\n Empty Header!!!");
        } else {
            List<PageId> pinnedPages = new ArrayList<>();
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
            pinnedPages.add(bmPageId);
            BMPage bmPage = new BMPage(page);
            int position = 0;
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
                    pinnedPages.add(bmPage.getNextPage());
                    bmPage.openBMpage(page);
                }
            }
            for (PageId pageId : pinnedPages) {
                unpinPage(pageId);
            }
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

    private static Page pinPage(PageId pageno) throws PinPageException {
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
