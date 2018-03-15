package bitmap;

import btree.PinPageException;
import diskmgr.Page;
import global.GlobalConst;
import global.PageId;
import global.SystemDefs;

public class BM implements GlobalConst {

    public BM() {
    }

    void printBitMap(BitMapHeaderPage header) throws Exception {
        if (header == null) {
            System.out.println("\n Empty Header!!!");
        } else {
            PageId bmPageId = header.get_rootId();
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
                    bmPage = new BMPage(page);
                }
            }
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
