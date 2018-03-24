package index;

import bitmap.BMPage;
import bitmap.BitMapFile;
import btree.PinPageException;
import bufmgr.PageNotReadException;
import columnar.Columnarfile;
import diskmgr.Page;
import global.*;
import heap.*;
import iterator.*;

import java.io.IOException;


/**
 * Created by dixith on 3/18/18.
 */

public class ColumnIndexScan extends Iterator implements GlobalConst {
    private final BitMapFile indFile;
    private final PageId rootId;
    private Columnarfile columnarfile;
    byte[] bitMaps;
    BMPage currentBMPage;
    int counter;
    int scanCounter = 0;
    Heapfile[] targetHeapFiles = null;
    AttrType[] targetAttrTypes = null;
    short[] targetShortSizes = null;
    short[] givenTargetedCols = null;


    public ColumnIndexScan(IndexType index,
                           String relName,
                           String indName, // R.A.5
                           AttrType type,
                           short str_sizes,
                           CondExpr[] selects, // buils R.A.5
                           boolean indexOnly,
                           short[] targetedCols) throws IndexException, UnknownIndexTypeException {

        try {
            targetHeapFiles = new Heapfile[targetedCols.length];
            targetAttrTypes = new AttrType[targetedCols.length];
            targetShortSizes = new short[targetedCols.length];
            givenTargetedCols = targetedCols;

        } catch (Exception e) {
            throw new IndexException(e, "IndexScan.java: Heapfile not created");
        }

        switch (index.indexType) {
            case IndexType.BitMapIndex:
                try {
                    indFile = new BitMapFile(indName);

                    rootId = indFile.getHeaderPage().get_rootId();
                    currentBMPage = new BMPage(pinPage(rootId));
                    counter = currentBMPage.getCounter();
                    bitMaps = new BMPage(pinPage(rootId)).getBMpageArray();

                    columnarfile = new Columnarfile(relName);

                    for(int i=0; i < targetedCols.length; i++) {
                        targetHeapFiles[i] = new Heapfile(relName + Short.toString(targetedCols[i]));
                    }
                    AttrType[] attributes = columnarfile.getAttributes();

                    for(int i=0; i < targetAttrTypes.length; i++) {
                        targetAttrTypes[i] = attributes[targetedCols[i]];
                    }

                    short[] attributeStringSizes = columnarfile.getStringSizes();

                    for(int i=0; i < targetAttrTypes.length; i++) {
                        targetShortSizes[i] = attributeStringSizes[targetedCols[i]];
                    }

                } catch (Exception e) {
                    throw new IndexException(e, "IndexScan.java: BTreeFile exceptions caught from BTreeFile constructor");
                }

                break;
            case IndexType.None:
            default:
                throw new UnknownIndexTypeException("Only BTree index is supported so far");

        }
    }

    // TODO think about unpinning the pages
    @Override
    public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {

        while (true) {
            if(scanCounter > counter) {
                PageId nextPage = currentBMPage.getNextPage();
                if(nextPage.pid !=INVALID_PAGE) {
                    currentBMPage = new BMPage(pinPage(nextPage));
                    counter = currentBMPage.getCounter();
                    bitMaps = new BMPage(pinPage(rootId)).getBMpageArray();
                } else {
                    close();
                    break;
                }
            } else {

                Tuple JTuple = new Tuple();
//                AttrType[] attrType = new AttrType[2];
//                attrType[0] = new AttrType(AttrType.attrString);
//                attrType[1] = new AttrType(AttrType.attrString);


                short[] strSizes = new short[2];
                strSizes[0] = 200;
                strSizes[1] = 200;

                JTuple.setHdr((short) givenTargetedCols.length, targetAttrTypes, targetShortSizes);

                if (bitMaps[scanCounter] == 1) {
                    for(int i =0 ; i < targetHeapFiles.length; i++) {
                        RID rid = targetHeapFiles[i].recordAtPosition(scanCounter);
                        Tuple record = targetHeapFiles[i].getRecord(rid);
                        switch (targetAttrTypes[i].attrType) {
                            case AttrType.attrInteger:
                                // why 1 as it col heap file will have one field
                                JTuple.setIntFld(i+1, record.getIntFld(1));
                                break;
                            case AttrType.attrString:
                                JTuple.setStrFld(i+1, record.getStrFld(1));
                                break;
                            default:
                                throw new Exception("Attribute type not supported");
                        }
                    }
                    scanCounter++;
                    return JTuple;
                } else {
                    scanCounter++;
                }
            }
        }

        return null;
    }

    public void close() throws IOException, JoinsException, SortException, IndexException, HFBufMgrException {
        if (!closeFlag) {
            closeFlag = true;

            PageId curPage = currentBMPage.getCurPage();
            unpinPage(curPage, false);
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

    /**
     * short cut to access the unpinPage function in bufmgr package.
     *
     * @see bufmgr.unpinPage
     */
    private void unpinPage(PageId pageno, boolean dirty)
            throws HFBufMgrException {

        try {
            SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
        } catch (Exception e) {
            throw new HFBufMgrException(e, "Heapfile.java: unpinPage() failed");
        }

    } // end of unpinPage

}
