package index;

import bitmap.BMPage;
import bitmap.BitMapFile;
import btree.PinPageException;
import btree.UnpinPageException;
import bufmgr.PageNotReadException;
import columnar.Columnarfile;
import diskmgr.Page;
import global.*;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import iterator.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by dixith on 3/18/18.
 */

public class ColumnIndexScan extends ColumnIterator implements GlobalConst {
    private final BitMapFile indFile;
    private final PageId rootId;
    private Columnarfile columnarfile;
    byte[] bitMaps;
    BMPage currentBMPage;
    int counter;
    int scanCounter = 0;
    Heapfile[] targetHeapFiles = null;


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
    public List<Tuple> get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {

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

                List<Tuple> nextTuples = new ArrayList<>();
                Tuple JTuple = new Tuple();
                AttrType[] attrType = new AttrType[1];
                attrType[0] = new AttrType(AttrType.attrString);


                short[] strSizes = new short[200];
                strSizes[0] = 200;

                JTuple.setHdr((short) 1, attrType, strSizes);
                if (bitMaps[scanCounter] == 1) {
                    for(Heapfile f: targetHeapFiles) {
                        RID rid = f.recordAtPosition(scanCounter);
                        nextTuples.add(f.getRecord(rid));
                    }
                    scanCounter++;
                    return nextTuples;
                } else {
                    scanCounter++;
                }
            }

        }

        return null;
    }

    public void close() throws IOException, JoinsException, SortException, IndexException, UnpinPageException {
        if (!closeFlag) {
            closeFlag = true;

            PageId curPage = currentBMPage.getCurPage();
            unpinPage(curPage);
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
