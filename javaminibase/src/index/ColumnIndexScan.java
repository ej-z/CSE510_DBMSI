package index;

import bitmap.BMPage;
import bitmap.BitMapFile;
import btree.PinPageException;
import bufmgr.PageNotReadException;
import columnar.Columnarfile;
import columnar.TID;
import columnar.TupleScan;
import diskmgr.Page;
import global.AttrType;
import global.IndexType;
import global.PageId;
import global.SystemDefs;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import iterator.*;

import java.io.IOException;


/**
 * Created by dixith on 3/18/18.
 */

public class ColumnIndexScan extends Iterator {
    private final BitMapFile indFile;
    private final PageId rootId;
    private final CondExpr[] given_selects;
    private Heapfile f;
    private Columnarfile columnarfile;
    byte[] bitMaps;
    BMPage currentBMPage;
    int counter;
    int scanCounter = 0;
    TupleScan scan;

    public ColumnIndexScan(IndexType index,
                           String relName,
                           String indName,
                           AttrType type,
                           short str_sizes,
                           CondExpr[] selects,
                           boolean indexOnly,
                           short[] targetedCols) throws IndexException, UnknownIndexTypeException {

        given_selects = selects;

        try {
            f = new Heapfile(relName);
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
                    scan = columnarfile.openTupleScan(targetedCols);

                } catch (Exception e) {
                    throw new IndexException(e, "IndexScan.java: BTreeFile exceptions caught from BTreeFile constructor");
                }

                break;
            case IndexType.None:
            default:
                throw new UnknownIndexTypeException("Only BTree index is supported so far");

        }
    }


    @Override
    public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {


        while (true) {
            if(scanCounter > counter) {
                PageId nextPage = currentBMPage.getNextPage();
                if(nextPage!=null) {
                    currentBMPage = new BMPage(pinPage(nextPage));
                    counter = currentBMPage.getCounter();
                    bitMaps = new BMPage(pinPage(rootId)).getBMpageArray();
                } else {
                    break;
                }
            } else {

                if (bitMaps[scanCounter] == 1) {
                    Tuple next = scan.getNext(new TID());
                    scanCounter++;
                    return next;
                } else {
                    scanCounter++;
                }
            }

        }

        return null;
    }

    @Override
    public void close() throws IOException, JoinsException, SortException, IndexException {

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
