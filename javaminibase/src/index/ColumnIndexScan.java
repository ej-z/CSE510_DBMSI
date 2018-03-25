package index;

import bitmap.BMPage;
import bitmap.BitMapFile;
import btree.PinPageException;
import columnar.Columnarfile;
import columnar.TID;
import diskmgr.Page;
import global.*;
import heap.*;
import iterator.CondExpr;
import iterator.Iterator;
import iterator.JoinsException;
import iterator.SortException;

import java.io.IOException;


/**
 * Created by dixith on 3/18/18.
 */

public class ColumnIndexScan extends Iterator implements GlobalConst {
    private final String indName;
    private final AttrType indexAttrType;
    private final short str_sizes;
    private BitMapFile indFile;
    private PageId rootId;
    private Columnarfile columnarfile;
    private byte[] bitMaps;
    private BMPage currentBMPage;
    private int counter;
    private int scanCounter = 0;
    private Heapfile[] targetHeapFiles = null;
    private AttrType[] targetAttrTypes = null;
    private short[] targetShortSizes = null;
    private short[] givenTargetedCols = null;
    private Boolean isIndexOnlyQuery = false;


    public ColumnIndexScan(IndexType index,
                           String relName,
                           String indName, // R.A.5
                           AttrType indexAttrType,
                           short str_sizes,
                           CondExpr[] selects, // buils R.A.5
                           boolean indexOnly,
                           short[] targetedCols) throws IndexException, UnknownIndexTypeException {

        try {
            targetHeapFiles = new Heapfile[targetedCols.length];
            targetAttrTypes = new AttrType[targetedCols.length];
            targetShortSizes = new short[targetedCols.length];
            givenTargetedCols = targetedCols;
            this.indName = indName;
            this.indexAttrType = indexAttrType;
            this.str_sizes = str_sizes;

        } catch (Exception e) {
            throw new IndexException(e, "IndexScan.java: Heapfile not created");
        }

        switch (index.indexType) {
            case IndexType.BitMapIndex:
                try {

                    if(indexOnly) {
                        isIndexOnlyQuery = true;
                        // if the query is index only, we can use the index to answer the queries
                        // no other heap files are opened except the bitmap file
                        indFile = new BitMapFile(indName);
                        rootId = indFile.getHeaderPage().get_rootId();
                        currentBMPage = new BMPage(pinPage(rootId));
                        counter = currentBMPage.getCounter();
                        bitMaps = new BMPage(pinPage(rootId)).getBMpageArray();

                    } else {
                        indFile = new BitMapFile(indName);
                        rootId = indFile.getHeaderPage().get_rootId();
                        currentBMPage = new BMPage(pinPage(rootId));
                        counter = currentBMPage.getCounter();
                        bitMaps = new BMPage(pinPage(rootId)).getBMpageArray();
                        columnarfile = new Columnarfile(relName);

                        setTargetHeapFiles(relName, targetedCols);
                        setTargetColumnAttributeTypes(targetedCols);
                        setTargetColuumStringSizes(targetedCols);
                    }
                } catch (Exception e) {
                    // any exception is swalled into a Index Exception
                    throw new IndexException(e, "IndexScan.java: BTreeFile exceptions caught from BTreeFile constructor");
                }

                break;
            case IndexType.None:
            default:
                throw new UnknownIndexTypeException("Only BTree index is supported so far");

        }
    }

    @Override
    public Tuple get_next() {

        try {
            if (isIndexOnlyQuery) {
                if (bitMaps[scanCounter] == 1) {
                    AttrType[] types = new AttrType[1];
                    types[0] = indexAttrType;
                    short[] sizes = new short[1];
                    sizes[0] = 200;
                    Tuple JTuple = new Tuple();
                    // set the tuple header to contain only one value as it is index only query
                    JTuple.setHdr((short) 1, types, sizes);

                    // return the value class on which the bitmap was built
                    String value = indName.split("-")[2];

                    switch (indexAttrType.attrType) {
                        case AttrType.attrInteger:
                            // why 1 as it col heap file will have one field
                            JTuple.setIntFld(1, Integer.parseInt(value));
                            break;
                        case AttrType.attrString:
                            JTuple.setStrFld(1, value);
                            break;
                        default:
                            throw new Exception("Attribute indexAttrType not supported");
                    }
                    // increment the scan counter on every get_next() call
                    scanCounter++;
                    return JTuple;
                } else {
                    scanCounter++;
                }

            } else {
                while (true) {
                    // if the scanCounter is greater than the current BM Page counter then scan
                    // the next BMPage else close the iterator
                    if (scanCounter > counter) {
                        PageId nextPage = currentBMPage.getNextPage();
                        if (nextPage.pid != INVALID_PAGE) {
                            currentBMPage = new BMPage(pinPage(nextPage));
                            counter = currentBMPage.getCounter();
                            bitMaps = new BMPage(pinPage(rootId)).getBMpageArray();
                        } else {
                            close();
                            break;
                        }
                    } else {

                        // tuple that needs to sent
                        Tuple JTuple = new Tuple();
                        // set the header which attribute types of the targeted columns
                        JTuple.setHdr((short) givenTargetedCols.length, targetAttrTypes, targetShortSizes);
                        // for every bit 1 in the bitmap file, get the record at the position in the targeted columns
                        // extract the data element and set it to the tuple based on the attribute type

                        if (bitMaps[scanCounter] == 1) {
                            for (int i = 0; i < targetHeapFiles.length; i++) {
                                RID rid = targetHeapFiles[i].recordAtPosition(scanCounter);
                                Tuple record = targetHeapFiles[i].getRecord(rid);
                                switch (targetAttrTypes[i].attrType) {
                                    case AttrType.attrInteger:
                                        // Assumed that col heap page will have only one entry
                                        JTuple.setIntFld(i + 1, record.getIntFld(1));
                                        break;
                                    case AttrType.attrString:
                                        JTuple.setStrFld(i + 1, record.getStrFld(1));
                                        break;
                                    default:
                                        throw new Exception("Attribute indexAttrType not supported");
                                }
                            }
                            // increment the scan counter on every get_next() call
                            scanCounter++;
                            // return the Tuple built by scanning the targeted columns
                            return JTuple;
                        } else {
                            scanCounter++;
                        }
                    }
                }

            }
        }
        catch (Exception e){
            System.err.println(scanCounter);
            e.printStackTrace();
        }
        return null;
    }


    public Tuple get_next(TID tid) {

        try {
            while (true) {
                // if the scanCounter is greater than the current BM Page counter then scan
                // the next BMPage else close the iterator
                if (scanCounter > counter) {
                    PageId nextPage = currentBMPage.getNextPage();
                    if (nextPage.pid != INVALID_PAGE) {
                        currentBMPage = new BMPage(pinPage(nextPage));
                        counter = currentBMPage.getCounter();
                        bitMaps = new BMPage(pinPage(rootId)).getBMpageArray();
                    } else {
                        close();
                        break;
                    }
                } else {

                    // tuple that needs to sent
                    Tuple JTuple = new Tuple();
                    // set the header which attribute types of the targeted columns
                    JTuple.setHdr((short) givenTargetedCols.length, targetAttrTypes, targetShortSizes);
                    // for every bit 1 in the bitmap file, get the record at the position in the targeted columns
                    // extract the data element and set it to the tuple based on the attribute type

                    if (bitMaps[scanCounter] == 1) {

                        for (int i = 0; i < targetHeapFiles.length; i++) {

                            RID rid = targetHeapFiles[i].recordAtPosition(scanCounter);
                            //construct TID from RID's
                            tid.setRID(i, rid);
                            tid.setPosition(scanCounter);
                            Tuple record = targetHeapFiles[i].getRecord(rid);
                            switch (targetAttrTypes[i].attrType) {
                                case AttrType.attrInteger:
                                    // Assumed that col heap page will have only one entry
                                    JTuple.setIntFld(i + 1, record.getIntFld(1));
                                    break;
                                case AttrType.attrString:
                                    JTuple.setStrFld(i + 1, record.getStrFld(1));
                                    break;
                                default:
                                    throw new Exception("Attribute indexAttrType not supported");
                            }
                        }
                        // increment the scan counter on every get_next() call
                        scanCounter++;
                        // return the Tuple built by scanning the targeted columns
                        return JTuple;
                    } else {
                        scanCounter++;
                    }
                }
            }
        }
        catch (Exception e){
            System.err.println(scanCounter);
            e.printStackTrace();
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
     */
    private void unpinPage(PageId pageno, boolean dirty)
            throws HFBufMgrException {

        try {
            SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
        } catch (Exception e) {
            throw new HFBufMgrException(e, "Heapfile.java: unpinPage() failed");
        }

    }

    /*
    * Gets the attribute string sizes from the coulumar file
    * and required for the seting the tuple header for the projection
    * */
    private void setTargetColuumStringSizes(short[] targetedCols) {
        short[] attributeStringSizes = columnarfile.getStringSizes();

        for(int i=0; i < targetAttrTypes.length; i++) {
            targetShortSizes[i] = attributeStringSizes[targetedCols[i]];
        }
    }

    /*
    * Gets the attribute types of the target columns for the columnar file
    * Is used while setting the Tuple header for the projection
    *
    * */
    private void setTargetColumnAttributeTypes(short[] targetedCols) {
        AttrType[] attributes = columnarfile.getAttributes();

        for(int i=0; i < targetAttrTypes.length; i++) {
            targetAttrTypes[i] = attributes[targetedCols[i]];
        }
    }

    // open the targeted column heap files and store those reference for scanning
    private void setTargetHeapFiles(String relName, short[] targetedCols) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
        for(int i=0; i < targetedCols.length; i++) {
            targetHeapFiles[i] = new Heapfile(relName + Short.toString(targetedCols[i]));
        }
    }
}
