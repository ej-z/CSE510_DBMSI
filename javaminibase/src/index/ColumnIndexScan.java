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
                        // no need to open the other column heap files
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
    public Tuple get_next() {

        try {
            if (isIndexOnlyQuery) {
                if (bitMaps[scanCounter] == 1) {
                    AttrType[] types = new AttrType[1];
                    types[0] = indexAttrType;
                    short[] sizes = new short[1];
                    sizes[0] = 200;
                    Tuple JTuple = new Tuple();
                    JTuple.setHdr((short) 1, types, sizes);

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
                    scanCounter++;
                    return JTuple;
                } else {
                    scanCounter++;
                }

            } else {
                while (true) {
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

                        Tuple JTuple = new Tuple();
                        JTuple.setHdr((short) givenTargetedCols.length, targetAttrTypes, targetShortSizes);
                        if (bitMaps[scanCounter] == 1) {
                            for (int i = 0; i < targetHeapFiles.length; i++) {
                                RID rid = targetHeapFiles[i].recordAtPosition(scanCounter);
                                Tuple record = targetHeapFiles[i].getRecord(rid);
                                switch (targetAttrTypes[i].attrType) {
                                    case AttrType.attrInteger:
                                        // why 1 as it col heap file will have one field
                                        JTuple.setIntFld(i + 1, record.getIntFld(1));
                                        break;
                                    case AttrType.attrString:
                                        JTuple.setStrFld(i + 1, record.getStrFld(1));
                                        break;
                                    default:
                                        throw new Exception("Attribute indexAttrType not supported");
                                }
                            }
                            scanCounter++;
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

    }

    private void setTargetColuumStringSizes(short[] targetedCols) {
        short[] attributeStringSizes = columnarfile.getStringSizes();

        for(int i=0; i < targetAttrTypes.length; i++) {
            targetShortSizes[i] = attributeStringSizes[targetedCols[i]];
        }
    }

    private void setTargetColumnAttributeTypes(short[] targetedCols) {
        AttrType[] attributes = columnarfile.getAttributes();

        for(int i=0; i < targetAttrTypes.length; i++) {
            targetAttrTypes[i] = attributes[targetedCols[i]];
        }
    }

    private void setTargetHeapFiles(String relName, short[] targetedCols) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
        for(int i=0; i < targetedCols.length; i++) {
            targetHeapFiles[i] = new Heapfile(relName + Short.toString(targetedCols[i]));
        }
    }
}
