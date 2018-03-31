package index;

import bitmap.BMPage;
import bitmap.BitMapFile;
import btree.PinPageException;
import columnar.Columnarfile;
import diskmgr.Page;
import global.*;
import heap.*;
import iterator.*;

import java.io.IOException;
import java.util.BitSet;

public class ColumnarBitmapScan extends Iterator implements GlobalConst{

    private final String indName;
    private final AttrType indexAttrType;
    private final short str_sizes;
    private BitMapFile bmIndFile;
    private PageId currentPageId;
    private Columnarfile columnarfile;
    private BitSet bitMaps;
    private BMPage currentBMPage;
    private int counter;
    private int scanCounter = 0;
    private Heapfile[] targetHeapFiles = null;
    private AttrType[] targetAttrTypes = null;
    private short[] targetShortSizes = null;
    private short[] givenTargetedCols = null;
    private CondExpr[] _selects;
    private boolean index_only;
    private String value;
    public ColumnarBitmapScan(String relName,
                              String indName,
                              AttrType indexAttrType,
                              short str_sizes,
                              CondExpr[] selects,
                              boolean indexOnly,
                              short[] targetedCols) throws IndexException {

        targetHeapFiles = new Heapfile[targetedCols.length];
        targetAttrTypes = new AttrType[targetedCols.length];
        targetShortSizes = new short[targetedCols.length];
        givenTargetedCols = targetedCols;
        this.indName = indName;
        this.indexAttrType = indexAttrType;
        this.str_sizes = str_sizes;
        _selects = selects;
        index_only = indexOnly;

        try {

            columnarfile = new Columnarfile(relName);
            setTargetHeapFiles(relName, targetedCols);
            setTargetColumnAttributeTypes(targetedCols);
            setTargetColumnStringSizes(targetedCols);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        try {
            bmIndFile = new BitMapFile(indName);
            currentPageId = bmIndFile.getHeaderPage().get_rootId();
            value = bmIndFile.getHeaderPage().getValue();
            currentBMPage = new BMPage(pinPage(currentPageId));
            counter = currentBMPage.getCounter();
            bitMaps = BitSet.valueOf(currentBMPage.getBMpageArray());
        } catch (Exception e) {
            // any exception is swalled into a Index Exception
            throw new IndexException(e, "IndexScan.java: BitMapFile exceptions caught from BitMapFile constructor");
        }
    }

    @Override
    public Tuple get_next() throws IndexException, UnknownKeyTypeException {
        return get_next_BM();
    }

    public boolean delete_next() throws IndexException, UnknownKeyTypeException {

        return delete_next_BM();
    }

    public Tuple get_next_BM(){
        try {

            int position = getNextBMPosition();
            if(position < 0)
                return null;
            // tuple that needs to sent
            Tuple JTuple = new Tuple();
            // set the header which attribute types of the targeted columns
            JTuple.setHdr((short) givenTargetedCols.length, targetAttrTypes, targetShortSizes);
            JTuple = new Tuple(JTuple.size());
            JTuple.setHdr((short) givenTargetedCols.length, targetAttrTypes, targetShortSizes);
            if (index_only) {
                switch (indexAttrType.attrType) {
                    case AttrType.attrInteger:
                        // Assumed that col heap page will have only one entry
                        JTuple.setIntFld(1, Integer.parseInt(value));
                        break;
                    case AttrType.attrString:
                        JTuple.setStrFld(1, value);
                        break;
                    default:
                        throw new Exception("Attribute indexAttrType not supported");
                }
            } else {
                for (int i = 0; i < targetHeapFiles.length; i++) {
                    RID rid = targetHeapFiles[i].recordAtPosition(position);
                    Tuple record = targetHeapFiles[i].getRecord(rid);
                    switch (targetAttrTypes[i].attrType) {
                        case AttrType.attrInteger:
                            // Assumed that col heap page will have only one entry
                            JTuple.setIntFld(i + 1,
                                    Convert.getIntValue(0, record.getTupleByteArray()));
                            break;
                        case AttrType.attrString:
                            JTuple.setStrFld(i + 1,
                                    Convert.getStrValue(0, record.getTupleByteArray(),targetShortSizes[i]+2));
                            break;
                        default:
                            throw new Exception("Attribute indexAttrType not supported");
                    }
                }
            }
            // return the Tuple built by scanning the targeted columns
            return JTuple;

        }
        catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    private boolean delete_next_BM() throws IndexException, UnknownKeyTypeException {
        int position = getNextBMPosition();
        if(position < 0)
            return false;

        return columnarfile.markTupleDeleted(position);
    }

    public int getNextBMPosition(){
        try {

            if (scanCounter > counter) {
                PageId nextPage = currentBMPage.getNextPage();
                unpinPage(currentPageId, false);
                if (nextPage.pid != INVALID_PAGE) {
                    currentPageId.copyPageId(nextPage);
                    currentBMPage = new BMPage(pinPage(currentPageId));
                    counter = currentBMPage.getCounter();
                    bitMaps = BitSet.valueOf(currentBMPage.getBMpageArray());
                } else {
                    close();
                    return -1;
                }
            }
            while (scanCounter <= counter) {
                if (bitMaps.get(scanCounter)) {
                    int position = scanCounter;
                    scanCounter++;
                    return position;
                } else {
                    scanCounter++;
                }
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return -1;
    }

    public void close() throws IOException, JoinsException, SortException, IndexException, HFBufMgrException {
        if (!closeFlag) {
            closeFlag = true;
            unpinPage(currentPageId, false);
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

    /*
    * Gets the attribute string sizes from the coulumar file
    * and required for the seting the tuple header for the projection
    * */
    private void setTargetColumnStringSizes(short[] targetedCols) {
        short[] attributeStringSizes = columnarfile.getAttrSizes();

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
