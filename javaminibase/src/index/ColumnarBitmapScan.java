package index;

import bitmap.BMPage;
import bitmap.BitMapFile;
import bitmap.BitmapFileScan;
import btree.PinPageException;
import columnar.Columnarfile;
import diskmgr.Page;
import global.*;
import heap.*;
import iterator.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class ColumnarBitmapScan extends Iterator implements GlobalConst{

    //private final String indName;
    private final AttrType indexAttrType;
    private final short str_sizes;
    private BitMapFile bmIndFile;
    private List<BitmapFileScan> scans;
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
    private CondExpr[] _bitmapSelects;
    private boolean index_only;
    private String value;
    private int _columnNo;

    public ColumnarBitmapScan(String relName,
                              int columnNo,
                              AttrType indexAttrType,
                              short str_sizes,
                              CondExpr[] bitmapSelects,
                              CondExpr[] selects,
                              boolean indexOnly,
                              short[] targetedCols) throws IndexException {

        targetHeapFiles = new Heapfile[targetedCols.length];
        targetAttrTypes = new AttrType[targetedCols.length];
        targetShortSizes = new short[targetedCols.length];
        givenTargetedCols = targetedCols;
        this.indexAttrType = indexAttrType;
        this.str_sizes = str_sizes;
        _selects = selects;
        index_only = indexOnly;
        _columnNo = columnNo;
        _bitmapSelects = bitmapSelects;
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
            scans = new ArrayList<>();
            for (String bmName : columnarfile.getAvailableBM(columnNo)){
                if(evalBMName(bmName)){
                    scans.add((new BitMapFile(bmName)).new_scan());
                }
            }
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
        int position = 0;
        while (position != -1) {
            try {

                position = get_next_position();
                if (position < 0)
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
                                        Convert.getStrValue(0, record.getTupleByteArray(), targetShortSizes[i] + 2));
                                break;
                            default:
                                throw new Exception("Attribute indexAttrType not supported");
                        }
                    }
                }
                if(PredEval.Eval(_selects,JTuple,null, targetAttrTypes,null))
                    return JTuple;

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private boolean delete_next_BM() throws IndexException, UnknownKeyTypeException {
        int position = get_next_position();
        if(position < 0)
            return false;

        return columnarfile.markTupleDeleted(position);
    }

    public int get_next_position(){
        try {

            if (scanCounter == 0 || scanCounter > counter) {
                bitMaps = new BitSet();
                for(BitmapFileScan s : scans){
                    if(s == null) {
                        for(BitmapFileScan ss : scans){
                            ss.close();
                            return -1;
                        }
                    }
                    bitMaps.or(s.get_next_bitmap());
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

    boolean evalBMName(String s) throws Exception {
        if(_selects == null)
            return true;

        short[] _sizes = new short[1];
        _sizes[0] = columnarfile.getAttrsizeforcolumn(_columnNo);
        AttrType[] _types = new AttrType[1];
        _types[0] = columnarfile.getAttrtypeforcolumn(_columnNo);

        byte[] data = new byte[6+_sizes[0]];

        String val = s.split(".")[3];
        Convert.setStrValue(val, 6,data);
        Tuple jTuple = new Tuple(data,6,data.length);
        _sizes[0] -= 2;

        jTuple.setHdr((short)1,_types, _sizes);

        if(PredEval.Eval(_bitmapSelects,jTuple,null,_types, null))
            return true;

        return false;
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
