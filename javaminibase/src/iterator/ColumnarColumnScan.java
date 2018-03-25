package iterator;

import bitmap.BitMapFile;
import btree.BTreeFile;
import btree.IntegerKey;
import btree.KeyClass;
import btree.StringKey;
import bufmgr.PageNotReadException;
import columnar.*;
import global.*;
import heap.*;

import java.io.IOException;

public class ColumnarColumnScan extends Iterator {

    private Columnarfile columnarfile;
    private Scan scan;
    private CondExpr[] OutputFilter;
    public FldSpec[] perm_mat;
    private AttrType[] _in1 = null;
    private short[] s_sizes = null;
    private Heapfile[] targetHeapFiles = null;
    private AttrType[] targetAttrTypes = null;
    private short[] targetShortSizes = null;
    private short[] givenTargetedCols = null;
    private short _tuplesize;
    private int _attrsize;
    int _columnNo;
    Sort deletedTuples;
    private int currDeletePos = -1;

    public ColumnarColumnScan(String file_name,
                              int columnNo,
                              AttrType attrType,
                              short strSize,
                              short[] targetedCols,
                              CondExpr[] outFilter) throws FileScanException, TupleUtilsException, IOException, InvalidRelation {


        targetHeapFiles = new Heapfile[targetedCols.length];
        targetAttrTypes = new AttrType[targetedCols.length];
        targetShortSizes = new short[targetedCols.length];
        givenTargetedCols = targetedCols;
        OutputFilter = outFilter;
        _in1 = new AttrType[1];
        _in1[0] = attrType;
        _columnNo = columnNo;
        try {
            columnarfile = new Columnarfile(file_name);
            setTargetHeapFiles(file_name, targetedCols);
            setTargetColumnAttributeTypes(targetedCols);
            setTargetColumnStringSizes(targetedCols);
            _attrsize = columnarfile.getAttrSizes()[columnNo];
            if(attrType.attrType == AttrType.attrString)
                _attrsize += 2;
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        try {
            scan = columnarfile.openColumnScan(columnNo);
            PageId pid = SystemDefs.JavabaseDB.get_file_entry(columnarfile.getDeletedFileName());
            if (pid != null) {
                _in1 = new AttrType[1];
                _in1[0] = new AttrType(AttrType.attrInteger);
                s_sizes = new	short[1];
                s_sizes[0] = strSize;
                Tuple t = new Tuple();
                t.setHdr((short)1, _in1, s_sizes);
                _tuplesize = t.size();
                FldSpec[] projlist = new FldSpec[1];
                projlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
                FileScan fs = new FileScan(columnarfile.getDeletedFileName(), _in1, s_sizes, (short)1, 1, projlist, null);
                deletedTuples = new Sort(_in1, (short) 1, s_sizes, fs, 1, new TupleOrder(TupleOrder.Ascending), 4, 10);
            }
        } catch (Exception e) {
            throw new FileScanException(e, "openScan() failed");
        }

    }

    public FldSpec[] show() {
        return perm_mat;
    }

    /**
     * @return the result tuple
     * @throws JoinsException                 some join exception
     * @throws IOException                    I/O errors
     * @throws InvalidTupleSizeException      invalid tuple size
     * @throws InvalidTypeException           tuple type not valid
     * @throws PageNotReadException           exception from lower layer
     * @throws PredEvalException              exception from PredEval class
     * @throws UnknowAttrType                 attribute type unknown
     * @throws FieldNumberOutOfBoundException array out of bounds
     * @throws WrongPermat                    exception for wrong FldSpec argument
     */
    public Tuple get_next()
            throws Exception {

        int position = getNextPosition();
        if (position < 0)
            return null;
        //tuple1.setHdr(in1_len, _in1, s_sizes);
        Tuple JTuple = null;
        try {
            JTuple = new Tuple();
            JTuple.setHdr((short) givenTargetedCols.length, targetAttrTypes, targetShortSizes);
            JTuple = new Tuple(JTuple.size());
            JTuple.setHdr((short) givenTargetedCols.length, targetAttrTypes, targetShortSizes);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

        for (int i = 0; i < targetHeapFiles.length; i++) {
            RID r = targetHeapFiles[i].recordAtPosition(position);
            Tuple record = targetHeapFiles[i].getRecord(r);
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

        return JTuple;
    }

    public boolean delete_next()
            throws Exception{

        int position = getNextPosition();
        if (position < 0)
            return false;
        return columnarfile.markTupleDeleted(position);
    }

    private int getNextPosition()throws Exception {
        RID rid = new RID();

        Tuple t = null;
        while (true) {
            if ((t = scan.getNext(rid)) == null) {
                return -1;
            }

            Tuple tuple1= new Tuple(_tuplesize);
            tuple1.setHdr((short)1, _in1, s_sizes);
            tuple1 = new Tuple(tuple1.size());
            byte[] data = tuple1.getTupleByteArray();
            System.arraycopy(t.getTupleByteArray(), 0, data, 6, _attrsize);
            if (PredEval.Eval(OutputFilter, t, null, _in1, null) == true) {
                int position = columnarfile.getColumn(_columnNo).positionOfRecord(rid);
                if(deletedTuples != null && position > currDeletePos){
                    while (true){
                        Tuple dtuple = deletedTuples.get_next();
                        if(dtuple == null)
                            break;
                        currDeletePos = dtuple.getIntFld(1);
                        if(currDeletePos >= position)
                            break;
                    }
                }
                if(position == currDeletePos){
                    deletedTuples.get_next();
                    continue;
                }
                return position;
            }
        }
    }

    /**
     * implement the abstract method close() from super class Iterator
     * to finish cleaning up
     */
    public void close() {

        if (!closeFlag) {
            scan.closescan();
            closeFlag = true;
        }
    }

    /*
    * Gets the attribute string sizes from the coulumar file
    * and required for the seting the tuple header for the projection
    * */
    private void setTargetColumnStringSizes(short[] targetedCols) {
        short[] attributeStringSizes = columnarfile.getAttrSizes();

        for (int i = 0; i < targetAttrTypes.length; i++) {
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

        for (int i = 0; i < targetAttrTypes.length; i++) {
            targetAttrTypes[i] = attributes[targetedCols[i]];
        }
    }

    // open the targeted column heap files and store those reference for scanning
    private void setTargetHeapFiles(String relName, short[] targetedCols) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
        for (int i = 0; i < targetedCols.length; i++) {
            targetHeapFiles[i] = new Heapfile(relName + Short.toString(targetedCols[i]));
        }
    }
}
