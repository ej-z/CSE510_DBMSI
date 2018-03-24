package columnar;

import bitmap.BitMapFile;
import btree.*;
import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import global.*;
import heap.*;
import iterator.FileScanException;
import iterator.InvalidRelation;
import iterator.SortException;
import iterator.TupleUtilsException;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import static tests.TestDriver.FAIL;
import static tests.TestDriver.OK;

public class Columnarfile {
    short numColumns;
    AttrType[] atype = null;
    short[] attrsizes;

    //Best way handle +2 bytes for strings instead of multiple ifs
    short[] asize;
    Heapfile[] hf = null;
    String fname = null;
    //int tupleCnt = 0;
    Tuple hdr = null;
    RID hdrRid = null;
    HashMap<String, Integer> columnMap;
    HashSet<String> BTNames;
    HashSet<String> BMNames;
    HashMap<String, BTreeFile> BTMap;
    HashMap<String, BitMapFile> BMMap;

    //for fetching the file
    public Columnarfile(java.lang.String name) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
        Heapfile f = null;
        Scan scan = null;
        RID rid = null;
        fname = name;
        columnMap = new HashMap<>();
        try {
            PageId pid = SystemDefs.JavabaseDB.get_file_entry(name + ".hdr");
            if (pid == null) {
                throw new Exception("Columnar with the name: " + name + ".hdr doesn't exists");
            }

            f = new Heapfile(name + ".hdr");

            //Header tuple is organized this way
            //NumColumns, AttrType1, AttrSize1, AttrName1, AttrType2, AttrSize2, AttrName3...

            scan = f.openScan();
            hdrRid = new RID();
            Tuple hdr = scan.getNext(hdrRid);
            this.numColumns = (short) hdr.getIntFld(1);
            atype = new AttrType[numColumns];
            attrsizes = new short[numColumns];
            asize = new short[numColumns];
            hf = new Heapfile[numColumns + 1];
            hf[0] = f;
            int k = 0;
            for (int i = 0; i < numColumns; i++, k = k + 3) {
                atype[i] = new AttrType(hdr.getIntFld(2 + k));
                attrsizes[i] = (short) hdr.getIntFld(3 + k);
                String colName = hdr.getStrFld(4 + k);
                columnMap.put(colName, i);
                asize[i] = attrsizes[i];
                if (atype[i].attrType == AttrType.attrString)
                    asize[i] += 2;
                hf[i + 1] = new Heapfile(name + String.valueOf(i));
            }

            pid = SystemDefs.JavabaseDB.get_file_entry(name + ".idx");
            if (pid != null) {
                f = new Heapfile(name + ".idx");
                BTNames = new HashSet<>();
                BMNames = new HashSet<>();
                scan = f.openScan();
                Tuple t = f.getRecord(null);
                while (t != null){
                    int indexType = t.getIntFld(1);
                    if(indexType == 0)
                        BTNames.add(t.getStrFld(2));
                    else if(indexType == 1)
                        BMNames.add(t.getStrFld(2));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*Convention: hf[] => 0 for hdr file; the rest for tables. The implementation is modified from HFTest.java*/
    public Columnarfile(java.lang.String name, int numcols, AttrType[] types, short[] attrSizes, String[] colnames) throws IOException, InvalidTupleSizeException, InvalidTypeException, FieldNumberOutOfBoundException, SpaceNotAvailableException, HFException, HFBufMgrException, InvalidSlotNumberException, HFDiskMgrException {
        RID rid1 = new RID();
        boolean status = true;
        columnMap = new HashMap<>();
        try {
            hf = new Heapfile[numcols + 1];
            //hf[0] for header file by default
            hf[0] = new Heapfile(name + ".hdr");

        } catch (Exception e) {
            status = false;
            e.printStackTrace();
        }
        if (status == true) {
            numColumns = (short) (numcols);
            this.fname = name;
            atype = new AttrType[numColumns];
            attrsizes = new short[numColumns];
            asize = new short[numColumns];
            int k = 0;
            for (int i = 0; i < numcols; i++) {
                atype[i] = new AttrType(types[i].attrType);
                switch (types[i].attrType) {
                    case 0:
                        asize[i] = attrsizes[i] = attrSizes[k];
                        asize[i] += 2;
                        k++;
                        break;
                    case 1:
                    case 2:
                        asize[i] = attrsizes[i] = 4;
                        break;
                    case 3:
                        asize[i] = attrsizes[i] = 1;
                        break;
                    case 4:
                        attrsizes[i] = 0;
                        break;
                }
            }

            AttrType[] htypes = new AttrType[2 + (numcols * 3)];
            htypes[0] = new AttrType(AttrType.attrInteger);
            for (int i = 1; i < htypes.length-1; i = i + 3) {
                htypes[i] = new AttrType(AttrType.attrInteger);
                htypes[i + 1] = new AttrType(AttrType.attrInteger);
                htypes[i + 2] = new AttrType(AttrType.attrString);
            }
            htypes[htypes.length - 1] = new AttrType(AttrType.attrInteger);
            short[] hsizes = new short[numcols];
            for (int i = 0; i < numcols; i++) {
                hsizes[i] = 20; //column name can't be more than 20 chars
            }
            hdr = new Tuple();
            hdr.setHdr((short) htypes.length, htypes, hsizes);
            int size = hdr.size();

            hdr = new Tuple(size);
            hdr.setHdr((short) htypes.length, htypes, hsizes);
            hdr.setIntFld(1, numcols);
            int j = 0;
            for (int i = 0; i < numcols; i++, j = j + 3) {
                hdr.setIntFld(2 + j, atype[i].attrType);
                hdr.setIntFld(3 + j, attrsizes[i]);
                hdr.setStrFld(4 + j, colnames[i]);
                columnMap.put(colnames[i], i);
            }
            hdrRid = hf[0].insertRecord(hdr.returnTupleByteArray());

            //allocating memory for the others
            try {
                for (int i = 1; i <= numColumns; i++) {
                    hf[i] = new Heapfile(name + String.valueOf(i - 1));
                }
            } catch (Exception e) {
                status = false;
                e.printStackTrace();
            }
        }
    }

    public void deleteColumnarFile() throws InvalidSlotNumberException, FileAlreadyDeletedException, InvalidTupleSizeException, HFBufMgrException, HFDiskMgrException, IOException {
        for (int i = 0; i <= numColumns; i++) {
            hf[i].deleteFile();
        }
        hf = null;
        atype = null;
        fname = null;
        numColumns = 0;
    }

    //Assumption: tupleptr contains header information.
    public TID insertTuple(byte[] tuplePtr) throws Exception {

        int offset = getOffset();
        RID[] rids = new RID[numColumns];
        int position = 0;
        for (int i = 0; i < numColumns; i++) {

            int size = 6 + asize[i]; //6 bytes for count and offset

            AttrType[] ttype = new AttrType[1];
            ttype[0] = atype[i];
            short[] tsize = new short[1];
            tsize[0] = attrsizes[i];
            Tuple t = new Tuple(size);
            t.setHdr((short) 1, ttype, tsize);
            byte[] data = t.getTupleByteArray();
            System.arraycopy(tuplePtr, offset, data, 6, asize[i]);
            t.tupleInit(data, 0, data.length);
            rids[i] = hf[i + 1].insertRecord(t.getTupleByteArray());
            offset += asize[i];

            String btIndexname = getBTName(i);
            String bmIndexname = getBMName(i, ValueFactory.getValueClass(t, atype[i]));
            if(BTMap == null || !BTNames.contains(btIndexname)){
                setBTFile(btIndexname);
            }
            if(BMMap == null || !BMNames.contains(bmIndexname)){
                setBMFile(bmIndexname);
            }
            if (BTMap != null && BTMap.containsKey(btIndexname)) {
                BTMap.get(btIndexname).insert(KeyFactory.getKeyClass(t, atype[i]), rids[i]);
            }
            if (BMMap != null && BMMap.containsKey(bmIndexname)) {
                position = hf[i + 1].positionOfRecord(rids[i]);
                BMMap.get(bmIndexname).insert(position);
            }
        }
        position = hf[1].positionOfRecord(rids[0]);
        TID tid = new TID(numColumns, position, rids);
        return tid;
    }

    public Tuple getTuple(TID tidarg) throws Exception {

        Tuple result = new Tuple(getTupleSize());
        result.setHdr(numColumns, atype, getStrSize());
        byte[] data = result.getTupleByteArray();
        int offset = getOffset();
        for (int i = 0; i < numColumns; i++) {
            Tuple t = hf[i + 1].getRecord(tidarg.recordIDs[i]);
            System.arraycopy(t.getTupleByteArray(), 6, data, offset, asize[i]);
            offset += asize[i];
        }

        result.tupleInit(data, 0, data.length);

        return result;
    }

    public ValueClass getValue(TID tidarg, int column) throws Exception {

        Tuple t = hf[column + 1].getRecord(tidarg.recordIDs[column]);
        return ValueFactory.getValueClass(t, atype[column]);
    }

    public int getTupleCnt() throws HFBufMgrException, IOException, HFDiskMgrException, InvalidSlotNumberException, InvalidTupleSizeException {
        return hf[1].getRecCnt();
    }

    public TupleScan openTupleScan() throws Exception {

        TupleScan result = new TupleScan(this);
        return result;
    }

    public TupleScan openTupleScan(short[] columns) throws InvalidTupleSizeException, IOException, FileScanException, TupleUtilsException, FileIOException, SortException, InvalidPageNumberException, InvalidRelation, DiskMgrException {

        TupleScan result = new TupleScan(this, columns);
        return result;
    }

    public Scan openColumnScan(int columnNo) throws Exception {
        Scan scanobj = null;
        if (columnNo < hf.length) {
            scanobj = new Scan(hf[columnNo + 1]);
        } else {

            throw new Exception("Invalid Column number");
        }

        return scanobj;
    }

    public boolean updateTuple(TID tidarg, Tuple newtuple) {
        try {

            int offset = getOffset();
            byte[] tuplePtr = newtuple.getTupleByteArray();
            for (int i = 0; i < numColumns; i++) {

                int size = 6 + asize[i]; //6 bytes for count and offset

                AttrType[] ttype = new AttrType[1];
                ttype[0] = atype[i];
                short[] tsize = new short[1];
                tsize[0] = attrsizes[i];
                Tuple t = new Tuple(size);
                t.setHdr((short) 1, ttype, tsize);
                byte[] data = t.getTupleByteArray();
                System.arraycopy(tuplePtr, offset, data, 6, asize[i]);
                t.tupleInit(data, 0, data.length);
                hf[i + 1].updateRecord(tidarg.recordIDs[i], t);
                offset += asize[i];
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public boolean updateColumnofTuple(TID tidarg, Tuple newtuple, int column) {
        try {
            int offset = getOffset(column);
            byte[] tuplePtr = newtuple.getTupleByteArray();

            int size = 6 + asize[column]; //6 bytes for count and offset

            AttrType[] ttype = new AttrType[1];
            ttype[0] = atype[column];
            short[] tsize = new short[1];
            tsize[0] = attrsizes[column];
            Tuple t = new Tuple(size);
            t.setHdr((short) 1, ttype, tsize);
            byte[] data = t.getTupleByteArray();
            System.arraycopy(tuplePtr, offset, data, 6, asize[column]);
            t.tupleInit(data, 0, data.length);
            hf[column + 1].updateRecord(tidarg.recordIDs[column], t);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public int getTupleSize() {

        int size = getOffset();
        for (int i = 0; i < numColumns; i++) {
            size += asize[i];
        }
        return size;
    }

    public short[] getStrSize() {

        int n = 0;
        for (int i = 0; i < numColumns; i++) {
            if (atype[i].attrType == AttrType.attrString)
                n++;
        }

        short[] strSize = new short[n];
        int cnt = 0;
        for (int i = 0; i < numColumns; i++) {
            if (atype[i].attrType == AttrType.attrString) {
                strSize[cnt++] = attrsizes[i];
            }
        }

        return strSize;
    }

    public int getOffset() {
        return 4 + (numColumns * 2);
    }

    public int getOffset(int column) {
        int offset = 4 + (numColumns * 2);
        for (int i = 0; i < column; i++) {
            offset += asize[i];
        }
        return offset;
    }

    public String getColumnarFileName() {
        return fname;
    }

    boolean createBTreeIndex(int columnNo) throws Exception {
        String indexName = getBTName(columnNo);

        int keyType = atype[columnNo - 1].attrType;
        int keySize = asize[columnNo - 1];
        int deleteFashion = 0;
        BTreeFile bTreeFile = new BTreeFile(indexName, keyType, keySize, deleteFashion);
        Scan columnScan = openColumnScan(columnNo);
        RID rid = new RID();
        Tuple tuple;
        while (true) {
            tuple = columnScan.getNext(rid);
            if (tuple == null) {
                break;
            }
            if (keyType == AttrType.attrInteger) {
                bTreeFile.insert(new IntegerKey(tuple.getIntFld(1)), rid);
            } else {
                bTreeFile.insert(new StringKey(tuple.getStrFld(1)), rid);
            }
        }
        columnScan.closescan();

        addIndexToColumnar(0, indexName);
        BTNames.add(indexName);

        return true;
    }


    public boolean createBitMapIndex(int columnNo, ValueClass value) throws Exception {
        Scan columnScan = openColumnScan(columnNo);
        String indexName = getBMName(columnNo, value);
        BitMapFile bitMapFile = new BitMapFile(indexName, this, columnNo, value);
        RID rid = new RID();
        Tuple tuple;
        int position = 1;
        while (true) {
            tuple = columnScan.getNext(rid);
            if (tuple == null) {
                break;
            }
            ValueClass valueClass;
            if (value instanceof ValueInt) {
                valueClass = new ValueInt(tuple.getIntFld(1));
                if (valueClass.getValue() == value.getValue()) {
                    bitMapFile.insert(position);
                } else {
                    bitMapFile.delete(position);
                }
            } else {
                valueClass = new ValueString(tuple.getStrFld(1));
                if (valueClass.toString().equals(value.toString())) {
                    bitMapFile.insert(position);
                } else {
                    bitMapFile.delete(position);
                }
            }
            position++;
        }
        columnScan.closescan();

        addIndexToColumnar(1, indexName);
        BMNames.add(indexName);

        return true;
    }

    public boolean markTupleDeleted(TID tidarg) {
        String name = getDeletedFileName();
        try {
            Heapfile f = new Heapfile(name);
            Integer pos = tidarg.position;
            AttrType[] types = new AttrType[1];
            types[0] = new AttrType(AttrType.attrInteger);
            short[] sizes = new short[0];
            Tuple t = new Tuple(10);
            t.setHdr((short)1,types, sizes);
            t.setIntFld(1, pos);
            f.insertRecord(t.getTupleByteArray());

            for (int i = 0; i < tidarg.numRIDs; i++) {
                RID rid = hf[i + 1].recordAtPosition(tidarg.position);
                Tuple tuple = hf[i + 1].getRecord(rid);
                ValueClass valueClass;
                KeyClass keyClass;
                if (atype[i + 1].attrType == AttrType.attrInteger) {
                    valueClass = new ValueInt(tuple.getIntFld(1));
                    keyClass = new IntegerKey(tuple.getIntFld(1));
                } else {
                    valueClass = new ValueString(tuple.getStrFld(1));
                    keyClass = new StringKey(tuple.getStrFld(1));
                }

                String bTreeFileName = getBTName(i + 1);
                String bitMapFileName = getBMName(i + 1, valueClass);
                if (BTNames.contains(bTreeFileName)) {
                    BTreeFile bTreeFile = BTMap.get(bTreeFileName);
                    bTreeFile.Delete(keyClass, tidarg.recordIDs[i]);
                }
                if (BMNames.contains(bitMapFileName)) {
                    BitMapFile bitMapFile = BMMap.get(bitMapFileName);
                    bitMapFile.delete(tidarg.position);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public boolean purgeAllDeletedTuples() {
        boolean status = OK;
        Scan scan = null;
        RID rid = new RID();
        Heapfile f = null;
        int pos_marked;
        boolean done = false;
        try {
            f = new Heapfile(getDeletedFileName());
        } catch (Exception e) {
            status = FAIL;
            System.err.println(" Could not open heapfile");
            e.printStackTrace();
        }

        if (status == OK) {
            try {
                scan = f.openScan();
            } catch (Exception e) {
                status = FAIL;
                System.err.println("*** Error opening scan\n");
                e.printStackTrace();
            }
        }

        if (status == OK) {
            int i = 0;
            Tuple tuple;
            while (!done) {
                try {
                    tuple = scan.getNext(rid);
                    if (tuple == null) {
                        done = true;
                        return true;
                    }
                    pos_marked = Convert.getIntValue(6, tuple.getTupleByteArray());
                    for (int j = 1; j <= numColumns; j++) {
                        rid = hf[i].recordAtPosition(pos_marked);
                        status = hf[i].deleteRecord(rid);
                    }
                    //destroy th file
                    f.deleteFile();

                } catch (Exception e) {
                    e.printStackTrace();
                    return false;
                }
            }
        }
        return true;
    }


    public AttrType[] getAttributes() {
        return atype;
    }

    public short[] getStringSizes() {
        return attrsizes;
    }

    public int getAttributePosition(String name){
        return columnMap.get(name);
    }

    public String getBTName(int columnNo){
        return "BT"+fname+columnNo;
    }

    public String getBMName(int columnNo, ValueClass value){
        return "BM"+fname+columnNo+value.toString();
    }

    public String getDeletedFileName(){
        return fname+"-markedTupleDeleted";
    }

    private boolean addIndexToColumnar(int indexType, String indexName){

        try {
            AttrType[] itypes = new AttrType[2];
            itypes[0] = new AttrType(AttrType.attrInteger);
            itypes[1] = new AttrType(AttrType.attrString);
            short[] isizes = new short[1];
            isizes[0] = 20; //index name can't be more than 20 chars
            Tuple t = new Tuple();
            t.setHdr((short) 2, itypes, isizes);
            int size = t.size();
            t = new Tuple(size);
            t.setHdr((short) 2, itypes, isizes);
            t.setIntFld(1, indexType);
            t.setStrFld(2, indexName);
            Heapfile f = new Heapfile(indexName);
            f.insertRecord(t.getTupleByteArray());

            if(indexType == 0 && BTNames == null){
                BTNames = new HashSet<>();
            }
            else if(indexType == 1 && BMNames == null){
                BMNames = new HashSet<>();
            }
        } catch (Exception e){
            e.printStackTrace();
            return false;
        }
        return true;
    }

    private void setBTFile(String name) throws Exception{

        PageId pid = SystemDefs.JavabaseDB.get_file_entry(name);
        if (pid == null) {
            return;
        }

        BTMap.put(name, new BTreeFile(name));
    }

    private void setBMFile(String name) throws Exception {

        PageId pid = SystemDefs.JavabaseDB.get_file_entry(name);
        if (pid == null) {
            return;
        }

        BMMap.put(name, new BitMapFile(name));
    }
}
