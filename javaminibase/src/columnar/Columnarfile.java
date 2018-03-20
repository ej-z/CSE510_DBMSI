package columnar;



import global.AttrType;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.*;

import java.io.IOException;

public class Columnarfile {
    short numColumns;
    AttrType[] atype = null;
    short[] attrsizes;

    //Best way handle +2 bytes for strings instead of multiple ifs
    short[] asize;
    Heapfile[] hf = null;
    String fname = null;
    int tupleCnt = 0;
    Tuple hdr = null;
    RID hdrRid = null;
    //for fetching the file
    public Columnarfile(java.lang.String name) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
        Heapfile f = null;
        Scan scan = null;
        RID rid = null;
        fname = name;

        try {
            PageId pid = SystemDefs.JavabaseDB.get_file_entry(name+ ".hdr");
            if (pid == null) {
                throw new Exception("Columnar with the name: " + name + ".hdr doesn't exists");
            }

            f = new Heapfile(name + ".hdr") ;

            //Header tuple is organized this way
            //NumColumns, AttrType1, AttrSize1, AttrType2, AttrSize2,

            scan = f.openScan();
            hdrRid = new RID();
            Tuple hdr = scan.getNext(hdrRid);
            this.numColumns = (short) hdr.getIntFld(1);
            this.tupleCnt = hdr.getIntFld(2);
            atype = new AttrType[numColumns];
            attrsizes = new short[numColumns];
            asize=new short[numColumns];
            hf = new Heapfile[numColumns+1];
            hf[0] = f;
            int k = 0;
            for (int i = 0; i < numColumns; i++, k = k + 2) {
                atype[i] = new AttrType(hdr.getIntFld(3 + k));
                attrsizes[i] = (short)hdr.getIntFld(4 + k);
                asize[i] = attrsizes[i];
                if(atype[i].attrType == AttrType.attrString)
                    asize[i] += 2;
                hf[i+1] = new Heapfile(name + String.valueOf(i));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    /*Convention: hf[] => 0 for hdr file; the rest for tables. The implementation is modified from HFTest.java*/
    public Columnarfile(java.lang.String name, int numcols, AttrType[] types, short[] attrSizes) throws IOException, InvalidTupleSizeException, InvalidTypeException, FieldNumberOutOfBoundException, SpaceNotAvailableException, HFException, HFBufMgrException, InvalidSlotNumberException, HFDiskMgrException {
        RID rid1 = new RID();
        boolean status = true;
        try{
            hf = new Heapfile[numcols+1];
            //hf[0] for header file by default
            hf[0] = new Heapfile(name+".hdr");

        }
        catch(Exception e){
            status = false;
            e.printStackTrace();
        }
        if(status==true){
            //initializing member variables
            numColumns = (short)(numcols);
            this.fname = name;
            //tid?
            //tids = new TID[numColumns];
            //for TID
            atype = new AttrType[numColumns];
            attrsizes=new short[numColumns];
            asize=new short[numColumns];
            int k = 0;
            for(int i=0;i<numcols;i++){
                atype[i] = new AttrType(types[i].attrType);
                switch(types[i].attrType){
                    case 0:
                        asize[i] = attrsizes[i]=attrSizes[k];
                        asize[i] += 2;
                        k++;
                        break;
                    case 1:
                    case 2:
                        asize[i] = attrsizes[i]=4;
                        break;
                    case 3:
                        asize[i] = attrsizes[i]=1;
                        break;
                    case 4:
                        attrsizes[i]=0;
                        break;
                }
            }

            AttrType[] htypes = new AttrType[2+(numcols*2)];
            for(int i =0; i < htypes.length; i++) {
                htypes[i] = new AttrType(AttrType.attrInteger);
            }
            short[] hsizes = new short[0];
            hdr = new Tuple();
            hdr.setHdr((short)htypes.length, htypes, hsizes);
            int size = hdr.size();

            hdr = new Tuple(size);
            hdr.setHdr((short)htypes.length, htypes, hsizes);
            hdr.setIntFld(1, numcols);
            hdr.setIntFld(2, tupleCnt);
            int j = 0;
            for(int i = 0; i < numcols; i++,j=j+2){
                hdr.setIntFld(3+j, atype[i].attrType);
                hdr.setIntFld(4+j, attrsizes[i]);
            }
            hdrRid = hf[0].insertRecord(hdr.returnTupleByteArray());

            //allocating memory for the others
            try{
                for(int i=1;i<=numColumns;i++){
                    hf[i] = new Heapfile(name+String.valueOf(i-1));
                }
            }
            catch(Exception e){
                status = false;
                e.printStackTrace();
            }
        }
    }
    public void deleteColumnarFile() throws InvalidSlotNumberException, FileAlreadyDeletedException, InvalidTupleSizeException, HFBufMgrException, HFDiskMgrException, IOException{
        for(int i=0;i<=numColumns;i++){
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
        for(int i =0; i<numColumns;i++){

            int size = 6 + asize[i]; //6 bytes for count and offset

            AttrType[] ttype = new AttrType[1];
            ttype[0] = atype[i];
            short[] tsize = new short[1];
            tsize[0] = attrsizes[i];
            Tuple t = new Tuple(size);
            t.setHdr((short)1,ttype,tsize);
            byte[] data = t.getTupleByteArray();
            System.arraycopy(tuplePtr,offset,data,6,asize[i]);
            t.tupleInit(data,0,data.length);
            rids[i] = hf[i+1].insertRecord(t.getTupleByteArray());
            offset += asize[i];
        }
        TID tid = new TID(numColumns, tupleCnt, rids);
        tupleCnt++;
        hdr.setIntFld(2,tupleCnt);
        hf[0].updateRecord(hdrRid, hdr);
        return tid;
    }
    public Tuple getTuple(TID tidarg) throws Exception {

        Tuple result = new Tuple(getTupleSize());
        result.setHdr(numColumns, atype, getStrSize());
        byte[] data = result.getTupleByteArray();
        int offset = getOffset();
        for (int i = 0; i < numColumns; i++) {
            Tuple t = hf[i+1].getRecord(tidarg.recordIDs[i]);
            System.arraycopy(t.getTupleByteArray(),6,data,offset,asize[i]);
            offset += asize[i];
        }

        result.tupleInit(data, 0, data.length);

        return result;
    }
    public ValueClass getValue(TID tidarg, int column) throws Exception {

        Tuple t = hf[column + 1].getRecord(tidarg.recordIDs[column]);
        return ValueFactory.getValueClass(t, atype[column]);
    }
    public int getTupleCnt(){
        return tupleCnt;
    }
    public TupleScan openTupleScan() throws InvalidTupleSizeException, IOException{

        TupleScan result=new TupleScan(this);
        return result;
    }
    public TupleScan openTupleScan(short[] columns) throws InvalidTupleSizeException, IOException{

        TupleScan result=new TupleScan(this, columns);
        return result;
    }
    public Scan openColumnScan(int columnNo) throws Exception{
        Scan scanobj=null;
        if(columnNo < hf.length){
            scanobj = new Scan(hf[columnNo+1]);
        }
        else{

            throw new Exception("Invalid Column number");
        }

        return scanobj;
    }
    public boolean updateTuple(TID tidarg, Tuple newtuple){
        try{

            int offset = getOffset();
            byte[] tuplePtr = newtuple.getTupleByteArray();
            for(int i =0; i<numColumns;i++){

                int size = 6 + asize[i]; //6 bytes for count and offset

                AttrType[] ttype = new AttrType[1];
                ttype[0] = atype[i];
                short[] tsize = new short[1];
                tsize[0] = attrsizes[i];
                Tuple t = new Tuple(size);
                t.setHdr((short)1,ttype,tsize);
                byte[] data = t.getTupleByteArray();
                System.arraycopy(tuplePtr,offset,data,6,asize[i]);
                t.tupleInit(data,0,data.length);
                hf[i+1].updateRecord(tidarg.recordIDs[i], t);
                offset += asize[i];
            }
        }
        catch(Exception e){
            e.printStackTrace();
            return false;
        }
        return true;
    }
    public boolean  updateColumnofTuple(TID tidarg, Tuple newtuple, int column) {
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

    public int getTupleSize(){

        int size = getOffset();
        for(int i = 0; i < numColumns; i++){
            size += asize[i];
        }
        return size;
    }

    public short[] getStrSize(){

        int n = 0;
        for(int i = 0; i < numColumns; i++){
            if(atype[i].attrType == AttrType.attrString)
                n++;
        }

        short[] strSize = new short[n];
        int cnt = 0;
        for(int i = 0; i < numColumns; i++){
            if(atype[i].attrType == AttrType.attrString) {
                strSize[cnt++] = attrsizes[i];
            }
        }

        return strSize;
    }

    public int getOffset(){
        return 4 + (numColumns*2);
    }

    public int getOffset(int column){
        int offset = 4 + (numColumns*2);
        for(int i = 0; i < column; i++){
            offset += asize[i];
        }
        return offset;
    }

    public String getColumnarFileName() {
        return fname;
    }
}
