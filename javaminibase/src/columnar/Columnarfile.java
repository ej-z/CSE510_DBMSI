package columnar;

import global.*;
import heap.*;
import iterator.FileScan;

import java.io.IOException;

public class Columnarfile {

    int numColumns;
    AttrType[] type;

    private String _name;

    /**
     * Constructor to retrieve existing columnar
     * @param name
     */
    public Columnarfile(String name){

        this._name = name;
        try {
            PageId pid = SystemDefs.JavabaseDB.get_file_entry(name);

            if (pid == null) {
                throw new Exception("Columnar with the name: " + name + " doesn't exists");
            }

            Heapfile columnar = new Heapfile(_name);
            Scan scan = columnar.openScan();
            Tuple t = new Tuple(scan.getNext(new RID()));
            this.numColumns = t.getIntFld(1);
            this.type = new AttrType[numColumns];
            for(int i = 0; i < numColumns; i++){
                type[i] = new AttrType(t.getIntFld(i+2));
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * Constructor to create new columnar
     * @param name
     */
    public Columnarfile(String name, int n, AttrType[] t){

        try {
            PageId pid = SystemDefs.JavabaseDB.get_file_entry(name);

            if(pid != null){
                throw new Exception("Columnar with the name: "+name+" already exists");
            }

            this._name = name;
            this.numColumns = n;
            this.type = new AttrType[numColumns];
            for(int i = 0; i < numColumns; i++) {
                this.type[i] = new AttrType(t[i].attrType);
            }

            //Create new heap file this will store num of columns, and attr types.
            //It should ideally hold the TIDs too.
            AttrType[] ctypes = new AttrType[1+numColumns];
            ctypes[0] = new AttrType(AttrType.attrInteger);
            for(int i = 0; i < numColumns; i++){
                ctypes[i+1] = new AttrType(AttrType.attrInteger);
            }

            short[] csizes = new short[0];
            Tuple ct = new Tuple();
            ct.setHdr((short) (1+numColumns), ctypes, csizes);
            int s = ct.size();
            ct = new Tuple(s);
            ct.setHdr((short) (1+numColumns), ctypes, csizes);
            ct.setIntFld(1, numColumns);
            for(int i = 0; i < numColumns; i++){
                int h = type[i].attrType;
                ct.setIntFld(i+2, h);
            }

            Heapfile columnar = new Heapfile(_name);
            columnar.insertRecord(ct.returnTupleByteArray());

            //Create header file for columnar. This should have TID information
            Heapfile header = new Heapfile(getHeaderName());

            //Creating a heapfile for each column and inserting relevant data to header
            for(int i = 0; i < numColumns; i++){
                String columnName = _name+(i+1);
                new Heapfile(columnName);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    void deleteColumnarFile(){

    }
    TID insertTuple(byte[] tuplePtr){
        return null;
    }
    Tuple getTuple(TID tid){
        return null;
    }
    ValueClass getValue(TID tid, int column){
        return null;
    }
    int getTupleCnt(){
        return -1;
    }
    //TupleScan openTupleScan();
    Scan openColumnScan(int columnNo){
        return null;
    }
    boolean updateTuple(TID tid, Tuple newtuple){
        return false;
    }
    boolean updateColumnofTuple(TID tid, Tuple newtuple, int column){
        return false;
    }
    boolean createBTreeIndex(int column){
        return false;
    }
    boolean createBitMapIndex(int columnNo, ValueClass value){
        return false;
    }
    boolean markTupleDeleted(TID tid){
        return false;
    }
    boolean purgeAllDeletedTuples(){
        return false;
    }

    private String getHeaderName(){
        return _name+".hdr";
    }

    public int getNumColumns() {
        return numColumns;
    }

    public AttrType[] getType() {
        return type;
    }

}
