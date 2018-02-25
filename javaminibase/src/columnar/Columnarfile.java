package columnar;

import global.AttrType;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.*;

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
            Tuple t = scan.getNext(new RID());
            this.numColumns = t.getIntFld(1);
            this.type = new AttrType[numColumns];
            for(int i = 0; i < numColumns; i++){
                type[i] = new AttrType(t.getIntFld(2+i));
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
    public Columnarfile(String name, int numColumns, AttrType[] type){

        this._name = name;
        this.numColumns = numColumns;
        this.type = new AttrType[numColumns];
        try {
            PageId pid = SystemDefs.JavabaseDB.get_file_entry(name);

            if(pid != null){
                throw new Exception("Columnar with the name: "+name+" already exists");
            }

            //Create new heap file this will store num of columns, and attr types.
            //It should ideally hold the TIDs too.
            AttrType[] ctypes = new AttrType[1+numColumns];
            ctypes[1] = new AttrType(AttrType.attrInteger);
            for(int i = 0; i < numColumns; i++){
                ctypes[i+2] = new AttrType(AttrType.attrInteger);
            }

            short[] csizes = new short[0];
            Tuple ct = new Tuple();
            ct.setHdr((short) (1+numColumns), ctypes, csizes);
            int s = ct.size();

            Tuple k = new Tuple(s);
            k.setIntFld(1, numColumns);
            for(int i = 0; i < numColumns; i++){
                k.setIntFld(i+1, type[i].attrType);
            }
            Heapfile columnar = new Heapfile(_name);
            columnar.insertRecord(k.returnTupleByteArray());

            //Create header file for columnar. This should have columnname and position
            //Having this file just to store this information doesn't make sense,
            //as column files can just be retrieved using get_file_entry(name+column_number);
            AttrType[] htypes = new AttrType[2];
            htypes[0] = new AttrType(AttrType.attrInteger);
            htypes[1] = new AttrType(AttrType.attrString);

            short[] hsizes = new short[1];
            hsizes[0] = 20; //This means that the length of heap file names cannot be >20
            Tuple ht = new Tuple();
            ht.setHdr((short) 2, htypes, hsizes);
            int size = ht.size();
            Heapfile header = new Heapfile(getHeaderName());

            //Creating a heapfile for each column and inserting relevant data to header
            for(int i = 0; i < numColumns; i++){
                this.type[i] = new AttrType(type[i].attrType);
                String columnName = _name+(i+1);
                Tuple t = new Tuple(size);
                t.setIntFld(1, i+1);
                t.setStrFld(2, columnName);
                header.insertRecord(t.returnTupleByteArray());
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

}
