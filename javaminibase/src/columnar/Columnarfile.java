package columnar;

import global.AttrType;
import heap.Scan;
import heap.Tuple;

public class Columnarfile {

    static int numColumns;
    AttrType[] type;

    public Columnarfile(String name, int numColumns, AttrType[] type){

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

}
