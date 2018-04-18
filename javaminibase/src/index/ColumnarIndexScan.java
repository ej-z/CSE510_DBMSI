package index;

import bitmap.BMPage;
import bitmap.BitMapFile;
import btree.*;
import bufmgr.PageNotReadException;
import btree.PinPageException;
import columnar.Columnarfile;
import columnar.TID;
import columnar.ValueClass;
import diskmgr.Page;
import global.*;
import heap.*;
import iterator.*;
import org.w3c.dom.Attr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Set;


/**
 * Created by dixith on 3/18/18.
 */

public class ColumnarIndexScan extends Iterator{

    Iterator[] scan;
    private Heapfile[] targetHeapFiles = null;
    private AttrType[] targetAttrTypes = null;
    private short[] targetShortSizes = null;
    private short[] givenTargetedCols = null;
    private CondExpr[] _selects;
    private int index=0,max_pos=0;
    private Columnarfile columnarfile;
    /*
    * relName: columnarfileName
    * columnNos: number of columns
    * indexTypes: for the corresponding columnNos
    * index_selects: Conditional expressions for columns which has index on it
    * selects: Conditional expressions for columns that has no index on them
    * indexOnly: true/false
    * targetedCols: Columns on which the conditions should be applied
    * proj_list: Output fields
    **/
    public ColumnarIndexScan(String relName,
                             int[] columnNos,
                             IndexType[] indexTypes,
                             CondExpr[][] index_selects,
                             CondExpr[] selects,
                             boolean indexOnly,
                             short[] targetedCols,
                             FldSpec[] proj_list) throws IndexException, UnknownIndexTypeException, IOException, HFException, HFBufMgrException, HFDiskMgrException, SortException {


        _selects = selects;
        scan= new Iterator[columnNos.length];
        columnarfile = new Columnarfile(relName);
        for(int i = 0; i < columnNos.length; i++) {
            switch (indexTypes[i].indexType) {
                case IndexType.B_Index:
                    Iterator im = new ColumnarBTreeScan(columnarfile, columnNos[i], index_selects[i], indexOnly);
                    AttrType[] types = new AttrType[1];
                    types[0] = new AttrType(AttrType.attrInteger);
                    short[] sizes = new short[0];
                    scan[i] = new Sort(types, (short) 1, sizes, im, 1, new TupleOrder(TupleOrder.Ascending), 4, 4);
                    break;
                case IndexType.BitMapIndex:
                    scan[i] = new ColumnarBitmapScan(columnarfile, columnNos[i], index_selects[i], indexOnly);
                    break;
                case IndexType.None:
                default:
                    throw new UnknownIndexTypeException("Only BTree and Bitmap indices is supported so far");
            }
        }
    }

    @Override
    public Tuple get_next() throws Exception {
        int position = 0;
        while (position != -1) {
            try {
                if(scan.length>=1){
                    max_pos=scan[0].get_next_position();
                }
                position = get_next_position();
                if (position < 0)
                    return null;
                // tuple that needs to sent
                Tuple JTuple = new Tuple();
                // set the header which attribute types of the targeted columns
                JTuple.setHdr((short) givenTargetedCols.length, targetAttrTypes, targetShortSizes);
                JTuple = new Tuple(JTuple.size());
                JTuple.setHdr((short) givenTargetedCols.length, targetAttrTypes, targetShortSizes);
                for (int i = 0; i < targetHeapFiles.length; i++) {
                    Tuple record = targetHeapFiles[i].getRecord(position);
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
                if (PredEval.Eval(_selects, JTuple, null, targetAttrTypes, null))
                    return JTuple;

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
        //return scan.get_next();
    }

    /*
    * get the first matching position in all the scans and return the satisfying position one by one
    * */
    public int get_next_position() throws Exception {
        /*iterate through all the scan objects*/
        HashMap<Integer,Integer> result=new HashMap<>();
        boolean retvalue=fun_recurse(result,scan,max_pos,index);
        if(retvalue==true)
            return result.get(0);
        else
            return -1;
    }

    private boolean fun_recurse(HashMap<Integer, Integer> result, Iterator[] scan, int max_pos, int index) throws Exception {
        int tempos=-1,i=0;
        for(i=0;i<scan.length;i++){
            if(i!=index){
                tempos=scan[i].get_next_position();
                if(tempos!=-1){
                    if(tempos!=max_pos){
                        if(max_pos<tempos){
                            break;
                        }
                        else{
                            while(tempos!=-1 && max_pos>tempos){
                                tempos=scan[i].get_next_position();
                            }
                            if(tempos==-1){
                                return false;
                            }
                            if(tempos==max_pos){
                                result.put(i,tempos);
                            }
                            else{
                                break;
                            }
                        }
                    }
                    else{
                        result.put(i,tempos);
                    }
                }
            }
        }
        if(tempos!=-1){
            if(tempos>max_pos){
                max_pos=tempos;index=i;
                result.put(index,max_pos);
                return fun_recurse(result,scan,max_pos,index);
            }
        }
        Set<Integer> keyvalue=result.keySet();
        int prev=-1;
        for(Integer j:keyvalue){
            if(prev!=-1){
                if(prev==result.get(j)){
                    prev=result.get(j);
                }
                else{
                    return false;
                }
            }
            else{
                prev=result.get(j);
            }
        }
        if(keyvalue.size()<scan.length){
            return false;
        }
        return true;
    }

    public boolean delete_next() throws Exception {

        /*switch (_index.indexType) {
            case IndexType.B_Index:
                return ((ColumnarBTreeScan)scan).delete_next();
            case IndexType.BitMapIndex:
                return ((ColumnarBitmapScan)scan).delete_next();
            case IndexType.None:
            default:
                throw new UnknownIndexTypeException("Only BTree index is supported so far");

        }*/
        return true;
    }

    public void close(){
        try {
            for(int i=0;i<scan.length;i++)
            scan[i].close();
        } catch (Exception e) {
            e.printStackTrace();
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
