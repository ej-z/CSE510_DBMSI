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
import java.util.BitSet;


/**
 * Created by dixith on 3/18/18.
 */

public class ColumnarIndexScan extends Iterator{

    Iterator scan;
    IndexType _index;

    public ColumnarIndexScan(IndexType index,
                             String relName,
                             String indName,
                             AttrType indexAttrType,
                             short str_sizes,
                             CondExpr[] selects,
                             boolean indexOnly,
                             short[] targetedCols) throws IndexException, UnknownIndexTypeException {



        _index = index;
        switch (_index.indexType) {
            case IndexType.B_Index:
                scan = new ColumnarBTreeScan(relName, indName, indexAttrType, str_sizes, selects, indexOnly, targetedCols);
                break;
            case IndexType.BitMapIndex:
                scan = new ColumnarBitmapScan(relName, indName, indexAttrType, str_sizes, selects, indexOnly, targetedCols);
                break;
            case IndexType.None:
            default:
                throw new UnknownIndexTypeException("Only BTree index is supported so far");

        }
    }

    @Override
    public Tuple get_next() throws Exception {

        return scan.get_next();
    }

    public boolean delete_next() throws Exception {

        switch (_index.indexType) {
            case IndexType.B_Index:
                return ((ColumnarBTreeScan)scan).delete_next();
            case IndexType.BitMapIndex:
                return ((ColumnarBitmapScan)scan).delete_next();
            case IndexType.None:
            default:
                throw new UnknownIndexTypeException("Only BTree index is supported so far");

        }
    }

    public void close(){
        try {
            scan.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
