package interfaces;

import bufmgr.*;
import columnar.Columnarfile;
import diskmgr.PCounter;
import global.AttrType;
import global.SystemDefs;
import global.TupleOrder;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Tuple;
import iterator.*;
import java.io.IOException;

import static global.GlobalConst.NUMBUF;

public class SortInterface {

    public static void main(String[] args) throws Exception {
        // Query Skeleton: COLUMNDB COLUMNARFILE COLUMNNAME SORTORDER BUFFSIZE
        // Example Query: testColumnDB columnarTable columnName ASC 100
        String columnDB = args[0];
        String columnarFile = args[1];
        String columnName = args[2];
        String sortOrder = args[3];
        int bufferSize = Integer.parseInt(args[4]);

        String dbpath = InterfaceUtils.dbPath(columnDB);
        SystemDefs sysdef = new SystemDefs(dbpath, 0, 25, "Clock");
        int un = SystemDefs.JavabaseBM.getNumUnpinnedBuffers();
        runInterface(columnarFile, columnName, sortOrder, bufferSize);

        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();

        System.out.println("Reads: " + PCounter.rcounter);
        System.out.println("Writes: " + PCounter.wcounter);
    }

    private static void runInterface(String columnarFile, String columnName, String sortOrder, int bufferSize) throws Exception {

        Columnarfile cf = new Columnarfile(columnarFile);

        int numCols = cf.getnumColumns();
        FldSpec[] Sprojection = new FldSpec[numCols];
        RelSpec rel = new RelSpec(RelSpec.outer);

        for (int i = 0; i < numCols; i++) {
            Sprojection[i] = new FldSpec(rel, i + 1);
        }

        short[] targets = new short[numCols];

        for(int i = 0; i< numCols;i++){
            targets[i] = (short)i;
        }

        ColumnarFileScan cfs = new ColumnarFileScan(columnarFile, Sprojection, targets, null);

        TupleOrder tupleSortOrder = (sortOrder.equals("ASC")) ? new TupleOrder(TupleOrder.Ascending) : new TupleOrder(TupleOrder.Descending);
        int un = SystemDefs.JavabaseBM.getNumUnpinnedBuffers();
        int sortColNumber = cf.getAttributePosition(columnName);
        ColumnarSort sort = new ColumnarSort(cf.getAttributes(), (short) cf.getAttributes().length, cf.getStrSize(), cfs, sortColNumber+1, tupleSortOrder, cf.getAttrsizeforcolumn(sortColNumber), 12);


        int cnt = 1;
        while (true) {
            Tuple result = sort.get_next();
            if (result == null) {
                break;
            }
            cnt++;
            result.print(cf.getAttributes());
        }

        System.out.println();
        System.out.println(cnt + " tuples selected");
        System.out.println();
        System.out.println("Sort completed using "+sort.getPasses()+" passes.");
        // clean up
        sort.close();

    }
}
