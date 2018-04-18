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
        SystemDefs sysdef = new SystemDefs(dbpath, 0, NUMBUF, "Clock");

        runInterface(columnarFile, columnName, sortOrder, bufferSize);

        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();

        System.out.println("Reads: " + PCounter.rcounter);
        System.out.println("Writes: " + PCounter.wcounter);
    }

    private static void runInterface(String columnarFile, String columnName, String sortOrder, int bufferSize) throws Exception {

        Columnarfile cf = new Columnarfile(columnarFile);

        int numAttributes = cf.getAttributes().length;
        FldSpec[] Sprojection = new FldSpec[numAttributes];
        RelSpec rel = new RelSpec(RelSpec.outer);

        for (int i = 0; i < numAttributes; i++) {
            Sprojection[i] = new FldSpec(rel, i + 1);
        }
        ColumnarFileScan cfs = new ColumnarFileScan(columnarFile, cf.getAttributes(), cf.getStrSize(), (short) cf.getAttributes().length, (short) cf.getAttributes().length, Sprojection, null);

        TupleOrder tupleSortOrder = (sortOrder == "ASC") ? new TupleOrder(TupleOrder.Ascending) : new TupleOrder(TupleOrder.Descending);

        int sortColNumber = cf.getAttributePosition(columnName);

        ColumnarSort sort = new ColumnarSort(cf.getAttributes(), (short) cf.getAttributes().length, cf.getStrSize(), cfs, sortColNumber, tupleSortOrder, cf.getAttrsizeforcolumn(sortColNumber), bufferSize);


        Tuple t = null;
        String[] outval = null;


        try {
            t = sort.get_next();
        } catch (Exception e) {
            e.printStackTrace();
        }

        while (t != null) {
            try {
                outval = new String[numAttributes];
                for (int i = 0; i < numAttributes; i++) {
                    if (cf.getAttrtypeforcolumn(i).attrType == AttrType.attrString) {
                        outval[i] = t.getStrFld(i + 1);

                    } else {
                        outval[i] = String.valueOf(t.getIntFld(i + 1));
                    }
                    System.out.print(outval[i] + ' ');
                }
                System.out.print('\n');

            } catch (Exception e) {
                e.printStackTrace();
            }

            try {
                t = sort.get_next();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // clean up
        try {
            cfs.close();
            sort.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
