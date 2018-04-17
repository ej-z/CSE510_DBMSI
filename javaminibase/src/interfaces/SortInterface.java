package interfaces;

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
import tests.TestDriver;

import java.io.File;
import java.io.IOException;

import static global.GlobalConst.NUMBUF;

class SortTestDriver extends TestDriver {

    private  int numPages = 1000;

    private String colFilename;
    private String sortingOrder;
    private int sortColNumber;
    private int bufferSize;
    AttrType[] types;
    short[] sizes;
    public static FldSpec[] Sprojection;
    TupleOrder[] order;

    //private boolean delete = true;
    public SortTestDriver() {
        super("BatchInsert");
    }

    public SortTestDriver(String columnDBName, String columnFileName, int sortColNum, String sortOrder, int buffSize) {
        super(columnDBName);
        colFilename = columnFileName;
        this.sortingOrder = sortOrder;
        sortColNumber = sortColNum;
        bufferSize = buffSize;

        order = new TupleOrder[2];
        order[0] = new TupleOrder(TupleOrder.Ascending);
        order[1] = new TupleOrder(TupleOrder.Descending);

    }


    public boolean runTests() {

        System.out.println("\n" + "Running " + testName() + " tests...." + dbpath+"\n");

        numPages = new File(dbpath).exists()? 0 : 10000;
        SystemDefs sysdef = new SystemDefs(dbpath, 0, NUMBUF, "Clock");

        // Kill anything that might be hanging around
        String newdbpath;
        String newlogpath;
        String remove_logcmd;
        String remove_dbcmd;
        String remove_cmd = isUnix()? "/bin/rm -rf " : "cmd /c del /f ";

        newdbpath = dbpath;
        newlogpath = logpath;

        remove_logcmd = remove_cmd + logpath;
        remove_dbcmd = remove_cmd + dbpath;

        boolean _pass = test1();
        try {
            SystemDefs.JavabaseBM.flushAllPages();
            SystemDefs.JavabaseDB.closeDB();
        }catch (Exception e) {
            System.out.println("coming from here");
            System.err.println("error: " + e);
        }


        System.out.println("Reads: "+PCounter.rcounter);
        System.out.println("Writes: "+PCounter.wcounter);
        System.out.print("\n" + "..." + testName() + " tests ");
        System.out.print(_pass == OK ? "completely successfully" : "failed");
        System.out.print(".\n\n");

        return _pass;

    }

    protected boolean test1(){

        try {
            Columnarfile cf = new Columnarfile(colFilename);

            int numAttributes = cf.getAttributes().length;
            Sprojection = new FldSpec[numAttributes];
            RelSpec rel = new RelSpec(RelSpec.outer);

            for(int i=0;i<numAttributes;i++){
                Sprojection[i] = new FldSpec(rel, i+1);
            }
            ColumnarFileScan cfs = new ColumnarFileScan(colFilename, cf.getAttributes(), cf.getStrSize(), (short)cf.getAttributes().length, (short)cf.getAttributes().length, Sprojection ,null);

            TupleOrder sortOrder = (sortingOrder == "ASC")?order[1]:order[0];

            ColumnarSort sort = new ColumnarSort(cf.getAttributes(), (short)cf.getAttributes().length, cf.getStrSize(), cfs, sortColNumber, sortOrder, cf.getAttrsizeforcolumn(sortColNumber), bufferSize);


            Tuple t =new Tuple();
            int count = 0;
            t = null;
            String[] outval = null;


            try {
                t = sort.get_next();
            } catch (Exception e) {
                e.printStackTrace();
            }

            boolean flag = true;


            while (t != null) {
                try {
                    outval = new String[numAttributes];
                    for (int i=0;i<numAttributes;i++){
                        if(cf.getAttrtypeforcolumn(i).toString() == "attrString"){
                            outval[i] = t.getStrFld(i+1);

                        }
                        else{
                            outval[i] = String.valueOf(t.getIntFld(i+1));
                        }
                        System.out.print(outval[i]+' ');
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


        } catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return true;
    }

    protected String testName() {

        return "Sort ";
    }
}

public class SortInterface {

    public static String columnDBName;
    public static String columnarFileName;
    public static String sortOrder;
    public static int sortColumn;
    public static int buffSize;

    public static void runTests() {

        SortTestDriver st = new SortTestDriver(columnDBName, columnarFileName, sortColumn, sortOrder, buffSize);
        st.runTests();
    }

    public static void main(String[] argvs) {

        try {
            SortInterface sortTest = new SortInterface();

            columnDBName = argvs[0];
            columnarFileName = argvs[1];
            sortColumn = Integer.parseInt(argvs[2]);
            sortOrder = argvs[3];
            buffSize = Integer.parseInt(argvs[4]);

            SortInterface.runTests();

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Error encountered during buffer manager tests:\n");
            Runtime.getRuntime().exit(1);
        }
    }
}
