package tests;

import columnar.Columnarfile;
import columnar.TID;
import columnar.TupleScan;
import com.sun.org.apache.xpath.internal.SourceTree;
import global.AttrType;
import global.SystemDefs;
import heap.Tuple;

import javax.sound.midi.Soundbank;
import java.io.IOException;

import static global.GlobalConst.NUMBUF;

class ColumnarDriver extends TestDriver {

    public ColumnarDriver() {
        super("cmtest");
    }

    public boolean runTests() {

        System.out.println("\n" + "Running " + testName() + " tests...." + "\n");

        SystemDefs sysdef = new SystemDefs(dbpath, 1000, NUMBUF, "Clock");

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

        // Commands here is very machine dependent.  We assume
        // user are on UNIX system here
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (IOException e) {
            System.err.println("IO error: " + e);
        }

        remove_logcmd = remove_cmd + newlogpath;
        remove_dbcmd = remove_cmd + newdbpath;

        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (IOException e) {
            System.err.println("IO error: " + e);
        }

        //Run the tests. Return type different from C++
        boolean _pass = runAllTests();


        //Clean up again
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (IOException e) {
            System.err.println("IO error: " + e);
        }

        System.out.print("\n" + "..." + testName() + " tests ");
        System.out.print(_pass == OK ? "completely successfully" : "failed");
        System.out.print(".\n\n");

        return _pass;

    }

    protected boolean test1(){
        try {
            String name = "file1";
            int numColumns = 3;
            AttrType[] types = new AttrType[numColumns];
            types[0] = new AttrType(AttrType.attrInteger);
            types[1] = new AttrType(AttrType.attrReal);
            types[2] = new AttrType(AttrType.attrString);
            short[] sizes = new short[1];
            sizes[0] = 20;
            System.out.println("Creating columnar " + name);
            Columnarfile cf = new Columnarfile(name, numColumns, types, sizes);

            System.out.println("Inserting columns..");
            for(int i = 0; i < 20; i++){

                Tuple t = new Tuple();
                t.setHdr((short)3, types, sizes);
                int s = t.size();
                t = new Tuple(s);
                t.setHdr((short)3, types, sizes);
                t.setIntFld(1,i);
                t.setFloFld(2, (float)(i*1.1));
                t.setStrFld(3, "A"+i);
                cf.insertTuple(t.getTupleByteArray());
                System.out.println(i+","+(i*1.1)+",A"+i);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    protected boolean test2() {

        String name = "file1";
        System.out.println("Opening columnar " + name);

        try {
            Columnarfile cf = new Columnarfile(name);
            System.out.println("File contains " + cf.getTupleCnt()+" tuples.");
            TupleScan scan = cf.openTupleScan();

            TID tid = new TID();
            Tuple t = scan.getNext(tid);
            while (t != null){
                System.out.println(t.getIntFld(1)+","+t.getFloFld(2)+","+t.getStrFld(3));
                t = scan.getNext(tid);
            }

        } catch (Exception e){
            e.printStackTrace();
            return false;
        }
        return true;
    }

    protected boolean test3() {
        return true;
    }

    protected boolean test4() {
        return true;
    }

    protected boolean test5() {
        return true;
    }

    protected boolean test6() {
        return true;
    }

    protected String testName() {

        return "Columnar file";
    }
}

public class ColumnarTest {
    public static void runTests() {

        ColumnarDriver cd = new ColumnarDriver();

        cd.runTests();
    }
}
