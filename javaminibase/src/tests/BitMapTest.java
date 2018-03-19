package tests;

import global.SystemDefs;

import static global.GlobalConst.NUMBUF;

class BitMapDriver extends TestDriver {

    private int numPages = 1000;

    //private boolean delete = true;
    public BitMapDriver() {
        super("bitmaptest");
    }

    public boolean runTests() {
        System.out.println("\n" + "Running " + testName() + " tests...." + "\n");
        SystemDefs sysdef = new SystemDefs(dbpath, numPages, NUMBUF, "Clock");

        boolean _pass = runAllTests();
        try {
            SystemDefs.JavabaseBM.flushAllPages();
            SystemDefs.JavabaseDB.closeDB();
        } catch (Exception e) {
            System.err.println("error: " + e);
        }

        System.out.print("\n" + "..." + testName() + " tests ");
        System.out.print(_pass == OK ? "completely successfully" : "failed");
        System.out.print(".\n\n");

        return _pass;
    }

    protected boolean test1() {
        return true;
    }

    protected boolean test2() {
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
        return "Bit Map file";
    }
}

public class BitMapTest {
    public static void runTests() {
        BitMapDriver bm = new BitMapDriver();
        bm.runTests();
    }

    public static void main(String[] argvs) {
        try {
            BitMapTest bitMapTest = new BitMapTest();
            bitMapTest.runTests();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Error encountered during buffer manager tests:\n");
            Runtime.getRuntime().exit(1);
        }
    }
}