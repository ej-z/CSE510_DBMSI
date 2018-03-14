package tests;

import global.SystemDefs;

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
        String remove_cmd = isUnix() ? "/bin/rm -rf " : "cmd /c del /f ";

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

//    protected boolean test1() {
//
//        try {
//            String name = "file1";
//            int numColumns = 3;
//            AttrType[] types = new AttrType[numColumns];
//            types[0] = new AttrType(AttrType.attrInteger);
//            types[1] = new AttrType(AttrType.attrInteger);
//            types[2] = new AttrType(AttrType.attrInteger);
//
//            System.out.println("Creating columnar " + name);
//            new Columnarfile(name, numColumns, types);
//
//            System.out.println("Opening columnar " + name);
//            Columnarfile c = new Columnarfile(name);
//            int n = c.getNumColumns();
//            System.out.println("Column count :" + n);
//        } catch (Exception e) {
//            e.printStackTrace();
//            return false;
//        }
//
//        return true;
//    }

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

        return "Columnar file";
    }
}

public class ColumnarTest {
    public static void runTests() {

        ColumnarDriver cd = new ColumnarDriver();

        cd.runTests();
    }
}
