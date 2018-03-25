package tests;

import columnar.Columnarfile;
import columnar.TID;
import columnar.TupleScan;
import columnar.ValueClass;
import columnar.ValueInt;
import columnar.ValueString;
import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import diskmgr.PCounter;
import global.AttrType;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.NClob;
import java.util.ArrayList;
import java.util.Arrays;

import static global.GlobalConst.NUMBUF;

class IndexTestDriver extends TestDriver {

    private  int numPages = 1000;
    private String dbName;
    private String colFilename;
    private String colName;
    private String indexType;
    private int numCols;
    AttrType[] types;
	short[] sizes;
	
    //private boolean delete = true;
    public IndexTestDriver() {
        super("IndexTest");
    }

    public IndexTestDriver(String columnName, String columnDBName, String columnarFileName, String indexTypeVal) {
    	colName = columnName;
    	dbName = columnDBName;
    	colFilename = columnarFileName;
    	indexType = indexTypeVal;
    }
    
    public boolean runTests() {

        System.out.println("\n" + "Running " + testName() + " tests...." + "\n");

      // SystemDefs sysdef = new SystemDefs(dbName, numPages, NUMBUF, "Clock");
        
		SystemDefs sysdef = new SystemDefs(dbName, 0, NUMBUF, "Clock");
		//SystemDefs.JavabaseDB.openDB(this.dbName);

        // Kill anything that might be hanging around
        /*String newdbpath;
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
        }*/

        //Run the tests. Return type different from C++
        boolean _pass = test1();


        
        
        //boolean _pass = runAllTests();
        try {
            SystemDefs.JavabaseBM.flushAllPages();
            SystemDefs.JavabaseDB.closeDB();
        }catch (Exception e) {
            System.err.println("error: " + e);
        }

        System.out.print("\n" + "..." + testName() + " tests ");
        System.out.print(_pass == OK ? "completely successfully" : "failed");
        System.out.print(".\n\n");

        return _pass;

    }

    protected boolean test1(){
        
    	try {
			Columnarfile cf = new Columnarfile(colFilename);
			int val = cf.getAttributePosition(colName);
			AttrType attval = cf.getAttributes()[val];
			ValueClass value;
			if (attval.toString() == "attrInteger") {
				value = new ValueInt<Integer>(10);
			}
			else {
				value =  new ValueString<String>("Demo");
			}
			if (indexType.equals("BitMap")) {
				cf.createBitMapIndex(val, value);
			}
			else {
				cf.createBTreeIndex(val);
			}
		} catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		System.out.println("Reads: "+PCounter.rcounter);
        System.out.println("Writes: "+PCounter.wcounter);
	 
        return true;
    }

    protected String testName() {

        return "Index Tests";
    }
}

public class IndexCreateTest {
	
	public static String columnDBName;
	public static String columnarFileName;
	public static String columnName;
	public static String indexType;
	
    public static void runTests() {

        IndexTestDriver cd = new IndexTestDriver(columnName, columnDBName, columnarFileName, indexType);
        cd.runTests();
    }

    public static void main(String[] argvs) {

        try {
            IndexCreateTest indexTest = new IndexCreateTest();
            
            columnName = argvs[2];
            columnDBName = argvs[0];
            columnarFileName = argvs[1];
            indexType = argvs[3];
            indexTest.runTests();
            
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Error encountered during buffer manager tests:\n");
            Runtime.getRuntime().exit(1);
        }
    }
}