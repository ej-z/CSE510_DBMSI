package tests;

import columnar.Columnarfile;
import columnar.TID;
import columnar.TupleScan;
import columnar.ValueClass;
import diskmgr.PCounter;
import global.AttrType;
import global.SystemDefs;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.sql.NClob;
import java.util.ArrayList;

import static global.GlobalConst.NUMBUF;

class ColumnarDriver extends TestDriver {

    private  int numPages = 100000;
    private String dataFile;
    private String dbName;
    private String colFilename;
    private int numCols;
    AttrType[] types;
	short[] sizes;
	ArrayList<String> tuples = new ArrayList<>();
	String[] names;
    
    //private boolean delete = true;
    public ColumnarDriver() {
        super("cmtest");
    }

    public ColumnarDriver(String datafileName, String columnDBName, String columnarFileName, int numColumns) {
    	dataFile = datafileName;
    	dbName = columnDBName;
    	colFilename = columnarFileName;
    	numCols = numColumns;
    	
    	types = new AttrType[numColumns];
		sizes = new short[numColumns];
		names  = new String[numColumns];
		
    }
    
    public boolean runTests() {

       // System.out.println("\n" + "Running " + testName() + " tests...." + "\n");

        SystemDefs sysdef = new SystemDefs(dbName, numPages, NUMBUF, "Clock");

        // Kill anything that might be hanging around
        String newdbpath;
        String newlogpath;
        String remove_logcmd;
        String remove_dbcmd;
        String remove_cmd = isUnix()? "/bin/rm -rf " : "cmd /c del /f ";

        newdbpath = dbpath;
        newlogpath = logpath;

        /*remove_logcmd = remove_cmd + logpath;
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
                
        System.out.print("\n" + "..." + testName() + " tests ");
        System.out.print(_pass == OK ? "completely successfully" : "failed");
        System.out.print(".\n\n");
        System.out.println("Reads: "+PCounter.rcounter);
        System.out.println("Writes: "+PCounter.wcounter);
        return _pass;

    }

    protected boolean test1(){
        if(numPages == 0)
            return true;

        FileInputStream fstream;
		try {
			fstream = new FileInputStream(dataFile);
			BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
			//Columnarfile cf = new Columnarfile(colFilename, numCols , types, sizes);

			String strLine;
			//Read First Line
			String attrType = br.readLine();
			String parts[] = attrType.split("\t");
			//System.out.println(parts[0]+" "+parts[1]+" "+parts[2]+" "+parts[3]);
			int i=0;
			for (String s:parts) {
				String temp[] = s.split(":");
				//System.out.println(temp[0]+" "+temp[1]);
				names[i] = temp[0];
				if (temp[1].contains("char")){
					
					types[i] = new AttrType(AttrType.attrString);
					sizes[i] = Short.parseShort(temp[1].substring(5, temp[1].length()-1));
					System.out.println(types[i]+" "+sizes[i]);
					i++;
				}
				else {
					
					types[i] = new AttrType(AttrType.attrInteger);
					sizes[i] = 4;
					i++;
					//System.out.println(types[i]+" "+sizes[i]);
				}
			}
			
			
			while ((strLine = br.readLine()) != null)   {
				  // Print the content on the console
				  tuples.add(strLine);
				  //System.out.println (strLine);
			}
				//Close the input stream
			br.close();
		      
			
			Columnarfile cf = new Columnarfile(colFilename, numCols , types, sizes, names);
			for (String s:tuples) {
				String values[] = s.split("\t");
				
				Tuple t = new Tuple();
		        t.setHdr((short)numCols, types, sizes);
		        int size = t.size();
		        
		        //System.out.println(size);
		        t = new Tuple(size);
		        t.setHdr((short)numCols, types, sizes);
		        int j =0;
		        for (String val:values) {
		            switch(types[j].attrType){
		            case 0:
		                t.setStrFld(j+1, val);
		                j++;
		                break;
		            case 1:
		                t.setIntFld(j+1, Integer.parseInt(val));
		                j++;
		                break;
		            default:
		                j++;
		                break;
		            }
		        }
		        cf.insertTuple(t.getTupleByteArray());
		        //System.out.println("Reads: "+PCounter.rcounter);
		        //System.out.println("Writes: "+PCounter.wcounter);
			}		
				
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} /*catch (InvalidTypeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidTupleSizeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/ catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//Read File Line By Line
	 
        return true;
    }

    protected String testName() {

        return "Columnar Tests";
    }
}

public class RangeInsert {
	
	public static String datafileName;
	public static String columnDBName;
	public static String columnarFileName;
	public static int numColumns;
	
    public static void runTests() {

        ColumnarDriver cd = new ColumnarDriver(datafileName, columnDBName, columnarFileName, numColumns);
        cd.runTests();
    }

    public static void main(String[] argvs) {

        try {
            RangeInsert colTest = new RangeInsert();
            
            datafileName = argvs[0];
            columnDBName = argvs[1];
            columnarFileName = argvs[2];
            numColumns = Integer.parseInt(argvs[3]);
            
            //PCounter.initialize();
            
            //System.out.println(datafileName+" "+columnDBName+" "+columnarFileName+" "+numColumns);
            
            colTest.runTests();
            
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Error encountered during buffer manager tests:\n");
            Runtime.getRuntime().exit(1);
        }
    }
}