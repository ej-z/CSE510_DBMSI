package tests;

import static global.GlobalConst.NUMBUF;
import global.AttrOperator;
import global.AttrType;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Tuple;
import iterator.ColumnarFileScan;
import iterator.CondExpr;
import iterator.FileScanException;
import iterator.FldSpec;
import iterator.InvalidRelation;
import iterator.RelSpec;
import iterator.TupleUtilsException;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import columnar.Columnarfile;
import columnar.TID;
import columnar.TupleScan;
import diskmgr.PCounter;
class ColumnarDriver2 extends TestDriver {

	String DBName;
	String Colfilename;
	String Projection;
	String expression;
	int bufspace;
	String Accesstype;
    
    //private boolean delete = true;
    public ColumnarDriver2(String dBName2, String colfilename2, String projection2, String expression2, int bufspace2, String accesstype2) {
    	DBName = dBName2;
    	Colfilename = colfilename2;
    	Projection = projection2;
    	expression = expression2;
    	bufspace = bufspace2;
    	Accesstype= accesstype2;
    }
    public ColumnarDriver2(){
    	
    		
    }
        
    public boolean runTests() {

    	SystemDefs sysdef = new SystemDefs(DBName, 0, bufspace, "Clock");

        // Kill anything that might be hanging around
        String newdbpath;
        String newlogpath;
        String remove_logcmd;
        String remove_dbcmd;
        String remove_cmd = isUnix()? "/bin/rm -rf " : "cmd /c del /f ";

    	
        boolean _pass = test1();
        try {
            SystemDefs.JavabaseBM.flushAllPages();
            SystemDefs.JavabaseDB.closeDB();
        }catch (Exception e) {
            System.err.println("error: " + e);
        }
                
        System.out.print("\n" + "..." + testName() + " tests ");
        System.out.print(_pass == OK ? "completely successfully" : "failed");
        System.out.print(".\n\n");
        System.out.println("Reads: "+PCounter.rcounter);
        System.out.println("Writes: "+PCounter.wcounter);
        return _pass;

    }

    protected boolean test1(){
    	try {
    		System.out.println("here");
			Columnarfile cf=new Columnarfile(Colfilename);
			short len_in1=cf.getnumColumns();
			AttrType[] in1=cf.getAttributes();
			short[] s1_sizes=cf.getStrSize();
			String[] temp=Projection.split(",");
			String[] expression1=expression.split(" ");
			int n_out_flds=temp.length;
			CondExpr[] expr=new CondExpr[2];
			System.out.println("here");
			if(temp[1].equals("=")){
				expr[0]=new CondExpr();
				expr[0].op = new AttrOperator(AttrOperator.aopEQ);
		        expr[0].next = null;
		        //assuming it is always variable to left and it is a character
		        expr[0].type1 = new AttrType(AttrType.attrSymbol);
		        expr[0].type2 = new AttrType(AttrType.attrInteger);
		        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), cf.getAttributePosition(expression1[0])+1);
		        expr[0].operand2.integer = Integer.parseInt(temp[2]);
		        expr[1]=null;
			}
			System.out.println("here");
			FldSpec[] projectionlist=new FldSpec[n_out_flds];
			for(int i=0;i<n_out_flds;i++){
				projectionlist[i]=new FldSpec(new RelSpec(RelSpec.outer),cf.getAttributePosition (temp[i])+1);
			}
			try {
				ColumnarFileScan fc=new ColumnarFileScan(Colfilename, in1,s1_sizes,len_in1,n_out_flds,projectionlist,expr);
				boolean done=false;
				while(!done){
					try {
						Tuple result=fc.get_next();
						if(result!=null){
							result.print(in1);
						}
						else{
							done=true;
							break;
						}
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			} catch (FileScanException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TupleUtilsException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidRelation e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		} catch (HFException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (HFBufMgrException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (HFDiskMgrException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;	
    }
    protected String testName() {

        return "Columnar Tests";
    }
}
public class Select_query extends TestDriver {
	String DBName;
	String Colfilename;
	String Projection;
	String expression;
	 int bufspace;
	String Accesstype;
	
	public boolean runTests() {		
        ColumnarDriver2 cd = new ColumnarDriver2(DBName, Colfilename, Projection, expression, bufspace, Accesstype);
        return cd.runTests();
    }
	Select_query(String a, String b, String c, String d, int inputsplit, String access){
		DBName=a;
		Colfilename = b;
		Projection = c;
		expression =d;
		bufspace = inputsplit;
		Accesstype = access;
		
	}
	
public static void main(String args[]){
	String sampleinput = "SELECT columndb columnfile A,B,C {A = South_Dakota} 100 FileScan";
	String[] inputsplit=sampleinput.split(" ");
	for(int i=0;i<inputsplit.length;i++){
		System.out.println(inputsplit[i]);
	}
	String temp=inputsplit[4].replace("{", "")+" "+inputsplit[5]+" "+inputsplit[6].replace("}","");
	Select_query sq=new Select_query(inputsplit[1], inputsplit[2], inputsplit[3], temp, Integer.parseInt(inputsplit[7]),inputsplit[8]);
	sq.runTests();
}

}

