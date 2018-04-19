package interfaces;
/*
import bitmap.BitMapFile;
import btree.BTreeFile;
import columnar.Columnarfile;
import diskmgr.PCounter;
import global.AttrOperator;
import global.AttrType;
import global.IndexType;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Tuple;
import index.ColumnarIndexScan;
import iterator.*;
import tests.TestDriver;

import java.io.IOException;
class ColumnarDriver2 extends TestDriver {

    String DBName;
    String Colfilename;
    String Projection;
    String expression;
    int bufspace;
    String Accesstype;

    //private boolean delete = true;
    public ColumnarDriver2(String dBName2, String colfilename2, String projection2, String expression2, int bufspace2, String accesstype2) {
        super(dBName2);
        DBName = dBName2;
        Colfilename = colfilename2;
        Projection = projection2;
        expression = expression2;
        bufspace = bufspace2;
        Accesstype = accesstype2;
    }

    public ColumnarDriver2() {


    }

    boolean isInteger(String s) {
        //assumption give only numbers
        try {
            int chumma = Integer.parseInt(s);

        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }

    public boolean runTests() {


        SystemDefs sysdef = new SystemDefs(dbpath, 0, bufspace, "Clock");

        // Kill anything that might be hanging around
        String newdbpath;
        String newlogpath;
        String remove_logcmd;
        String remove_dbcmd;
        String remove_cmd = isUnix() ? "/bin/rm -rf " : "cmd /c del /f ";
        boolean _pass = true;
        if (Accesstype.equals("FILESCAN")) {
            _pass = test1();
        } else if (Accesstype.equals("COLUMNSCAN")) {
            _pass = test2();
        } else if (Accesstype.equals("BTREE")) {
            _pass = test3(0);
        } else if (Accesstype.equals("BITMAP")) {
            _pass = test3(1);
        }
        try {
            SystemDefs.JavabaseBM.flushAllPages();
            SystemDefs.JavabaseDB.closeDB();
        } catch (Exception e) {
            System.err.println("error: " + e);
        }
        System.out.print("\n" + "..." + testName() + " tests ");
        System.out.print(_pass == OK ? "completely successfully" : "failed");
        System.out.print(".\n\n");
        System.out.println("Reads: " + PCounter.rcounter);
        System.out.println("Writes: " + PCounter.wcounter);
        return _pass;

    }

    protected boolean test3(int id){

    	//indexName
    	//BM.ColumnarFileName.0.value
    	//BT.ColumnarFileName.0

        Columnarfile cf;
        try{
            cf=new Columnarfile(Colfilename);
            String indName="";
            IndexType it;
            StringBuilder sb = new StringBuilder();
            String[] temp=Projection.split(",");
            String[] expression1=expression.split(" ");
            expression1[0]=expression1[0].replace("{","");
            expression1[2]=expression1[2].replace("}", "");
            int columnNo=cf.getAttributePosition(expression1[0]);
            AttrType indexAttrType = cf.getAttrtypeforcolumn(columnNo);
            short[] targetedCols = new short[temp.length];
            boolean indexOnly;
            if (temp.length == 1) {
                if (temp[0].equals(expression1[0]))
                    indexOnly = true;
                else
                    indexOnly = false;
            }
            else {
                indexOnly = false;
            }
            int index = 0;
            for (String i : temp) {
                targetedCols[index++] = (short) cf.getAttributePosition(i);
            }
            CondExpr[] expr;
            if(expression1.length<2){
                expr=new CondExpr[1];
                expr[0]=null;
            }
            else{
                expr = new CondExpr[2];
                expr[0] = new CondExpr();
                expr[0].next = null;
                expr[0].type1 = new AttrType(AttrType.attrSymbol);
                expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), cf.getAttributePosition(expression1[0]) + 1);
                expr[1] = null;


                if (expression1[1].equals("=")) {
                    expr[0].op = new AttrOperator(AttrOperator.aopEQ);
                } else if (expression1[1].equals(">")) {
                    expr[0].op = new AttrOperator(AttrOperator.aopGT);
                } else if (expression1[1].equals("<")) {
                    expr[0].op = new AttrOperator(AttrOperator.aopLT);
                } else if (expression1[1].equals("!=")) {
                    expr[0].op = new AttrOperator(AttrOperator.aopNE);
                } else if (expression1[1].equals("<=")) {
                    expr[0].op = new AttrOperator(AttrOperator.aopLE);
                } else if (expression1[1].equals(">=")) {
                    expr[0].op = new AttrOperator(AttrOperator.aopGE);
                }
                if (isInteger(expression1[2])) {
                    expr[0].type2 = new AttrType(AttrType.attrInteger);
                    expr[0].operand2.integer = Integer.parseInt(expression1[2]);
                } else {
                    expr[0].type2 = new AttrType(AttrType.attrString);
                    expr[0].operand2.string = expression1[2];
                }
            }

            if(id==1){
                //Bitmap
                //build index name
                sb.append("BM.");
                sb.append(Colfilename);
                sb.append(".");
                sb.append(String.valueOf(columnNo));
                sb.append(".");
                sb.append(expression1[2]);
                indName = sb.toString();
                it=new IndexType(3);
                BitMapFile bf = cf.getBMIndex(indName);
                if(bf==null){
                    throw new Exception("Bitmap file does not exists");
                }
            }
            else{
                //Btree
                sb.append("BT.");
                sb.append(Colfilename);
                sb.append(".");
                sb.append(String.valueOf(columnNo));
                indName = sb.toString();
                it=new IndexType(1);
                BTreeFile bf = cf.getBTIndex(indName);
                if(bf==null){
                    throw new Exception("Btree file does not exists");
                }
            }
            ColumnarIndexScan cis = new ColumnarIndexScan(it, Colfilename, indName, indexAttrType, cf.getAttrsizeforcolumn(columnNo), expr, indexOnly, targetedCols);
            boolean done = false;
            AttrType[] atype2 = new AttrType[temp.length];
            for (int i = 0; i < temp.length; i++) {
                atype2[i] = cf.getAttrtypeforcolumn(targetedCols[i]);
            }
            while (!done) {
                Tuple result = cis.get_next();
                if (result == null) {
                    done = true;
                    break;
                } else {
                    result.print(atype2);
                }
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
        return true;
    }
    protected boolean test4() {
        return true;
    }

    protected boolean test1()  {
        try {
            //System.out.println("here");
            Columnarfile cf=new Columnarfile(Colfilename);
            short len_in1=cf.getnumColumns();
            AttrType[] in1=cf.getAttributes();
            short[] s1_sizes=cf.getAttrSizes();
            String[] temp=Projection.split(",");
            int n_out_flds=temp.length;
            AttrType[] opattr=new AttrType[temp.length];
            String[] expression1=expression.split(" ");
            CondExpr[] expr;
            if(expression1.length<=2){
                expr=new CondExpr[1];
                expr[0]=null;
            }
            else{
                expression1[0]=expression1[0].replace("{","");
                expression1[2]=expression1[2].replace("}", "");
                expr=new CondExpr[2];
                expr[0]=new CondExpr();
                expr[0].next = null;
                expr[0].type1 = new AttrType(AttrType.attrSymbol);
                expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), cf.getAttributePosition(expression1[0])+1);
                expr[1]=null;
                //System.out.println("here");

                if(expression1[1].equals("=")){
                    expr[0].op = new AttrOperator(AttrOperator.aopEQ);
                }
                else if(expression1[1].equals(">")){
                    expr[0].op = new AttrOperator(AttrOperator.aopGT);
                }
                else if(expression1[1].equals("<")){
                    expr[0].op = new AttrOperator(AttrOperator.aopLT);
                }
                else if(expression1[1].equals("!=")){
                    expr[0].op = new AttrOperator(AttrOperator.aopNE);
                }
                else if(expression1[1].equals("<=")){
                    expr[0].op = new AttrOperator(AttrOperator.aopLE);
                }
                else if(expression1[1].equals(">=")){
                    expr[0].op = new AttrOperator(AttrOperator.aopGE);
                }
                if (isInteger(expression1[2])) {

                    expr[0].type2 = new AttrType(AttrType.attrInteger);
                    expr[0].operand2.integer = Integer.parseInt(expression1[2]);
                }
                else{
                    expr[0].type2 = new AttrType(AttrType.attrString);
                    expr[0].operand2.string = expression1[2];
                }
            }
            //System.out.println("here");
            FldSpec[] projectionlist=new FldSpec[n_out_flds];
            for(int i=0;i<n_out_flds;i++){
                projectionlist[i]=new FldSpec(new RelSpec(RelSpec.outer),cf.getAttributePosition (temp[i])+1);
                try {
                    opattr[i] = new AttrType(cf.getAttrtypeforcolumn(cf.getAttributePosition(temp[i])).attrType);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            try {
                ColumnarFileScan fc=new ColumnarFileScan(Colfilename, in1,s1_sizes,len_in1,n_out_flds,projectionlist,expr);
                boolean done=false;
                while(!done){
                    try {
                        Tuple result=fc.get_next();
                        if(result!=null){
                            result.print(opattr);
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
                    fc.close();


            } catch (SortException l) {
                l.printStackTrace();
            }
            catch (FileScanException e) {
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
        return true;
    }

    protected boolean test2() {
        try {
            //assumption no nested conditions
            Columnarfile cf=new Columnarfile(Colfilename);
            String[] expression1=expression.split(" ");
            CondExpr[] expr;
            int columnNo=cf.getAttributePosition((expression1[0]).replace("{", ""));
            try {
                AttrType attrtype = cf.getAttrtypeforcolumn(columnNo);
                String[] temp=Projection.split(",");
                short[] targetedCols=new short[temp.length];
                int index=0;
                for(String i:temp){
                    targetedCols[index++]=(short) cf.getAttributePosition(i);
                }
                expression1[0]=expression1[0].replace("{","");
                expression1[2]=expression1[2].replace("}", "");

                expr=new CondExpr[2];
                expr[0]=new CondExpr();
                expr[0].next = null;
                //assuming it is always variable to left and it is a character
                expr[0].type1 = new AttrType(AttrType.attrSymbol);
                expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                expr[1] = null;
                if (expression1[1].equals("=")) {
                    expr[0].op = new AttrOperator(AttrOperator.aopEQ);
                }
                else if(expression1[1].equals(">")){
                    expr[0].op = new AttrOperator(AttrOperator.aopGT);
                }
                else if(expression1[1].equals("<")){
                    expr[0].op = new AttrOperator(AttrOperator.aopLT);
                }
                else if(expression1[1].equals("!=")){
                    expr[0].op = new AttrOperator(AttrOperator.aopNE);
                }
                else if(expression1[1].equals("<=")){
                    expr[0].op = new AttrOperator(AttrOperator.aopLE);
                }
                else if(expression1[1].equals(">=")){
                    expr[0].op = new AttrOperator(AttrOperator.aopGE);
                }
                if (isInteger(expression1[2])) {
                    expr[0].type2 = new AttrType(AttrType.attrInteger);
                    expr[0].operand2.integer = Integer.parseInt(expression1[2]);
                }
                else{
                    expr[0].type2 = new AttrType(AttrType.attrString);
                    expr[0].operand2.string = expression1[2];
                }

                AttrType[] atype2 = new AttrType[temp.length];
                for (int i = 0; i < temp.length; i++) {
                    atype2[i] = cf.getAttrtypeforcolumn(targetedCols[i]);
                }

                ColumnarColumnScan ccs = new ColumnarColumnScan(Colfilename, columnNo, attrtype, cf.getAttrsizeforcolumn(columnNo), targetedCols, expr);
                boolean done = false;
                while (!done) {
                    //RID rid = new RID();
                    Tuple result = ccs.get_next();
                    if (result == null) {
                        done = true;
                        break;
                    }
                    result.print(atype2);
                }
            } catch (Exception e) {
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
        return true;
    }

    protected String testName() {

        return "Select Query";
    }
}
*/

import bufmgr.*;
import columnar.Columnarfile;
import diskmgr.PCounter;
import global.AttrType;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Tuple;
import index.ColumnarIndexScan;
import iterator.*;

import java.io.IOException;

public class SelectQuery{

    private static String FILESCAN = "FILE";
    private static String COLUMNSCAN = "COLUMN";
    private static String BITMAPSCAN = "BITMAP";
    private static String BTREESCAN = "BTREE";

    public static void main(String args[]) throws Exception {
        // Query Skeleton: COLUMNDB COLUMNFILE PROJECTION OTHERCONST SCANCOLS [SCANTYPE] [SCANCONST] TARGETCOLUMNS NUMBUF
        // Example Query: testColumnDB columnarTable A,B,C "C = 5" A,B [BTREE,BITMAP] "(A = 5 v A = 6),(B > 7)" A,B,C 100
        // In case no constraints need to be applied, pass "" as input.
        String columnDB = args[0];
        String columnarFile = args[1];
        String[] projection = args[2].split(",");
        String otherConstraints = args[3];
        String[] scanColumns = args[4].split(",");
        String[] scanTypes = args[5].split(",");
        String[] scanConstraints = args[6].split(",");
        String[] targetColumns = args[7].split(",");
        Integer bufferSize = Integer.parseInt(args[8]);

        String dbpath = InterfaceUtils.dbPath(columnDB);
        SystemDefs sysdef = new SystemDefs(dbpath, 0, bufferSize, "Clock");

        runInterface(columnarFile, projection, otherConstraints, scanColumns, scanTypes, scanConstraints, targetColumns);

        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();

        System.out.println("Reads: " + PCounter.rcounter);
        System.out.println("Writes: " + PCounter.wcounter);
    }

    private static void runInterface(String columnarFile, String[] projection, String otherConstraints, String[] scanColumns, String[] scanTypes, String[] scanConstraints, String[] targetColumns) throws Exception {

        Columnarfile cf = new Columnarfile(columnarFile);

        AttrType[] opAttr = new AttrType[projection.length];
        FldSpec[] projectionList = new FldSpec[projection.length];
        for (int i = 0; i < projection.length; i++) {
            String attribute = InterfaceUtils.getAttributeName(projection[i]);
            projectionList[i] = new FldSpec(new RelSpec(RelSpec.outer), cf.getAttributePosition(attribute) + 1);
            opAttr[i] = new AttrType(cf.getAttrtypeforcolumn(cf.getAttributePosition(attribute)).attrType);
        }

        int[] scanCols = new int[scanColumns.length];
        for (int i = 0; i < scanColumns.length; i++) {
            if(!scanColumns[i].equals("")) {
                String attribute = InterfaceUtils.getAttributeName(scanColumns[i]);
                scanCols[i] = cf.getAttributePosition(attribute);
            }
        }

        short[] targets = new short[targetColumns.length];
        for (int i = 0; i < targetColumns.length; i++) {
            String attribute = InterfaceUtils.getAttributeName(targetColumns[i]);
            targets[i] = (short)cf.getAttributePosition(attribute);
        }

        CondExpr[] otherConstraint = InterfaceUtils.processRawConditionExpression(otherConstraints, cf);

        CondExpr[][] scanConstraint = new CondExpr[scanTypes.length][1];

        for(int i = 0; i < scanTypes.length;i++){
            scanConstraint[i] = InterfaceUtils.processRawConditionExpression(scanConstraints[i]);
        }
        cf.close();
        Iterator it = null;

        if(scanTypes[0].equals(FILESCAN)){
            it = new ColumnarFileScan(columnarFile, projectionList, targets, otherConstraint);
        }
        else if(scanTypes[0].equals(COLUMNSCAN)){
            it = new ColumnarColumnScan(columnarFile, scanCols[0], projectionList, targets, scanConstraint[0], otherConstraint);
        }

        int cnt = 0;
        while (true) {
            Tuple result = it.get_next();
            if (result == null) {
                break;
            }
            cnt++;
            result.print(opAttr);
        }

        System.out.println();
        System.out.println(cnt +" tuples selected");
        System.out.println();
    }
}

