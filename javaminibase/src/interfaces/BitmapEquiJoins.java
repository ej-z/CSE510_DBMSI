package interfaces;

import columnar.Columnarfile;
import diskmgr.PCounter;
import global.AttrOperator;
import global.AttrType;
import global.SystemDefs;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.RelSpec;

import static tests.TestDriver.isUnix;

public class BitmapEquiJoins {

    public static void main(String[] args) throws Exception {
        // Query Skeleton: COLUMNDB OUTERFILE INNERFILE OUTERCONST INNERCONST EQUICONST [TARGETCOLUMNS] NUMBUF
        // Example Query: testColumnDB columnarTable1 columnarTable2 {columnarTable1.X = 5} {columnarTable2.A > 15} {columnarTable1.Y = columnarTable2.B} columnarTable1.A,columnarTable1.B,columnarTable2.C,columnarTable2.D 100
        String columnDB = args[0];
        String outerColumnarFile = args[1];
        String innerColumnarFile = args[2];
        String rawOuterConstraint = args[3] + " " + args[4] + " " + args[5];
        String rawInnerConstraint = args[6] + " " + args[7] + " " + args[8];
        String rawEquijoinConstraint = args[9] + " " + args[10] + " " + args[11];
        String[] targetColumns = args[12].split(",");
        Integer bufferSize = Integer.parseInt(args[13]);

        String path = isUnix() ? "/tmp/" : "C:\\Windows\\Temp\\";
        String dbpath = path + columnDB + ".minibase-db";
        SystemDefs sysdef = new SystemDefs(dbpath, 0, bufferSize, "Clock");

        runInterface(outerColumnarFile, innerColumnarFile, rawOuterConstraint, rawInnerConstraint, rawEquijoinConstraint, targetColumns);

        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();

        System.out.println("Reads: " + PCounter.rcounter);
        System.out.println("Writes: " + PCounter.wcounter);
    }

    private static void runInterface(String outerColumnarFile, String innerColumnarFile, String rawOuterConstraint, String rawInnerConstraint, String rawEquijoinConstraint, String[] targetColumns) throws Exception {
        Columnarfile outer = new Columnarfile(outerColumnarFile);
        Columnarfile inner = new Columnarfile(innerColumnarFile);

        CondExpr[] innerColumnarConstraint = processRawConditionExpression(rawInnerConstraint, inner);
        CondExpr[] outerColumnarConstraint = processRawConditionExpression(rawOuterConstraint, outer);
        CondExpr[] equiJoinConstraint = processEquiJoinConditionExpression(rawEquijoinConstraint, inner, outer);

//        FldSpec[] projectionlist = new FldSpec[targetColumns.length];
//        for(int i=0; i < targetColumns.length; i++){
//            projectionlist[i]=new FldSpec(new RelSpec(RelSpec.outer),cf.getAttributePosition (temp[i])+1);
//            try {
//                opattr[i] = new AttrType(cf.getAttrtypeforcolumn(cf.getAttributePosition(temp[i])).attrType);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }

        outer.close();
        inner.close();
    }

    private static CondExpr[] processEquiJoinConditionExpression(String expression, Columnarfile innerColumnarFile, Columnarfile outerColumnarFile) {
        CondExpr[] condExpr = new CondExpr[2];
        String trimmedExpression = expression.replace("{", "");
        trimmedExpression = trimmedExpression.replace("}", "");

        if (trimmedExpression.length() == 0) {
            System.out.println("Invalid input format for equijoin constraint. Correct format = {columnarFile1.attribute1 = columnnarFile2.attribute2}");
            System.exit(2);
        }

        String[] temp = trimmedExpression.split(" ");
        String outerAttributeName = temp[0].split("\\.")[1];
        String innerAttributeName = temp[2].split("\\.")[1];

        condExpr[0].next = null;
        condExpr[0].op = new AttrOperator(AttrOperator.aopEQ);
        condExpr[0].type1 = new AttrType(AttrType.attrSymbol);
        condExpr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), outerColumnarFile.getAttributePosition(outerAttributeName) + 1);
        condExpr[0].type2 = new AttrType(AttrType.attrSymbol);
        condExpr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), innerColumnarFile.getAttributePosition(innerAttributeName) + 1);
        condExpr[1] = null;

        return condExpr;
    }

    private static CondExpr[] processRawConditionExpression(String expression, Columnarfile cf) {
        CondExpr[] condExprs;
        String trimmedExpression = expression.replace("{", "");
        trimmedExpression = trimmedExpression.replace("}", "");

        if (trimmedExpression.length() == 0) {
            condExprs = new CondExpr[1];
            condExprs[0] = null;

            return condExprs;
        }

        String[] temp = trimmedExpression.split(" ");
        String attributeName = temp[0].split("\\.")[1];
        String stringOperator = temp[1];
        String attributeValue = temp[2];

        condExprs = new CondExpr[2];
        condExprs[0] = new CondExpr();
        condExprs[0].next = null;
        condExprs[0].type1 = new AttrType(AttrType.attrSymbol);
        condExprs[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), cf.getAttributePosition(attributeName) + 1);
        condExprs[0].op = getOperatorForString(stringOperator);
        condExprs[1] = null;
        if (isInteger(attributeValue)) {
            condExprs[0].type2 = new AttrType(AttrType.attrInteger);
            condExprs[0].operand2.integer = Integer.parseInt(attributeValue);
        } else {
            condExprs[0].type2 = new AttrType(AttrType.attrString);
            condExprs[0].operand2.string = attributeValue;
        }

        return condExprs;
    }

    private static AttrOperator getOperatorForString(String operator) {
        switch (operator) {
            case "=":
                return new AttrOperator(AttrOperator.aopEQ);
            case ">":
                return new AttrOperator(AttrOperator.aopGT);
            case "<":
                return new AttrOperator(AttrOperator.aopLT);
            case "!=":
                return new AttrOperator(AttrOperator.aopNE);
            case "<=":
                return new AttrOperator(AttrOperator.aopLE);
            case ">=":
                return new AttrOperator(AttrOperator.aopGE);
        }

        return null;
    }

    private static boolean isInteger(String s) {
        try {
            Integer intValue = Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }

}
