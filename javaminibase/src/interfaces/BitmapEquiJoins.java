package interfaces;

import columnar.ColumnarBitmapEquiJoins;
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
        // Example Query: testColumnDB columnarTable1 columnarTable2 "([columnarTable1.A = 10] v [columnnarTable1.B > 2]) ^ ([columnnarTable1.C = 20])" "([columnarTable2.A = 10] v [columnarTable2.B > 2]) ^ ([columnarTable2.C = 20])" "([columnarTable1.A = columnarTable2.A] v [columnarTable1.B = columnarTable2.B]) ^ ([columnarTable1.C = columnarTable2.C])" columnarTable1.A,columnarTable1.B,columnarTable2.C,columnarTable2.D 100
        // In case no constraints need to be applied, pass "" as input.
        String columnDB = args[0];
        String outerColumnarFile = args[1];
        String innerColumnarFile = args[2];
        String rawOuterConstraint = args[3];
        String rawInnerConstraint = args[4];
        String rawEquijoinConstraint = args[5];
        String[] targetColumns = args[6].split(",");
        Integer bufferSize = Integer.parseInt(args[7]);

        String path = isUnix() ? "/tmp/" : "C:\\Windows\\Temp\\";
        // TODO: Remove nME
        String dbpath = path + columnDB + "dixith.minibase-db";
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

        AttrType[] opAttr = new AttrType[targetColumns.length];
        //todO Uncomment this
//        FldSpec[] projectionList = new FldSpec[targetColumns.length];
//        for (int i = 0; i < targetColumns.length; i++) {
//            String attribute = targetColumns[i].split("\\.")[1];
//            if (targetColumns[i].equals(outerColumnarFile)) {
//                projectionList[i] = new FldSpec(new RelSpec(RelSpec.outer), outer.getAttributePosition(attribute) + 1);
//                opAttr[i] = new AttrType(outer.getAttrtypeforcolumn(outer.getAttributePosition(attribute)).attrType);
//            } else {
//                projectionList[i] = new FldSpec(new RelSpec(RelSpec.innerRel), inner.getAttributePosition(attribute) + 1);
//                opAttr[i] = new AttrType(inner.getAttrtypeforcolumn(inner.getAttributePosition(attribute)).attrType);
//            }
//        }

        // Call the equijoin interface
        /*
        *
        * AttrType[] in1,
            int len_in1,
            short[] t1_str_sizes,
            AttrType[] in2,
            int len_in2,
            short[] t2_str_sizes,
            int amt_of_mem,
            String leftColumnarFileName,
            int leftJoinField,
            String rightColumnarFileName,
            int rightJoinField,
            FldSpec[] proj_list,
            int n_out_flds,
            CondExpr[] joinExp,
            CondExpr[] innerExp,
            CondExpr[] outerExp
        * */

        ColumnarBitmapEquiJoins columnarBitmapEquiJoins = new ColumnarBitmapEquiJoins(null, -1, null,
                null, -1, null, -1, outerColumnarFile, -1,
                innerColumnarFile, -1, null, -1, equiJoinConstraint,
                innerColumnarConstraint, outerColumnarConstraint);

        outer.close();
        inner.close();
    }

    private static CondExpr[] processEquiJoinConditionExpression(String expression, Columnarfile innerColumnarFile, Columnarfile outerColumnarFile) {
        CondExpr[] condExprs;

        if (expression.length() == 0) {
            condExprs = new CondExpr[1];
            condExprs[0] = null;

            return condExprs;
        }

        String[] andExpressions = expression.split(" ^ ");
        condExprs = new CondExpr[andExpressions.length + 1];
        for (int i = 0; i < andExpressions.length; i++) {
            String temp = andExpressions[i].replace("(", "");
            temp = temp.replace(")", "");
            String[] orExpressions = temp.split(" v ");

            condExprs[i] = new CondExpr();
            CondExpr conditionalExpression = condExprs[i];
            for (int j = 0; j < orExpressions.length; j++) {
                String singleExpression = orExpressions[j].replace("[", "");
                singleExpression = singleExpression.replace("]", "");
                String[] expressionParts = singleExpression.split(" ");
                String attribute1Name = expressionParts[0].split("\\.")[1];
                String stringOperator = expressionParts[1];
                String attribute2Name = expressionParts[2].split("\\.")[1];

                conditionalExpression.type1 = new AttrType(AttrType.attrSymbol);
                conditionalExpression.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), outerColumnarFile.getAttributePosition(attribute1Name) + 1);
                conditionalExpression.op = getOperatorForString(stringOperator);
                conditionalExpression.type2 = new AttrType(AttrType.attrSymbol);
                conditionalExpression.operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), innerColumnarFile.getAttributePosition(attribute2Name) + 1);


                if (j == orExpressions.length - 1) {
                    conditionalExpression.next = null;
                } else {
                    conditionalExpression.next = new CondExpr();
                    conditionalExpression = conditionalExpression.next;
                }
            }
        }
        condExprs[andExpressions.length] = null;

        return condExprs;
    }

    private static CondExpr[] processRawConditionExpression(String expression, Columnarfile cf) {
        CondExpr[] condExprs;

        if (expression.length() == 0) {
            condExprs = new CondExpr[1];
            condExprs[0] = null;

            return condExprs;
        }

        String[] andExpressions = expression.split(" ^ ");
        condExprs = new CondExpr[andExpressions.length + 1];
        for (int i = 0; i < andExpressions.length; i++) {
            String temp = andExpressions[i].replace("(", "");
            temp = temp.replace(")", "");
            String[] orExpressions = temp.split(" v ");

            condExprs[i] = new CondExpr();
            CondExpr conditionalExpression = condExprs[i];
            for (int j = 0; j < orExpressions.length; j++) {
                String singleExpression = orExpressions[j].replace("[", "");
                singleExpression = singleExpression.replace("]", "");
                String[] expressionParts = singleExpression.split(" ");
                String attributeName = expressionParts[0].split("\\.")[1];
                String stringOperator = expressionParts[1];
                String attributeValue = expressionParts[2];

                conditionalExpression.type1 = new AttrType(AttrType.attrSymbol);
                conditionalExpression.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), cf.getAttributePosition(attributeName) + 1);
                conditionalExpression.op = getOperatorForString(stringOperator);
                if (isInteger(attributeValue)) {
                    conditionalExpression.type2 = new AttrType(AttrType.attrInteger);
                    conditionalExpression.operand2.integer = Integer.parseInt(attributeValue);
                } else {
                    conditionalExpression.type2 = new AttrType(AttrType.attrString);
                    conditionalExpression.operand2.string = attributeValue;
                }

                if (j == orExpressions.length - 1) {
                    conditionalExpression.next = null;
                } else {
                    conditionalExpression.next = new CondExpr();
                    conditionalExpression = conditionalExpression.next;
                }
            }
        }
        condExprs[andExpressions.length] = null;

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
