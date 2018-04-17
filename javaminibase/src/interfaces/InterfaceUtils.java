package interfaces;

import columnar.Columnarfile;
import global.AttrOperator;
import global.AttrType;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.RelSpec;

public class InterfaceUtils {

    public static CondExpr[] processEquiJoinConditionExpression(String expression, Columnarfile innerColumnarFile, Columnarfile outerColumnarFile) {
        CondExpr[] condExprs;

        if (expression.length() == 0) {
            condExprs = new CondExpr[1];
            condExprs[0] = null;

            return condExprs;
        }

        String[] andExpressions = expression.split(" \\^ ");
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

    public static CondExpr[] processRawConditionExpression(String expression, Columnarfile cf) {
        // Sample input
        // String expression = "([columnarTable1.A = 'RandomTextHere'] v [columnarTable1.B > 2]) ^ ([columnarTable1.C = columnarTable1.D])"
        CondExpr[] condExprs;

        if (expression.length() == 0) {
            condExprs = new CondExpr[1];
            condExprs[0] = null;

            return condExprs;
        }

        String[] andExpressions = expression.split(" \\^ ");
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
                } else if (isString(attributeValue)) {
                    conditionalExpression.type2 = new AttrType(AttrType.attrString);
                    conditionalExpression.operand2.string = attributeValue;
                } else {
                    conditionalExpression.type2 = new AttrType(AttrType.attrSymbol);
                    String name = getAttributeName(attributeValue);
                    conditionalExpression.operand2.symbol = new FldSpec(new RelSpec(RelSpec.outer), cf.getAttributePosition(name) + 1);
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

    private static Boolean isString(String value) {
        if (value.charAt(0) == '\'' && value.charAt(value.length() - 1) == '\'') {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    private static String getAttributeName(String value) {
        return value.split("\\.")[1];
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

    public static boolean isInteger(String s) {
        try {
            Integer intValue = Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }

}