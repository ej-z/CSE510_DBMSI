package columnar;

import bitmap.BitMapFile;
import btree.PinPageException;
import diskmgr.Page;
import global.AttrOperator;
import global.AttrType;
import global.PageId;
import global.SystemDefs;
import iterator.CondExpr;
import iterator.FldSpec;

import java.util.*;
import java.util.stream.Collectors;

public class ColumnarBitmapEquiJoins  {
    private final Columnarfile leftColumnarFile;
    private final Columnarfile rightColumnarFile;
    private int offset1;
    private int offset2;
    private List<AttrOperator> equiOperators = new ArrayList<>();
    // contains two lists with R1 and R2 offsets
    private List<List<Integer>> offsets = new ArrayList<>();
    // need to change to ValueClass

    public ColumnarBitmapEquiJoins(
            AttrType[] in1,
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
            CondExpr[] outerExp) throws Exception {



        assert innerExp.length == 1;
        assert outerExp.length == 1;

        leftColumnarFile = new Columnarfile(leftColumnarFileName);
        rightColumnarFile = new Columnarfile(rightColumnarFileName);


//        leftColumnarFile.createAllBitMapIndexForColumn(leftJoinField);
//        rightColumnarFile.createAllBitMapIndexForColumn(rightJoinField);
//
//       leftBitMaps = leftColumnarFile.getAllBitMaps();
//       rightBitMaps = rightColumnarFile.getAllBitMaps();

//       if(leftColumnarFile.getAttrtypeforcolumn(leftJoinField) != rightColumnarFile.getAttrtypeforcolumn(rightJoinField)) {
//           throw new Exception("Join cannot be done only on same field");
//       } else {
//
//           uniqueValues = new HashSet<>();
//       }

        offsets.add(new ArrayList<>());
        offsets.add(new ArrayList<>());
        List<HashSet<String>> uniSet = getUniqueSetFromJoin(joinExp, leftColumnarFile, rightColumnarFile);
        List<List<String>> combinations = getSubs(uniSet);



        for(List<String> combination: combinations) {
            for (int i = 0; i < combination.size(); i++) {
                String bmName = leftColumnarFile.getBMName(offsets.get(0).get(i) - 1, new ValueString<>(combination.get(i)));
                System.out.println(bmName);
            }
        }

        System.out.println("**********************");
        System.out.println("Right bitmaps");
        System.out.println("**********************");
        for(List<String> combination: combinations) {
            for (int i = 0; i < combination.size(); i++) {
                String bmName = rightColumnarFile.getBMName(offsets.get(1).get(i) - 1, new ValueString<>(combination.get(i)));
                System.out.println(bmName);
            }
        }

    }


    private List<HashSet<String>> getUniqueSetFromJoin(CondExpr[] joinEquation, Columnarfile leftColumnarFile,
                                               Columnarfile rightColumnarFile) throws Exception {

        List<HashSet<String>> uniquesList = new ArrayList<>();

        for(int i = 0; i < joinEquation.length; i++) {

            CondExpr currentCondition = joinEquation[i];
            while(currentCondition != null) {
                equiOperators.add(currentCondition.op);

                FldSpec symbol = currentCondition.operand1.symbol;
                offset1 = symbol.offset;

                offsets.get(0).add(offset1);

                // move this is to interface we assume that the colums already have indexes
                leftColumnarFile.createAllBitMapIndexForColumn(offset1 - 1);
                HashMap<String, BitMapFile> allBitMaps = leftColumnarFile.getAllBitMaps();

                FldSpec symbol2 = currentCondition.operand2.symbol;
                offset2 = symbol2.offset;
                offsets.get(1).add(offset2);

                rightColumnarFile.createAllBitMapIndexForColumn(offset2 -1);
                AttrType attrtypeforLeftcolumn =  leftColumnarFile.getAttrtypeforcolumn(offset1 -1);
                AttrType attrtypeforRightColumn = rightColumnarFile.getAttrtypeforcolumn(offset2 -1);


                HashSet<String> set1 = extractUniqueValues(offset1 - 1, allBitMaps);
                HashSet<Integer> setLeftInt = null, setRightInt = null;
//                if(attrtypeforLeftcolumn.attrType == AttrType.attrInteger) {
//                    setLeftInt = set1.stream().map(Integer::parseInt).collect(Collectors.toCollection(HashSet::new));
//                }

                //inner

                HashMap<String, BitMapFile> allRightRelationBitMaps = rightColumnarFile.getAllBitMaps();


                HashSet<String> set2 = extractUniqueValues(offset2 -1, allRightRelationBitMaps);
//                if(attrtypeforRightColumn.attrType == AttrType.attrInteger) {
//                     setRightInt = set2.stream().map(Integer::parseInt).collect(Collectors.toCollection(HashSet::new));
//                }

//                if(setLeftInt != null && setRightInt != null) {
//                    setLeftInt.retainAll(setRightInt);
//                    uniquesList.add(setLeftInt);
//                } else {
                    set1.retainAll(set2);
                    uniquesList.add(set1);
//                }

                currentCondition = currentCondition.next;
            }
        }

        return uniquesList;
    }

    public HashSet<String> extractUniqueValues(int offset, HashMap<String, BitMapFile> allBitMaps) {

        HashSet<String> collect = allBitMaps.
                keySet()
                .stream()
                .filter(e -> {
                    String[] split = e.split("\\.");
                    if (Integer.parseInt(split[2]) == offset) {
                        return true;
                    }
                    return false;
                })
                .map(e -> e.split("\\.")[3])
                .collect(Collectors.toCollection(HashSet::new));

        return collect;
    }


    public List<List<String>> getSubs(List<HashSet<String>> uniqueSets)  {
        List<List<String>> res = new ArrayList<>();
        bt(uniqueSets, 0,res, new ArrayList<>());
        return res;
    }

    private void bt(List<HashSet<String>> uniqueSets, int index, List<List<String>> res, List<String> path) {

        if(path.size() == uniqueSets.size()) {
            ArrayList<String> k = new ArrayList<>(path);
            res.add(k);
            return;
        }

        HashSet<String> uniqueSet = uniqueSets.get(index);
        for(String entry: uniqueSet) {
            path.add(entry);
            bt(uniqueSets, index+1, res, path);
            path.remove(path.size() - 1);
        }
    }

    //delete this later
    private Page pinPage(PageId pageno) throws PinPageException {
        try {
            Page page = new Page();
            SystemDefs.JavabaseBM.pinPage(pageno, page, false/*Rdisk*/);
            return page;
        } catch (Exception e) {
            e.printStackTrace();
            throw new PinPageException(e, "");
        }
    }


}
