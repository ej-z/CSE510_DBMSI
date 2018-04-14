package columnar;

import bitmap.BitMapFile;
import btree.PinPageException;
import diskmgr.Page;
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

        List<HashSet> uniSet = getUniqueSetFromJoin(joinExp, leftColumnarFile, rightColumnarFile);




    }


    private List<HashSet> getUniqueSetFromJoin(CondExpr[] joinEquation, Columnarfile leftColumnarFile,
                                               Columnarfile rightColumnarFile) throws Exception {

        List<HashSet> uniquesList = new ArrayList<>();

        for(int i = 0; i < joinEquation.length; i++) {

            CondExpr currentCondition = joinEquation[i];
            while(currentCondition != null) {

                FldSpec symbol = currentCondition.operand1.symbol;
                int offset = symbol.offset;

                // move this is to interface we assume that the colums already have indexes
                leftColumnarFile.createAllBitMapIndexForColumn(offset - 1);
                HashMap<String, BitMapFile> allBitMaps = leftColumnarFile.getAllBitMaps();

                FldSpec symbol2 = currentCondition.operand2.symbol;
                int offset2 = symbol2.offset;

                rightColumnarFile.createAllBitMapIndexForColumn(offset2 -1);
                AttrType attrtypeforLeftcolumn =  leftColumnarFile.getAttrtypeforcolumn(offset -1);
                AttrType attrtypeforRightColumn = rightColumnarFile.getAttrtypeforcolumn(offset2 -1);


                HashSet<String> set1 = extractUniqueValues(offset - 1, allBitMaps);
                HashSet<Integer> setLeftInt = null, setRightInt = null;
                if(attrtypeforLeftcolumn.attrType == AttrType.attrInteger) {
                    setLeftInt = set1.stream().map(Integer::parseInt).collect(Collectors.toCollection(HashSet::new));
                }

                //inner

                HashMap<String, BitMapFile> allRightRelationBitMaps = rightColumnarFile.getAllBitMaps();


                HashSet<String> set2 = extractUniqueValues(offset2 -1, allRightRelationBitMaps);
                if(attrtypeforRightColumn.attrType == AttrType.attrInteger) {
                     setRightInt = set2.stream().map(Integer::parseInt).collect(Collectors.toCollection(HashSet::new));
                }

                if(setLeftInt != null && setRightInt != null) {
                    setLeftInt.retainAll(setRightInt);
                    uniquesList.add(setLeftInt);
                } else {
                    set1.retainAll(set2);
                    uniquesList.add(set1);
                }

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
