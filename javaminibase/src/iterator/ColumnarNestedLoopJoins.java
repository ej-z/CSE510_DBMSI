package iterator;

import columnar.Columnarfile;
import columnar.TID;
import columnar.TupleScan;
import global.AttrType;
import heap.*;
import index.IndexException;

import java.io.IOException;

public class ColumnarNestedLoopJoins extends Iterator{
    private AttrType[] _in1,_in2;
    private int in1_len,in2_len;
    private Iterator outerIterator;
    private Tuple innerTuple, Jtuple, outerTuple;
    private CondExpr[] RightFilter, OutputFilter;
    private short t2_str_sizescopy[];
    private int n_buf_pages;
    private TupleScan innerScan;
    private boolean done,getfromouter;
    private FldSpec perm_mat[];
    private int n_out_flds;
    private Columnarfile cf;

    public ColumnarNestedLoopJoins(AttrType[] in1, short[] t1_str_sizes, AttrType[] in2, short[] t2_str_sizes, int amt_of_mem,
                                   Iterator am1,String relName, CondExpr[] outputFilter, CondExpr[] rightFilter, FldSpec[] proj_list,
                                   int nout_flds){
        _in1 = new AttrType[in1.length];
        _in2 = new AttrType[in2.length];
        in1_len = in1.length;
        in2_len = in2.length;
        System.arraycopy(in1, 0, _in1, 0, in1.length);
        System.arraycopy(in2, 0, _in2, 0, in2.length);
        outerIterator = am1;
        innerTuple = new Tuple();
        Jtuple=new Tuple();
        RightFilter = rightFilter;
        OutputFilter=outputFilter;
        n_buf_pages = amt_of_mem;
        innerScan = null;
        done = false;
        getfromouter = true;
        t2_str_sizescopy=t2_str_sizes;
        AttrType[] Jtypes=new AttrType[nout_flds];
        short[] t_size;
        perm_mat=proj_list;
        n_out_flds = nout_flds;
        try {
            t_size = TupleUtils.setup_op_tuple(Jtuple,Jtypes,in1,in1_len,in2,in2_len,t1_str_sizes,t2_str_sizes,proj_list,n_out_flds);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TupleUtilsException e) {
            e.printStackTrace();
        }
        try {
            cf = new Columnarfile(relName);
        } catch (HFException e) {
            e.printStackTrace();
        } catch (HFBufMgrException e) {
            e.printStackTrace();
        } catch (HFDiskMgrException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Tuple get_next() throws Exception {
        if(done)
            return null;
        do{
            if(getfromouter==true){
                getfromouter=false;
                if(innerScan!=null){
                    innerScan=null;
                }
                try {
                    innerScan=cf.openTupleScan();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if((outerTuple=outerIterator.get_next())==null){
                    done=true;
                    if(innerScan!=null){
                        innerScan=null;
                    }
                    return null;
                }
            }

            TID tid=new TID();
            while((innerTuple=innerScan.getNext(tid))!=null){
                innerTuple.setHdr((short)in2_len,_in2,t2_str_sizescopy);
                if(PredEval.Eval(RightFilter,innerTuple,null,_in2,null)==true){
                    if(PredEval.Eval(OutputFilter,outerTuple,innerTuple,_in1,_in2)==true){
                        Projection.Join(outerTuple,_in1,innerTuple,_in2,Jtuple,perm_mat,n_out_flds);
                        return Jtuple;
                    }
                }
            }
            getfromouter=true;
        }while(true);
    }

    public void close() throws JoinsException, IOException, IndexException {
        if (!closeFlag) {

            try {
                outerIterator.close();
            } catch (Exception e) {
                throw new JoinsException(e, "NestedLoopsJoin.java: error in closing iterator.");
            }
            closeFlag = true;
        }
    }
}
