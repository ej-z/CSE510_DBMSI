package columnar;

import heap.Scan;
import heap.Tuple;

public class TupleScan {
	Scan[] sc = null;
	public TupleScan(){
		
	}
	public TupleScan(Columnarfile fname){
		sc=new Scan[fname.numColumns];
	}
	Tuple tuple = new Tuple();
    boolean done = false;
}