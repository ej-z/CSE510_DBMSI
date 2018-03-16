package columnar;

import java.io.IOException;
import global.RID;
import heap.InvalidTupleSizeException;
import heap.Scan;
import heap.Tuple;

public class TupleScan {
	Scan[] sc;	
	public TupleScan(){
		
	}
	public TupleScan(Columnarfile fname) throws InvalidTupleSizeException, IOException{
		sc=new Scan[fname.numColumns];
		for(int i=0;i<fname.numColumns;i++){
			sc[i] = fname.hf[i+1].openScan();
		}
	}
	void closetuplescan(){
		for(int i=0;i<sc.length;i++){
			sc[i].closescan();
		}	
	}
	Tuple getNext(TID tid) throws InvalidTupleSizeException, IOException{
		Tuple[] temparray=new Tuple[sc.length];
		RID[] rids = new RID[sc.length];
		RID rid=new RID();
		for(int i=0;i<sc.length;i++){
			temparray[i]=sc[i].getNext(rid);
			rids[i] = new RID();
			rids[i].copyRid(rid);
			rid=new RID();
		}
		//combine this array to a single tuple and send it
		byte[] datatobe = new byte[sc.length];
		int counter=0;
		for(int j = 0;j<sc.length;j++){
			System.arraycopy(temparray[j].returnTupleByteArray(), 0, datatobe, counter, temparray[j].getLength());
			counter+=temparray[j].getLength();
		}
		tid.numRIDs = sc.length;
		tid.recordIDs = rids;
		//TODO: set TID position
		Tuple result=new Tuple(datatobe,0,counter);
		return result;
	}
	boolean position(TID tidarg){
		RID[] ridstemp=new RID[tidarg.numRIDs];
		for(int i=0;i<tidarg.numRIDs;i++){
			ridstemp[i].copyRid(tidarg.recordIDs[i]);
			try {
				boolean ret=sc[i].position(ridstemp[i]);
				if(ret==false){
					return false;
				}
			} catch (InvalidTupleSizeException e) {
				
				e.printStackTrace();
			} catch (IOException e) {
				
				e.printStackTrace();
			}
		}
		return true;
	}
	
}
