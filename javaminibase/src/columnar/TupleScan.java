package columnar;

import java.io.IOException;

import global.AttrType;
import global.RID;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.Tuple;

public class TupleScan {
	private int counter = 0;
	private Columnarfile file;
	Scan[] sc;
	public TupleScan(){

	}
	public TupleScan(Columnarfile f) throws InvalidTupleSizeException, IOException{
		file = f;
		sc=new Scan[file.numColumns];
		for(int i=0;i<file.numColumns;i++){
			sc[i] = file.hf[i+1].openScan();
		}
	}
	public void closetuplescan(){
		for(int i=0;i<sc.length;i++){
			sc[i].closescan();
		}
	}
	public Tuple getNext(TID tid) throws InvalidTupleSizeException, IOException, InvalidTypeException {

		Tuple result = new Tuple(file.getTupleSize());
		result.setHdr(file.numColumns, file.atype, file.getStrSize());
		byte[] data = result.getTupleByteArray();
		int offset = file.getOffset();
		RID[] rids = new RID[sc.length];
		RID rid=new RID();
		for (int i = 0; i < file.numColumns; i++) {
			Tuple t = sc[i].getNext(rid);

			if(t == null)
				return null;

			rids[i] = new RID();
			rids[i].copyRid(rid);
			rid=new RID();
			int size = file.asize[i]; //6 bytes for count and offset
			System.arraycopy(t.getTupleByteArray(),6,data,offset,size);
			offset += file.asize[i];
		}
		tid.numRIDs = sc.length;
		tid.recordIDs = rids;
		tid.setPosition(counter++);
		result.tupleInit(data, 0, data.length);

		return result;

	}
	public boolean position(TID tidarg){
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