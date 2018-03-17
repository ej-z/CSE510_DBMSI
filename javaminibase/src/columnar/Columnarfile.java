package columnar;



import java.io.IOException;
import java.util.ArrayList;

import global.*;
import heap.*;

public class Columnarfile {
	short numColumns;
	AttrType[] atype = null;
	short[] attrsizes;

	//Best way handle +2 bytes for strings instead of multiple ifs
    short[] asize;
	Heapfile[] hf = null;
    String fname = null;
    int tupleCnt = 0;
    //for fetching the file
    public Columnarfile(java.lang.String name) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
		Heapfile f = null;
		Scan scan = null;
		RID rid = null;
		fname = name;

		try {
			PageId pid = SystemDefs.JavabaseDB.get_file_entry(name+ ".hdr");
			if (pid == null) {
				throw new Exception("Columnar with the name: " + name + ".hdr doesn't exists");
			}

			f = new Heapfile(name + ".hdr");

			//Header tuple is organized this way
			//NumColumns, AttrType1, AttrSize1, AttrType2, AttrSize2,

			scan = f.openScan();
			Tuple t = scan.getNext(new RID());
			this.numColumns = (short) t.getIntFld(1);
			this.tupleCnt = t.getIntFld(2);
			atype = new AttrType[numColumns];
			attrsizes = new short[numColumns];
            asize=new short[numColumns];
			hf = new Heapfile[numColumns+1];
			hf[0] = f;
			int k = 0;
			for (int i = 0; i < numColumns; i++, k = k + 2) {
				atype[i] = new AttrType(t.getIntFld(3 + k));
				attrsizes[i] = (short)t.getIntFld(4 + k);
				asize[i] = attrsizes[i];
				if(atype[i].attrType == AttrType.attrString)
				    asize[i] += 2;
				hf[i+1] = new Heapfile(name + String.valueOf(i));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
    /*Convention: hf[] => 0 for hdr file; the rest for tables. The implementation is modified from HFTest.java*/
	public Columnarfile(java.lang.String name, int numcols, AttrType[] types, short[] attrSizes) throws IOException, InvalidTupleSizeException, InvalidTypeException, FieldNumberOutOfBoundException, SpaceNotAvailableException, HFException, HFBufMgrException, InvalidSlotNumberException, HFDiskMgrException {
		RID rid1 = new RID();
		boolean status = true;
		try{
			hf = new Heapfile[numcols+1];
			//hf[0] for header file by default
			hf[0] = new Heapfile(name+".hdr");

		}
		catch(Exception e){
			status = false;
			e.printStackTrace();
		}
		if(status==true){
			//initializing member variables
			numColumns = (short)(numcols);
			this.fname = name;
			//tid?
			//tids = new TID[numColumns];
			//for TID
			atype = new AttrType[numColumns];
			attrsizes=new short[numColumns];
            asize=new short[numColumns];
			int k = 0;
			for(int i=0;i<numcols;i++){
				atype[i] = new AttrType(types[i].attrType);
				switch(types[i].attrType){
					case 0:
						asize[i] = attrsizes[i]=attrSizes[k];
						asize[i] += 2;
						k++;
						break;
					case 1:
                    case 2:
                        asize[i] = attrsizes[i]=4;
						break;
					case 3:
                        asize[i] = attrsizes[i]=1;
						break;
					case 4:
						attrsizes[i]=0;
						break;
				}
			}
			System.out.println("Inserting "+numColumns+" records");

            AttrType[] htypes = new AttrType[2+(numcols*2)];
            for(int i =0; i < htypes.length; i++) {
                htypes[i] = new AttrType(AttrType.attrInteger);
            }
            short[] hsizes = new short[0];
            Tuple ht = new Tuple();
            ht.setHdr((short)htypes.length, htypes, hsizes);
            int size = ht.size();

            ht = new Tuple(size);
            ht.setHdr((short)htypes.length, htypes, hsizes);
            ht.setIntFld(1, numcols);
            ht.setIntFld(2, tupleCnt);
            int j = 0;
            for(int i = 0; i < numcols; i++,j=j+2){
                ht.setIntFld(3+j, atype[i].attrType);
                ht.setIntFld(4+j, attrsizes[i]);
            }
            hf[0].insertRecord(ht.returnTupleByteArray());

			//allocating memory for the others
			try{
				for(int i=1;i<=numColumns;i++){
					hf[i] = new Heapfile(name+String.valueOf(i-1));
				}
			}
			catch(Exception e){
				status = false;
				e.printStackTrace();
			}
		}
	}
	public void deleteColumnarFile() throws InvalidSlotNumberException, FileAlreadyDeletedException, InvalidTupleSizeException, HFBufMgrException, HFDiskMgrException, IOException{
		for(int i=0;i<=numColumns;i++){
			hf[i].deleteFile();
		}
		hf = null;
		atype = null;
		fname = null;
		numColumns = 0;
	}

	//Assumption: tupleptr contains header information.
	public TID insertTuple(byte[] tuplePtr) throws IOException, InvalidTypeException, InvalidTupleSizeException, FieldNumberOutOfBoundException, SpaceNotAvailableException, HFException, HFBufMgrException, InvalidSlotNumberException, HFDiskMgrException {

	    int offset = getOffset();
        RID[] rids = new RID[numColumns];
	    for(int i =0; i<numColumns;i++){

	        int size = 6 + asize[i]; //6 bytes for count and offset

            AttrType[] ttype = new AttrType[1];
            ttype[0] = atype[i];
            short[] tsize = new short[1];
            tsize[0] = attrsizes[i];
	        Tuple t = new Tuple(size);
	        t.setHdr((short)1,ttype,tsize);
            byte[] data = t.getTupleByteArray();
            System.arraycopy(tuplePtr,offset,data,6,asize[i]);
            t.tupleInit(data,0,data.length);
            rids[i] = hf[i+1].insertRecord(t.getTupleByteArray());
            offset += asize[i];
        }
		TID tid = new TID(numColumns, tupleCnt, rids);
        tupleCnt++;
		return tid;
	}
	public Tuple getTuple(TID tidarg) throws Exception {

        Tuple result = new Tuple(getTupleSize());
        result.setHdr(numColumns, atype, getStrSize());
        byte[] data = result.getTupleByteArray();
        int offset = getOffset();
        for (int i = 0; i < numColumns; i++) {
            Tuple t = hf[i+1].getRecord(tidarg.recordIDs[i]);
            System.arraycopy(t.getTupleByteArray(),6,data,offset,attrsizes[i]);
            offset += attrsizes[i];
        }

        result.tupleInit(data, 0, data.length);

        return result;
    }
	public ValueClass getValue(TID tidarg, int column) throws Exception {

        Tuple tempvalue;
        tempvalue = hf[column + 1].getRecord(tidarg.recordIDs[column]);
        switch (atype[column].attrType) {
            case 0:
                ValueStr result1 = new ValueStr(tempvalue.getStrFld(column));
                return result1;
            case 1:
                //integer
                ValueInt result2 = new ValueInt(tempvalue.getIntFld(0));
                return result2;
            case 2:
                //real
                ValueFloat result3 = new ValueFloat(tempvalue.getFloFld(column));
                return result3;
        }

        return null;
    }
	int getTupleCnt(){
		return tupleCnt;
	}
	public TupleScan openTupleScan() throws InvalidTupleSizeException, IOException{
		//must clarify with others
		TupleScan result=new TupleScan(this);
		return result;		
	}
	public Scan openColumnScan(int columnNo) throws Exception{
		Scan scanobj=null;
		if(columnNo < hf.length){
			scanobj = new Scan(hf[columnNo+1]);
		}
		else{
			
			throw new Exception("Invalid Column number");
		}
		
		return scanobj;		
	}
	public boolean updateTuple(TID tidarg, Tuple newtuple){
		try{
		Tuple old = getTuple(tidarg);
		old.tupleCopy(newtuple);
		RID[] ridargs=new RID[numColumns];
		for(int i=0;i<numColumns;i++){
			ridargs[i].copyRid(tidarg.recordIDs[i]);
		}
		//insert it back to the corresponding position
		//rid, newtuple
		for(int i=0;i<numColumns;i++){
			switch(attrsizes[i]){
			case 0:
				//string
				String firststr = old.getStrFld(i);
				Tuple temp = new Tuple(firststr.getBytes(),0,firststr.length());
				hf[i+1].updateRecord(ridargs[i], temp);
				break;
			case 1:
				//integer
				int firstint = old.getIntFld(i);
				Tuple temp1 = new Tuple(String.valueOf(firstint).getBytes(),0,4);
				hf[i+1].updateRecord(ridargs[i], temp1);
				break;
			case 2:
				//float
				float firstfloat = old.getFloFld(i);
				Tuple temp2 = new Tuple(String.valueOf(firstfloat).getBytes(),0,4);
				hf[i+1].updateRecord(ridargs[i], temp2);
				break;
			case 3:
				//symbol
				break;
			}
		}
		return true;
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return false;
	}
	public boolean  updateColumnofTuple(TID tidarg, Tuple newtuple, int column){
		try{
			Tuple old = getTuple(tidarg);
			old.tupleCopy(newtuple);
			RID[] ridargs=new RID[numColumns];
			for(int i=0;i<numColumns;i++){
				ridargs[i].copyRid(tidarg.recordIDs[i]);
			}
			//insert it back to the corresponding position
			//rid, newtuple
			
				switch(attrsizes[column]){
				case 0:
					//string
					String firststr = old.getStrFld(column);
					Tuple temp = new Tuple(firststr.getBytes(),0,firststr.length());
					hf[column+1].updateRecord(ridargs[column], temp);
					return true;
				case 1:
					//integer
					int firstint = old.getIntFld(column);
					Tuple temp1 = new Tuple(String.valueOf(firstint).getBytes(),0,4);
					hf[column+1].updateRecord(ridargs[column], temp1);
					return true;
				case 2:
					//float
					float firstfloat = old.getFloFld(column);
					Tuple temp2 = new Tuple(String.valueOf(firstfloat).getBytes(),0,4);
					hf[column+1].updateRecord(ridargs[column], temp2);
					return true;
				case 3:
					//symbol
					return true;
				}
			
			}
			catch(Exception e){
				e.printStackTrace();
			}
			return false;	
	}

	public int getTupleSize(){

	    int size = getOffset();
	    for(int i = 0; i < numColumns; i++){
	        size += asize[i];
        }
        return size;
    }

    public short[] getStrSize(){

        int n = 0;
        for(int i = 0; i < numColumns; i++){
            if(atype[i].attrType == AttrType.attrString)
                n++;
        }

        short[] strSize = new short[n];
        int cnt = 0;
        for(int i = 0; i < numColumns; i++){
            if(atype[i].attrType == AttrType.attrString) {
                strSize[cnt++] = attrsizes[i];
            }
        }

        return strSize;
    }

    public int getOffset(){

        return 4 + (numColumns*2);
    }
}
