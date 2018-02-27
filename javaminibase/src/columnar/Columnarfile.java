package columnar;

import java.io.IOException;

import global.AttrType;
import global.Convert;
import global.RID;
import heap.FieldNumberOutOfBoundException;
import heap.FileAlreadyDeletedException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;

public class Columnarfile_trial {
	private static short numColumns;
	private AttrType[] atype = null;
	short[] attrsizes;
	Heapfile[] hf = null;  
    private String fname = null;
    TID[] tids; 
    /*Convention: hf[] => 0 for hdr file; the rest for tables. The implementation is modified from HFTest.java*/
	Columnarfile_trial(java.lang.String name, int numcols, AttrType[] types) throws IOException{
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
			tids = new TID[numColumns];
			//for TID
			atype = new AttrType[numColumns];
			attrsizes=new short[numColumns];
			
			for(int i=0;i<numcols;i++){
				atype[i] = new AttrType(types[i].attrType);
				switch(types[i].attrType){
					case 0:
						//assuming only numbers
						break;
					case 1:
						attrsizes[i]=4;
						break;
					case 2:
						attrsizes[i]=4;
						break;
					case 3:
						attrsizes[i]=1;
						break;
					case 4:
						attrsizes[i]=0;
						break;
				}
			}
			System.out.println("Inserting "+numColumns+" records");
			//.hdr file initialization hf[0]
			for(int i=0;i<numColumns;i++){				
	            //data = "name.colnumber" + type[i].attrType
	            StringBuilder sb = new StringBuilder();
	            sb.append(this.fname);
	            sb.append(".");
	            sb.append(String.valueOf(i));
	            sb.append("--");
	            sb.append(String.valueOf(atype[i].attrType));
	            String datastr = sb.toString();
	            
	            DummyRecord rec = new DummyRecord(datastr.getBytes());
	            try {
	                rid1 = hf[0].insertRecord(rec.toByteArray());
	            }
	            catch (Exception e) {
                    status = false;
                    System.err.println("*** Error inserting record " + i + "\n");
                    e.printStackTrace();
                }
			}
			//allocating memory for the others
			try{
				for(int i=1;i<numColumns;i++){
					hf[i] = new Heapfile(name+String.valueOf(i));
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
	public TID insertTuple(byte[] tuplePtr) throws IOException, InvalidTypeException, InvalidTupleSizeException, FieldNumberOutOfBoundException{
		Tuple temp = new Tuple();
		boolean status = true;
		temp.setHdr(numColumns, atype, attrsizes);
		temp.tupleInit(tuplePtr, 0, tuplePtr.length);
		RID[] rids = new RID[numColumns+1];
		for(int i=0;i<numColumns;i++){
			switch(atype[i].attrType){
				case 0:
					//string
					break;
				case 1:
					//integer
					int datafromtuple = temp.getIntFld(i);
					DummyRecord rec = new DummyRecord(String.valueOf(datafromtuple).getBytes());
		            try{
		            	byte[] two = rec.toByteArray();	
		            	//assumption that inserted at the end
		                rids[i] = hf[i+1].insertRecord(two);
		            }
		            catch (Exception e) {
	                    status = false;
	                    System.err.println("*** Error inserting record " + (i) + "\n");
	                    e.printStackTrace();
	                }
		            break;
				case 2:
					//real
					float datafromtuple1 = temp.getFloFld(i);
					DummyRecord rec1 = new DummyRecord(String.valueOf(datafromtuple1).getBytes());
		            try {
		            	byte[] two = rec1.toByteArray();		            	
		                rids[i] = hf[i+1].insertRecord(two);
		            }
		            catch (Exception e) {
	                    status = false;
	                    System.err.println("*** Error inserting record " + (i) + "\n");
	                    e.printStackTrace();
	                }
		            break;
		        default:
		        	//do nothing
			}
		}
		int last_index = tids.length;
		tids[last_index]=new TID(numColumns, last_index, rids);
		return tids[last_index];	
	}
	public Tuple getTuple(TID tidarg) throws Exception{
		//fetch the columns from the heap file with the specified RIDs
		int i;
		for(i=0;i<tids.length;i++){
			if(tids[i].equals(tidarg)){
				break;
			}
		}
		Tuple[] tempjoin=new Tuple[numColumns];
		Tuple result;
		if(i<tids.length){
			//means found the TID
			int length = 0;
			for(int j=1;j<numColumns+1;j++){
				//assuming jth column corresponds to j+1th file
				tempjoin[j-1] = hf[j].getRecord(tids[i].recordIDs[j-1]);
				length += tempjoin[j-1].getLength();
			}
			byte[] datatobe = new byte[length];
			int counter=0;
			for(int j = 0;j<numColumns;j++){
				System.arraycopy(tempjoin[j].returnTupleByteArray(), 0, datatobe, counter, tempjoin[j].getLength());
				counter+=tempjoin[j].getLength();
			}
			result=new Tuple(datatobe,0,length);
		}
		else{
			throw new Exception("TID doesnot exist");
		}
		return result;
	}
	 
}

class DummyRecord {

    //content of the record
    public int ival;
    public float fval;
    public String name;

    //length under control
    private int reclen;

    private byte[] data;

    /**
     * Default constructor
     */
    public DummyRecord() {
    }
    /**
     * another constructor
     */
    public DummyRecord(int _reclen) {
        setRecLen(_reclen);
        data = new byte[_reclen];
    }
public DummyRecord(byte[] arecord)
        throws java.io.IOException {
    setIntRec(arecord);
    setFloRec(arecord);
    setStrRec(arecord);
    data = arecord;
    setRecLen(name.length());
}

/**
 * constructor: translate a tuple to a DummyRecord object
 * it will make a copy of the data in the tuple
 *
 * @param atuple: the input tuple
 */
public DummyRecord(Tuple _atuple)
        throws java.io.IOException {
    data = new byte[_atuple.getLength()];
    data = _atuple.getTupleByteArray();
    setRecLen(_atuple.getLength());

    setIntRec(data);
    setFloRec(data);
    setStrRec(data);

}

/**
 * convert this class objcet to a byte array
 * this is used when you want to write this object to a byte array
 */
public byte[] toByteArray()
        throws java.io.IOException {
    //    data = new byte[reclen];
    Convert.setIntValue(ival, 0, data);
    Convert.setFloValue(fval, 4, data);
    Convert.setStrValue(name, 8, data);
    return data;
}

/**
 * get the integer value out of the byte array and set it to
 * the int value of the DummyRecord object
 */
public void setIntRec(byte[] _data)
        throws java.io.IOException {
    ival = Convert.getIntValue(0, _data);
}

/**
 * get the float value out of the byte array and set it to
 * the float value of the DummyRecord object
 */
public void setFloRec(byte[] _data)
        throws java.io.IOException {
    fval = Convert.getFloValue(4, _data);
}

/**
 * get the String value out of the byte array and set it to
 * the float value of the HTDummyRecorHT object
 */
public void setStrRec(byte[] _data)
        throws java.io.IOException {
    // System.out.println("reclne= "+reclen);
    // System.out.println("data size "+_data.size());
    name = Convert.getStrValue(8, _data, reclen - 8);
}

//Other access methods to the size of the String field and
//the size of the record
public void setRecLen(int size) {
    reclen = size;
}

public int getRecLength() {
    return reclen;
}
}
