package columnar;



import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import global.AttrType;
import global.Convert;
import global.RID;
import heap.FieldNumberOutOfBoundException;
import heap.FileAlreadyDeletedException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.Tuple;

public class Columnarfile {
	static short numColumns;
	AttrType[] atype = null;
	short[] attrsizes;
	Heapfile[] hf = null;  
    String fname = null;
    ArrayList<TID> tids;
    //for fetching the file
    Columnarfile(java.lang.String name) throws HFException, HFBufMgrException, HFDiskMgrException, IOException{
    	Heapfile f = null;
    	Scan scan = null;
    	RID rid=null;
    	
    	
    	try{
            f=new Heapfile(name+".hdr");
            scan = f.openScan();
            rid = new RID();
            //Tuple tuple;
            boolean done=false;
            fname = name;
            int counter=0;
            ArrayList<Integer> attrtypetemp = new ArrayList<Integer>();
            while(!done){
            	Tuple tuple = scan.getNext(rid);
            	if(tuple!=null){
            		done = true;
            	}
            	else{
            		String data=tuple.getStrFld(0);
            		String[] split_data=data.split("-");
            		attrtypetemp.add(Integer.parseInt(split_data[1]));
            		counter+=1;
            	}
            }
            numColumns = (short) counter;
            atype=new AttrType[numColumns];
            attrsizes=new short[numColumns];
            for(int k=0;k<numColumns;k++){
            	atype[k].attrType = attrtypetemp.get(k);
            	switch(atype[k].attrType){
	            	case 0:
	            		attrsizes[k]=25;
	            		break;
	            	case 1:
	            		attrsizes[k]=4;
	            		break;
	            	case 2:
	            		attrsizes[k]=4;
	            		break;
            	}
            	hf[k+1]=new Heapfile(name+String.valueOf(k));
            }
            //till here all the member vars except tids<> is set
            //for each entry in heapfile
            Scan[] scanobj=new Scan[numColumns];
            for(int k=0;k<numColumns;k++){
            	scanobj[k]=hf[k+1].openScan();
            }
            done = false;
            RID[] ridtemp=new RID[numColumns];
            int postemp=0;
            while(!done){
            	Tuple[] tuparray=new Tuple[numColumns];
            	for(int i=0;i<numColumns;i++){
            		tuparray[i]=scanobj[i].getNext(ridtemp[i]);
            		if(tuparray[i]==null){
            			done=true;
            			break;
            		}
            	}
            	if(!done){
            		//get rids of all the tuples
            		tids.add(new TID(numColumns,postemp,ridtemp));
            		postemp++;
            		ridtemp=new RID[numColumns];
            	}
            }
    	}
    	catch(Exception e){
    		e.printStackTrace();
    	}
    	
    }
    /*Convention: hf[] => 0 for hdr file; the rest for tables. The implementation is modified from HFTest.java*/
	Columnarfile(java.lang.String name, int numcols, AttrType[] types) throws IOException{
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
			
			for(int i=0;i<numcols;i++){
				atype[i] = new AttrType(types[i].attrType);
				switch(types[i].attrType){
					case 0:
						attrsizes[i]=25;
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
	            //data = "name.colnumber-" + type[i].attrType
	            StringBuilder sb = new StringBuilder();
	            sb.append(this.fname);
	            //sb.append('.');
	            sb.append(String.valueOf(i));
	            sb.append('-');
	            sb.append(String.valueOf(atype[i].attrType));
	            sb.append('\n');
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
					String datafromtuple1 = temp.getStrFld(i);
					DummyRecord rec1 = new DummyRecord(datafromtuple1.getBytes());
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
					float datafromtuple11 = temp.getFloFld(i);
					DummyRecord rec11 = new DummyRecord(String.valueOf(datafromtuple11).getBytes());
		            try {
		            	byte[] two = rec11.toByteArray();		            	
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
		int last_index = tids.size();
		tids.add(new TID(numColumns, last_index, rids));
		//this should be added to the header file2
		return tids.get(last_index);	
	}
	public Tuple getTuple(TID tidarg) throws Exception{
		//fetch the columns from the heap file with the specified RIDs
		int i;
		for(i=0;i<tids.size();i++){
			if(tids.get(i).equals(tidarg)){
				break;
			}
		}
		Tuple[] tempjoin=new Tuple[numColumns];
		Tuple result;
		if(i<tids.size()){
			//means found the TID
			int length = 0;
			for(int j=1;j<numColumns+1;j++){
				//assuming jth column corresponds to j+1th file
				tempjoin[j-1] = hf[j].getRecord(tids.get(i).recordIDs[j-1]);
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
	public ValueClass getValue(TID tidarg, int column) throws Exception{
		int i;
		for(i=0;i<tids.size();i++){
			if(tids.get(i).equals(tidarg)){
				break;
			}
		}
		
		Tuple tempvalue;
		if(i<tids.size()){
			tempvalue = hf[column+1].getRecord(tids.get(i).recordIDs[column]);
			switch(atype[column].attrType){
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
		}
		else{
			throw new Exception("TID doesnot exist");
		}
		return null;	
	}
	int getTupleCnt(){
		int result = tids.size();
		return result;
	}
	TupleScan openTupleScan() throws InvalidTupleSizeException, IOException{
		//must clarify with others
		Scan[] scanobj=new Scan[numColumns];
        for(int k=0;k<numColumns;k++){
        	scanobj[k]=hf[k+1].openScan();
        }
		return null;		
	}
	Scan openColumnScan(int columnNo) throws Exception{
		Scan scanobj=null;
		if(columnNo < hf.length){
			scanobj = new Scan(hf[columnNo+1]);
		}
		else{
			
			throw new Exception("Invalid Column number");
		}
		
		return scanobj;		
	}
	boolean updateTuple(TID tidarg, Tuple newtuple){
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
	boolean  updateColumnofTuple(TID tidarg, Tuple newtuple, int column){
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
