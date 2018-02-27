package columnar;

import java.io.IOException;

import global.AttrType;
import global.Convert;
import global.RID;
import heap.Heapfile;
import heap.Tuple;

public class Columnarfile_trial {
	private static int numColumns;
	private AttrType[] type;
	Heapfile[] hf;
	boolean status = true;    
    private String fname;
    /*Convention: hf[] => 0 for hdr file; the rest for tables. The implementation is modified from HFTest.java*/
	Columnarfile_trial(java.lang.String name, int numcols, AttrType[] types) throws IOException{
		RID rid1 = new RID();
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
			numColumns = numcols;	
			this.fname = name;
			for(int i=0;i<numColumns;i++){
				type[i] = new AttrType(types[i].attrType);
			}
			System.out.println("Inserting "+numcols+" records");
			//.hdr file initialization hf[0]
			for(int i=0;i<numColumns;i++){				
	            //data = "name.colnumber" + type[i].attrType
	            StringBuilder sb = new StringBuilder();
	            sb.append(this.fname);
	            sb.append(".");
	            sb.append(String.valueOf(i));
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
			//allocating memory for the
			try{
				for(int i=0;i<numColumns;i++){
					hf[i] = new Heapfile(name+String.valueOf(i));
				}
			}
			catch(Exception e){
				status = false;
				e.printStackTrace();
			}
		}
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
