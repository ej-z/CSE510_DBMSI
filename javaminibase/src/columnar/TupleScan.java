package columnar;

import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import global.*;
import heap.*;
import iterator.*;

import java.io.IOException;

public class TupleScan {
	//private Columnarfile file;
	Scan[] sc;

	private AttrType[] atype = null;
	private short[] asize = null;
	private short[] strSize = null;
	private short numColumns;
	private int toffset;
	private int tuplesize;
	private Sort deletedTuples;
	private int currDeletePos = -1;

    /**
     * Initiates scan on all columns
     *
     * @param f Columnarfile object
     * @throws InvalidTupleSizeException
     * @throws IOException
     */
	public TupleScan(Columnarfile f) throws Exception {

        numColumns = f.numColumns;
        atype = f.atype;
        asize = f.asize;
        strSize = f.getStrSize();
        toffset = f.getOffset();
        tuplesize = f.getTupleSize();
        sc=new Scan[numColumns];
        for(int i=0;i<numColumns;i++){
            sc[i] = f.getColumn(i).openScan();
        }

		PageId pid = SystemDefs.JavabaseDB.get_file_entry(f.getDeletedFileName());
		if (pid != null) {
			AttrType[] types = new AttrType[1];
			types[0] = new AttrType(AttrType.attrInteger);
			short[] sizes = new	short[0];
			FldSpec[] projlist = new FldSpec[1];
			projlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
			FileScan fs = new FileScan(f.getDeletedFileName(), types, sizes, (short)1, 1, projlist, null);
			deletedTuples = new Sort(types, (short) 1, sizes, fs, 1, new TupleOrder(TupleOrder.Ascending), 4, 10);
			currDeletePos = deletedTuples.get_next().getIntFld(1);
		}
    }

    /**
     * This constructor truly takes advantage of columnar organization by scanning only
     * required columns.
     *
     * @param f Columnarfile object
     * @param columns array of column numbers that need to be scanned
     * @throws InvalidTupleSizeException
     * @throws IOException
     */
    public TupleScan(Columnarfile f,short[] columns) throws Exception {

        numColumns = (short)columns.length;
        atype = new AttrType[numColumns];
        asize = new short[numColumns];
        sc=new Scan[numColumns];
        short strCnt = 0;
        for(int i=0;i<numColumns;i++){

            short c = columns[i];
            atype[i] = f.atype[c];
            asize[i] = f.asize[c];
            sc[i] = f.getColumn(c).openScan();

            if(atype[i].attrType == AttrType.attrString)
                strCnt++;
        }

        strSize = new short[strCnt];
        toffset = 4 + (numColumns * 2);
        tuplesize = toffset;
        int cnt = 0;
        for(int i = 0; i < numColumns; i++){
            short c = columns[i];
            if(atype[i].attrType == AttrType.attrString) {
                strSize[cnt++] = f.attrsizes[c];
            }
            tuplesize += asize[i];
        }

		PageId pid = SystemDefs.JavabaseDB.get_file_entry(f.getDeletedFileName());
		if (pid != null) {
			AttrType[] types = new AttrType[1];
			types[0] = new AttrType(AttrType.attrInteger);
			short[] sizes = new	short[0];
			FldSpec[] projlist = new FldSpec[1];
			projlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
			FileScan fs = new FileScan(f.getDeletedFileName(), types, sizes, (short)1, 1, projlist, null);
			deletedTuples = new Sort(types, (short) 1, sizes, fs, 1, new TupleOrder(TupleOrder.Ascending), 4, 10);
			currDeletePos = deletedTuples.get_next().getIntFld(1);
		}
    }
	public void closetuplescan(){
		for(int i=0;i<sc.length;i++){
			sc[i].closescan();
		}
	}
	public Tuple getNext(TID tid) throws Exception {

		Tuple result = new Tuple(tuplesize);
		result.setHdr(numColumns, atype, strSize);
		byte[] data = result.getTupleByteArray();
		RID[] rids = new RID[sc.length];
		RID rid=new RID();
		int position = 0;
		boolean canContinue;
		int offset = toffset;
		do {
			canContinue = false;
			for (int i = 0; i < numColumns; i++) {
				Tuple t = sc[i].getNext(rid);
				if(t != null && deletedTuples != null){
					position = sc[i].positionOfRecord(rid);
					if(position > currDeletePos){
						while (true){
							Tuple dtuple = deletedTuples.get_next();
							currDeletePos = dtuple.getIntFld(1);
							if(currDeletePos >= position)
								break;
						}
					}
					if(position == currDeletePos){
						for (int j = 1; j < numColumns; j++){
							sc[j].getNext(rid);
						}
						canContinue = true;
						Tuple dtuple = deletedTuples.get_next();
						if(dtuple != null)
							currDeletePos = dtuple.getIntFld(1);
						break;
					}
				}

				if(canContinue)
				    continue;

				if (t == null)
					return null;

				rids[i] = new RID();
				rids[i].copyRid(rid);
				rid = new RID();
				int size = asize[i]; //6 bytes for count and offset
				System.arraycopy(t.getTupleByteArray(), 6, data, offset, size);
				offset += asize[i];
			}
		}while (canContinue);
		tid.numRIDs = sc.length;
		tid.recordIDs = rids;
		tid.setPosition(position);
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