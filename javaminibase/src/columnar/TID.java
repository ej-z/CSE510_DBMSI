package columnar;

import global.Convert;
import global.RID;

public class TID {
    int numRIDs;
    int position;
    RID[] recordIDs;

    TID(int n){
        this.numRIDs = n;
    }
    TID(int n, int p){
        this(n);
        this.position = p;
    }
    TID(int n, int p, RID[] rids){
        this(n, p);
        recordIDs = new RID[n];
        for(int i=0;i<rids.length;i++){
        	recordIDs[i].copyRid(rids[i]);
        }
    }

    void copyTid(TID tid){

        numRIDs = tid.numRIDs;
        position = tid.position;
        recordIDs = new RID[numRIDs];
        for(int i = 0; i< numRIDs; i++){
        	recordIDs[i].copyRid(tid.recordIDs[i]);
        }
    }

    boolean equals(TID tid){

        if(numRIDs != tid.numRIDs) return false;
        if(position != tid.position) return false;
        for(int i = 0; i< numRIDs; i++){
            if(!recordIDs[i].equals(tid.recordIDs[i])) return false;
        }
        return true;
    }

    void writeToByteArray(byte[] array, int offset)throws java.io.IOException
    {
        Convert.setIntValue ( numRIDs, offset, array);
        Convert.setIntValue ( position, offset+4, array);

        for(int i = 0; i< numRIDs; i++){
            offset = offset + 8;
            recordIDs[i].writeToByteArray(array, offset);
        }
    }

    void setPosition(int position){

        this.position = position; //is this it? not sure
    }

    void setRID(int column, RID recordID){

        if(column < numRIDs){
            recordIDs[column].copyRid(recordID);
        }
    }
}
