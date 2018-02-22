package columnar;

import global.RID;

public class TID {
    int numRIDs;
    int position;
    RID[] recordIDs;

    TID(int numRIDs){}
    TID(int numRIDs, int position){}
    TID(int numRIDs, int position, RID[] recordIDs){}

    void copyTid(TID tid){

    }

    boolean equals(TID tid){
        return false;
    }

    void writeToByteArray(byte[] array, int offset){

    }

    void setPosition(int position){

    }

    void setRID(int column, RID recordID){

    }
}
