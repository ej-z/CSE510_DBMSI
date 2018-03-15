package bitmap;

import btree.GetFileEntryException;
import columnar.Columnarfile;
import columnar.ValueClass;
import columnar.ValueInt;
import columnar.ValueString;
import global.AttrType;
import global.GlobalConst;
import global.PageId;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.InvalidTupleSizeException;

import java.io.IOException;

public class BitMapFile implements GlobalConst {
    // TODO: Check if any other fields are needed
    private String fileName;
    private BitMapHeaderPage headerPage;
    private PageId headerPageId;
    private String columnarFileName;
    private Integer columnNumber;
    private AttrType attrType;
    private ValueClass value;

    public Integer getColumnNumber() {
        return columnNumber;
    }

    public void setColumnNumber(Integer columnNumber) {
        this.columnNumber = columnNumber;
    }

    public ValueClass getValue() {
        return value;
    }

    public void setValue(ValueClass value) {
        this.value = value;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public BitMapHeaderPage getHeaderPage() {
        return headerPage;
    }

    public void setHeaderPage(BitMapHeaderPage headerPage) {
        this.headerPage = headerPage;
    }

    // TODO: Complete the definition of constructor
    public BitMapFile(String fileName) throws Exception {
        this.fileName = fileName;
        String[] temp = fileName.split("-");
        if (temp.length != 4) {
            throw new Exception("Invalid BitMapFile name");
        }
        columnarFileName = temp[0];
        columnNumber = Integer.parseInt(temp[1]);
        attrType = new AttrType(Integer.parseInt(temp[2]));
        if (attrType.attrType == AttrType.attrString) {
            value = new ValueString(temp[3]);
        } else {
            value = new ValueInt(Integer.parseInt(temp[3]));
        }

        headerPageId = get_file_entry(fileName);
        if (headerPageId == null) {
            throw new Exception("This index file (" + fileName + ") doesn't exist");
        }
        headerPage = new BitMapHeaderPage(headerPageId);
    }

    // TODO: Complete the definition of constructor
    public BitMapFile(String filename, Columnarfile columnfile, Integer columnNo, ValueClass value) throws
            IOException, HFException, HFBufMgrException, HFDiskMgrException, InvalidTupleSizeException {
        
    }

    // TODO: Complete code for closing the file
    public void close() {

    }

    // TODO: Complete code for destroying the file
    public void destroyBitMapFile() {

    }

    // TODO: Complete code for delete operation
    public Boolean delete(int position) {
        return Boolean.TRUE;
    }

    // TODO: Complete code for insert operation
    public Boolean insert(int position) {
        return Boolean.TRUE;
    }

    private PageId get_file_entry(String filename)
            throws GetFileEntryException {
        try {
            return SystemDefs.JavabaseDB.get_file_entry(filename);
        } catch (Exception e) {
            e.printStackTrace();
            throw new GetFileEntryException(e, "");
        }
    }

}
