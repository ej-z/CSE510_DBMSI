package bitmap;

import columnar.Columnarfile;
import columnar.ValueClass;
import global.GlobalConst;
import global.PageId;
import heap.*;

import java.io.IOException;

public class BitMapFile implements GlobalConst {
    // TODO: Check if any other fields are needed
    private String fileName;
    private BitMapHeaderPage headerPage;
    private PageId headerPageId;
    private Heapfile heapfile;

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

    public PageId getHeaderPageId() {
        return headerPageId;
    }

    public void setHeaderPageId(PageId headerPageId) {
        this.headerPageId = headerPageId;
    }

    // TODO: Complete the definition of constructor
    public BitMapFile(String fileName) {
        this.fileName = fileName;
    }

    // TODO: Complete the definition of constructor
    public BitMapFile(String filename, Columnarfile columnfile, Integer columnNo, ValueClass value) throws
            IOException, HFException, HFBufMgrException, HFDiskMgrException, InvalidTupleSizeException {


        heapfile = new Heapfile(filename + " " + columnNo);
        Scan scan = heapfile.openScan();

        
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

}
