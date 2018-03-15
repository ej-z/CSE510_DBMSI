package bitmap;

import btree.AddFileEntryException;
import btree.GetFileEntryException;
import columnar.Columnarfile;
import columnar.ValueClass;
import columnar.ValueInt;
import columnar.ValueString;
import global.AttrType;
import global.GlobalConst;
import global.PageId;
import global.SystemDefs;


public class BitMapFile implements GlobalConst {

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

    public BitMapFile(String fileName) throws Exception {
        this.fileName = fileName;
        headerPageId = get_file_entry(fileName);
        if (headerPageId == null) {
            throw new Exception("This index file (" + fileName + ") doesn't exist");
        }
        headerPage = new BitMapHeaderPage(headerPageId);

        columnarFileName = headerPage.getColumnarFileName();
        columnNumber = headerPage.getColumnNumber();
        attrType = headerPage.getAttrType();
        if (attrType.attrType == AttrType.attrString) {
            value = new ValueString(headerPage.getValue());
        } else {
            value = new ValueInt(Integer.parseInt(headerPage.getValue()));
        }
    }

    public BitMapFile(String filename, Columnarfile columnarFile, Integer columnNo, ValueClass value) throws Exception {
        headerPageId = get_file_entry(filename);
        if (headerPageId == null) //file not exist
        {
            headerPage = new BitMapHeaderPage();
            headerPageId = headerPage.getPageId();
            add_file_entry(filename, headerPageId);
            headerPage.set_rootId(new PageId(INVALID_PAGE));
            headerPage.setColumnarFileName(columnarFile.getColumnarFileName());
            headerPage.setColumnNumber(columnNo);
            if (value instanceof ValueInt) {
                headerPage.setValue(((ValueInt) value).getValue().toString());
                headerPage.setAttrType(new AttrType(AttrType.attrInteger));
            } else {
                headerPage.setValue(((ValueString) value).getValue());
                headerPage.setAttrType(new AttrType(AttrType.attrString));
            }
        } else {
            headerPage = new BitMapHeaderPage(headerPageId);
        }
    }
    
    public void close() throws Exception {
        if (headerPage != null) {
            SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
            headerPage = null;
        }
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

    private void add_file_entry(String fileName, PageId pageno)
            throws AddFileEntryException {
        try {
            SystemDefs.JavabaseDB.add_file_entry(fileName, pageno);
        } catch (Exception e) {
            e.printStackTrace();
            throw new AddFileEntryException(e, "");
        }
    }

}
