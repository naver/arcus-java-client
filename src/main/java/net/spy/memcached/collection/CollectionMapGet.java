package net.spy.memcached.collection;

public abstract class CollectionMapGet<T> {

    protected boolean delete = false;
    protected boolean dropIfEmpty = true;

    protected String str;
    protected int headerCount;
    protected String subkey;
    protected int dataLength;

    protected byte[] elementFlag;

    public boolean isDelete() {
        return delete;
    }

    public void setDelete(boolean delete) {
        this.delete = delete;
    }

    public String getSubkey() {
        return subkey;
    }

    public int getDataLength() { return dataLength; }

    public byte[] getElementFlag() {
        return elementFlag;
    }

    public boolean headerReady(int spaceCount) {
        return headerCount == spaceCount;
    }

    public void setHeaderCount(int headerCount) {
        this.headerCount = headerCount;
    }

    public int getHeaderCount() {
        return headerCount;
    }

    public boolean eachRecordParseCompleted() {
        return true;
    }

    public abstract String stringify();
    public abstract String getCommand();
    public abstract void decodeItemHeader(String itemHeader);

}
