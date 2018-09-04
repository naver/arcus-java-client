package net.spy.memcached.collection;

public class RangeGet extends CollectionGet {
    private static final String command = "rget";
    private String frkey;
    private String tokey;

    public RangeGet(String frkey, String tokey) {
        this.frkey = frkey;
        this.tokey = tokey;
        this.range = frkey + " " + tokey;
    }

    public RangeGet(String frkey, String tokey, int count) {
        this(frkey, tokey);
        this.range = this.range + " " + String.valueOf(count);
    }

    @Override
    public byte[] getAddtionalArgs() { return null; }
    public String stringify() {
        if (str != null) return str;
        StringBuilder b = new StringBuilder();
        b.append(range);
        str = b.toString();
        return str;
    }
    public String getFrkey() { return frkey; }
    public String getRange() {
        return this.range;
    }
    public void decodeItemHeader(String itemHeader) { this.dataLength = Integer.parseInt(itemHeader); }
    public String getCommand() { return command; }

}
