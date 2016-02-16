package net.spy.memcached.collection;

import java.util.Map;
import java.util.List;

public class MapGet<T> extends CollectionMapGet<T> {

    private static final String command = "mop get";

    protected String range;
    protected int count = -1;
    protected Map<Integer, T> map;

    public MapGet(List<T> fieldList, boolean delete) {
        this.headerCount = 1;
        this.count = fieldList.size();
        this.delete = delete;
        if (fieldList.size() > 0) {
            int i;
            this.range = String.valueOf(fieldList.get(0));
            for (i = 1; i < count; i++) {
                this.range += " " + String.valueOf(fieldList.get(i));
            }
        }
    }

    public MapGet(List<T> fieldList, boolean delete, boolean dropIfEmpty) {
        this(fieldList, delete);
        this.dropIfEmpty = dropIfEmpty;
    }

    public MapGet(int count, boolean delete) {
        this.headerCount = 1;
        this.count = count;
        this.delete = delete;
    }

    public MapGet(int count, boolean delete, boolean dropIfEmpty) {
        this(count, delete);
        this.dropIfEmpty = dropIfEmpty;
    }

    public String getRange() {
        return range;
    }

    public void setRange(String range) {
        this.range = range;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public Map<Integer, T> getMap() {
        return map;
    }

    public String stringify() {
        if (str != null) return str;

        StringBuilder b = new StringBuilder();
        if (count > 0) b.append(count).append(" ").append(range);
        else b.append(count);
        if (delete && dropIfEmpty) b.append(" drop");
        if (delete && !dropIfEmpty) b.append(" delete");

        str = b.toString();
        return str;
    }

    public String getCommand() {
        return command;
    }

    private int headerParseStep = 1;

    @Override
    public boolean headerReady(int spaceCount) {
        return spaceCount == 2 || spaceCount == 3;
    }

    public void decodeItemHeader(String itemHeader) {
        String[] splited = itemHeader.split(" ");

        if (headerParseStep == 1) {
                this.subkey = splited[0];
                this.dataLength = Integer.parseInt(splited[1]);
        } else {
            this.headerParseStep = 1;
            this.dataLength = Integer.parseInt(splited[1]);
        }
    }
}
