package net.spy.memcached.collection;

import java.util.List;
import java.util.Map;

public class MapDelete<T> extends CollectionDelete<T>{
    private static final String command = "mop delete";

    protected String range;
    protected int count = -1;
    protected Map<Integer, T> map;

    public MapDelete(List<T> fieldList, boolean noreply) {
        this.count = fieldList.size();
        if (fieldList.size() > 0) {
            int i;
            this.range = String.valueOf(fieldList.get(0));
            for (i = 1; i < count; i++) {
                this.range += " " + String.valueOf(fieldList.get(i));
            }
        }
        this.noreply = noreply;
    }

    public MapDelete(List<T> fieldList, boolean noreply, boolean dropIfEmpty) {
        this(fieldList, noreply);
        this.dropIfEmpty = dropIfEmpty;
    }

    public MapDelete(int count, boolean noreply) {
        this.count = count;
        this.noreply = noreply;
    }

    public MapDelete(int count, boolean noreply, boolean dropIfEmpty) {
        this(count, noreply);
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
        if (dropIfEmpty) b.append(" drop");
        if (noreply) b.append(" noreply");

        str = b.toString();
        return str;
    }

    public String getCommand() {
        return command;
    }
}
