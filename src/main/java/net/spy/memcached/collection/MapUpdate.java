package net.spy.memcached.collection;

public class MapUpdate<T> extends CollectionUpdate<T> {

	private static final String command = "mop update";

	public MapUpdate(T newValue, boolean noreply) {
		super(newValue, noreply);
	}

	public String getCommand() {
		return command;
	}

}
