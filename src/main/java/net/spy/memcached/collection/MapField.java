package net.spy.memcached.collection;


/**
 * Collection element
 *
 * @param <T>
 */
public class MapField<T> {
	private final String field;
	private final T value;

	/**
	 * Create an element
	 *
	 * @param field field of mapfield
	 * @param value value of mapfield
	 */
	public MapField(String field, T value) {
		this.field = field;
		this.value = value;
	}

	/**
	 * get field in map op.
	 *
	 * @return field
	 */
	public String getField() {
		return field;
	}

	/**
	 * get value
	 *
	 * @return value
	 */
	public T getValue() {
		return value;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("{ \"");
		sb.append(getField());

		sb.append("\" : { ");

		sb.append(" \"value\" : \"").append(value.toString()).append("\"");
		sb.append(" }");

		return sb.toString();
	}

}
