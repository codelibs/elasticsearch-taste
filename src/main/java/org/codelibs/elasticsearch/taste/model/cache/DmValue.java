package org.codelibs.elasticsearch.taste.model.cache;

public class DmValue {
    private final Object value;

    private final int size;

    public DmValue(final Object value, final int size) {
        this.value = value;
        this.size = size;
    }

    @SuppressWarnings("unchecked")
    public <T> T getValue() {
        return (T) value;
    }

    public int getSize() {
        return size;
    }
}
