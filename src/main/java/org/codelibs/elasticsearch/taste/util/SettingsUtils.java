package org.codelibs.elasticsearch.taste.util;

import java.util.Map;

public final class SettingsUtils {
    private SettingsUtils() {
    }

    public static <T, V> T get(final Map<String, V> settings, final String key) {
        return get(settings, key, null);
    }

    @SuppressWarnings("unchecked")
    public static <T, V> T get(final Map<String, V> settings, final String key,
            final T defaultValue) {
        if (settings != null) {
            final V value = settings.get(key);
            if (value != null) {
                return (T) value;
            }
        }
        return defaultValue;
    }
}
