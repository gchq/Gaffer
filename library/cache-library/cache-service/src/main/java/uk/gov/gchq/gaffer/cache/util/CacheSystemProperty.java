package uk.gov.gchq.gaffer.cache.util;


public final class CacheSystemProperty {

    private CacheSystemProperty() {
        // do not instantiate
    }

    // Keys

    public static final String CACHE_SERVICE_CLASS = "gaffer.cache.service.class";

    // Defaults

    public static final String DEFAULT_CACHE_SERVICE_CLASS = "uk.gov.gchq.gaffer.cache.impl.HashMapCacheService";
}
