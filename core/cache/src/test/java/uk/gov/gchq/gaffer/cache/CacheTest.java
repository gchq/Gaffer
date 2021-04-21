/*
 * Copyright 2018-2020 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.gov.gchq.gaffer.cache;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.cache.util.CacheProperties;
import uk.gov.gchq.gaffer.commonutil.exception.OverwritingException;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CacheTest {

    private static final String CACHE_SERVICE_CLASS_STRING = "uk.gov.gchq.gaffer.cache.impl.HashMapCacheService";
    private static Cache<Integer> cache;
    private static Properties properties = new Properties();

    @BeforeAll
    public static void setUp() {
        properties.setProperty(CacheProperties.CACHE_SERVICE_CLASS, CACHE_SERVICE_CLASS_STRING);
        CacheServiceLoader.initialise(properties);
        cache = new Cache<>("serviceName1");
    }

    @BeforeEach
    public void beforeEach() throws CacheOperationException {
        cache.clearCache();
    }

    @Test
    public void shouldAddAndGetValueFromCache() throws CacheOperationException {
        cache.addToCache("key1", 1, true);

        assertEquals((Integer) 1, cache.getFromCache("key1"));
        assertNull(cache.getFromCache("key2"));
    }

    @Test
    public void shouldAddAndGetCacheOverwrite() throws CacheOperationException {
        cache.addToCache("key1", 1, true);
        cache.addToCache("key1", 2, true);

        assertEquals(2, cache.getFromCache("key1").intValue());
    }

    @Test
    public void shouldAddAndGetCacheNoOverwrite() throws CacheOperationException {
        cache.addToCache("key1", 1, true);

        final Exception exception = assertThrows(OverwritingException.class, () -> {
            cache.addToCache("key1", 2, false);
        });

        assertEquals("Cache entry already exists for key: key1", exception.getMessage());
        assertEquals(1, cache.getFromCache("key1").intValue());
    }

    @Test
    public void shouldGetCacheServiceName() {
        assertEquals("serviceName1", cache.getCacheName());
    }

    @Test
    public void shouldDeleteKeyValuePair() throws CacheOperationException {
        cache.addToCache("key1", 1, false);
        cache.deleteFromCache("key1");

        assertNull(cache.getFromCache("key1"));
    }
}
