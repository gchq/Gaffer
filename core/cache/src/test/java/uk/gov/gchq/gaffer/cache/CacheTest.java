/*
 * Copyright 2018-2019 Crown Copyright
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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.cache.util.CacheProperties;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class CacheTest {
    private static final String CACHE_SERVICE_CLASS_STRING = "uk.gov.gchq.gaffer.cache.impl.HashMapCacheService";
    private static Cache<Integer> cache;
    private static Properties properties = new Properties();

    @BeforeClass
    public static void setUp() {
        properties.setProperty(CacheProperties.CACHE_SERVICE_CLASS, CACHE_SERVICE_CLASS_STRING);
        CacheServiceLoader.initialise(properties);
        cache = new Cache<>("serviceName1");
    }

    @Before
    public void beforeEach() throws CacheOperationException {
        cache.clearCache();
    }

    @Test
    public void shouldAddAndGetCache() throws CacheOperationException {
        Integer expected = 1;
        cache.addToCache("key1", expected, true);
        Integer actual = cache.getFromCache("key1");
        Integer actual2 = cache.getFromCache("key2");

        assertEquals(expected, actual);
        assertNotEquals(expected, actual2);
        assertNull(actual2);
    }

    @Test
    public void shouldAddAndGetCacheOverwrite() throws CacheOperationException {
        Integer expected = 1;
        Integer before = 2;
        cache.addToCache("key1", before, true);
        cache.addToCache("key1", expected, true);
        Integer actual = cache.getFromCache("key1");

        assertEquals(expected, actual);
    }

    @Test
    public void shouldAddAndGetCacheNoOverwrite() throws CacheOperationException {
        Integer expected = 1;
        Integer before = 2;
        cache.addToCache("key1", before, true);
        try {
            cache.addToCache("key1", expected, false);
            fail("exception expected");
        } catch (Exception e) {
            assertEquals("Cache entry already exists for key: key1", e.getMessage());
        }
        Integer actual = cache.getFromCache("key1");

        assertEquals(before, actual);
    }

    @Test
    public void shouldGetCacheServiceName() throws CacheOperationException {
        assertEquals("serviceName1", cache.getCacheName());
    }

    @Test
    public void shouldDelete() throws CacheOperationException {
        cache.addToCache("key1", 1, false);
        cache.deleteFromCache("key1");
        Integer actual = cache.getFromCache("key1");

        assertEquals(null, actual);
    }
}
