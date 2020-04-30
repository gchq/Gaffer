/*
 * Copyright 2016-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.cache.impl;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.cache.ICache;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.commonutil.exception.OverwritingException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HashMapCacheServiceTest {

    private HashMapCacheService service = new HashMapCacheService();

    private static final String CACHE_NAME = "test";

    @BeforeEach
    public void before() {
        service.initialise(null);
    }

    @AfterEach
    public void after() {
        service.shutdown();
    }

    @Test
    public void shouldReturnInstanceOfHashMapCache() {
        final ICache cache = service.getCache(CACHE_NAME);

        assertTrue(cache instanceof HashMapCache);
    }

    @Test
    public void shouldCreateNewHashMapCacheIfOneDoesNotExist() {
        final ICache cache = service.getCache(CACHE_NAME);

        assertEquals(0, cache.size());
    }

    @Test
    public void shouldReUseCacheIfOneExists() throws CacheOperationException {
        final ICache<String, Integer> cache = service.getCache(CACHE_NAME);
        cache.put("key", 1);

        final ICache<String, Integer> sameCache = service.getCache(CACHE_NAME);

        assertEquals(1, sameCache.size());
        assertEquals(new Integer(1), sameCache.get("key"));
    }

    @Test
    public void shouldAddEntriesToCache() throws CacheOperationException {
        service.putInCache(CACHE_NAME, "test", 1);

        assertEquals((Integer) 1, service.getFromCache(CACHE_NAME, "test"));
    }

    @Test
    public void shouldOnlyUpdateIfInstructed() throws CacheOperationException {
        service.putInCache(CACHE_NAME, "test", 1);

        assertThrows(OverwritingException.class, () -> {
            service.putSafeInCache(CACHE_NAME, "test", 2);
        });
        assertEquals((Integer) 1, service.getFromCache(CACHE_NAME, "test"));

        service.putInCache(CACHE_NAME, "test", 2);

        assertEquals((Integer) 2, service.getFromCache(CACHE_NAME, "test"));
    }

    @Test
    public void shouldBeAbleToDeleteCacheEntries() throws CacheOperationException {
        service.putInCache(CACHE_NAME, "test", 1);

        service.removeFromCache(CACHE_NAME, "test");

        assertEquals(0, service.sizeOfCache(CACHE_NAME));
    }

    @Test
    public void shouldBeAbleToClearCache() throws CacheOperationException {
        populateCache();

        service.clearCache(CACHE_NAME);

        assertEquals(0, service.sizeOfCache(CACHE_NAME));
    }

    @Test
    public void shouldGetAllKeysFromCache() throws CacheOperationException {
        populateCache();

        assertEquals(3, service.sizeOfCache(CACHE_NAME));
        assertThat(service.getAllKeysFromCache(CACHE_NAME), hasItems("test1", "test2", "test3"));
    }

    @Test
    public void shouldGetAllValues() throws CacheOperationException {
        populateCache();
        service.putInCache(CACHE_NAME, "duplicate", 3);

        assertEquals(4, service.sizeOfCache(CACHE_NAME));
        assertEquals(4, service.getAllValuesFromCache(CACHE_NAME).size());
        assertThat(service.getAllValuesFromCache(CACHE_NAME), hasItems(1, 2, 3, 3));
    }

    private void populateCache() throws CacheOperationException {
        service.putInCache(CACHE_NAME, "test1", 1);
        service.putInCache(CACHE_NAME, "test2", 2);
        service.putInCache(CACHE_NAME, "test3", 3);
    }
}
