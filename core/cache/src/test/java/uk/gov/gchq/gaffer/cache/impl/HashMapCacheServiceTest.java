/*
 * Copyright 2017-2024 Crown Copyright
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class HashMapCacheServiceTest {

    private HashMapCacheService service = new HashMapCacheService();

    private static final String CACHE_NAME = "test";

    @BeforeEach
    void before() {
        service.initialise(null);
    }

    @AfterEach
    void after() {
        service.shutdown();
    }

    @Test
    void shouldReturnInstanceOfHashMapCache() {
        // When
        final ICache cache = service.getCache(CACHE_NAME);

        // Then
        assertThat(cache).isInstanceOf(HashMapCache.class);
    }

    @Test
    void shouldCreateNewHashMapCacheIfOneDoesNotExist() {
        // When
        final ICache cache = service.getCache(CACHE_NAME);

        // Then
        assertThat(cache.size()).isZero();
    }

    @Test
    void shouldReUseCacheIfOneExists() throws CacheOperationException {
        // Given
        final ICache<String, Integer> cache = service.getCache(CACHE_NAME);
        cache.put("key", 1);

        // When
        final ICache<String, Integer> sameCache = service.getCache(CACHE_NAME);

        // Then
        assertThat(sameCache.size()).isOne();
        assertThat(sameCache.get("key")).isOne();
    }

    @Test
    void shouldAddEntriesToCache() throws CacheOperationException {
       // When
        service.putInCache(CACHE_NAME, "test", 1);

        // Then
        service.getFromCache(CACHE_NAME, "test");
        assertThat((Object) service.getFromCache(CACHE_NAME, "test")).isEqualTo(1);
    }

    @Test
    void shouldOnlyUpdateIfInstructed() throws CacheOperationException {
        // When
        service.putInCache(CACHE_NAME, "test", 1);

        // Then
        assertThatExceptionOfType(OverwritingException.class)
            .isThrownBy(() -> service.putSafeInCache(CACHE_NAME, "test", 2))
            .withMessageContaining("Cache entry already exists for key: test");

        assertThat((Object) service.getFromCache(CACHE_NAME, "test")).isEqualTo(1);

        // When
        service.putInCache(CACHE_NAME, "test", 2);

        // Then
        assertThat((Object) service.getFromCache(CACHE_NAME, "test")).isEqualTo(2);
    }

    @Test
    void shouldBeAbleToDeleteCacheEntries() throws CacheOperationException {
        // Given
        service.putInCache(CACHE_NAME, "test", 1);

        // When
        service.removeFromCache(CACHE_NAME, "test");

        // Then
        assertThat(service.sizeOfCache(CACHE_NAME)).isZero();
    }

    @Test
    void shouldBeAbleToClearCache() throws CacheOperationException {
        // Given
        populateCache();

        // When
        service.clearCache(CACHE_NAME);

        // Then
        assertThat(service.sizeOfCache(CACHE_NAME)).isZero();
    }

    @Test
    void shouldGetAllKeysFromCache() throws CacheOperationException {
        // When
        populateCache();

        // Then
        assertThat(service.sizeOfCache(CACHE_NAME)).isEqualTo(3);
        assertThat(service.getAllKeysFromCache(CACHE_NAME)).contains("test1", "test2", "test3");
    }

    @Test
    void shouldGetAllValues() throws CacheOperationException {
        // Given
        populateCache();

        // When
        service.putInCache(CACHE_NAME, "duplicate", 3);

        // Then
        assertThat(service.sizeOfCache(CACHE_NAME)).isEqualTo(4);
        assertThat(service.getAllValuesFromCache(CACHE_NAME))
                .hasSize(4)
                .contains(1, 2, 3, 3);
    }

    private void populateCache() throws CacheOperationException {
        service.putInCache(CACHE_NAME, "test1", 1);
        service.putInCache(CACHE_NAME, "test2", 2);
        service.putInCache(CACHE_NAME, "test3", 3);
    }
}
