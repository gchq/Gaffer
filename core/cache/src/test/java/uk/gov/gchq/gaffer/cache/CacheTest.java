/*
 * Copyright 2018-2024 Crown Copyright
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.commonutil.exception.OverwritingException;

import java.util.Collections;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class CacheTest {
    private static final String CACHE_SERVICE_CLASS_STRING = "uk.gov.gchq.gaffer.cache.impl.HashMapCacheService";
    private static Cache<String, Integer> cache;

    @BeforeEach
    void beforeEach() {
        CacheServiceLoader.shutdown();
        CacheServiceLoader.initialise(CACHE_SERVICE_CLASS_STRING);
        cache = new Cache<>("cacheName1");
    }

    @Test
    void shouldAddAndGetValueFromCache() throws CacheOperationException {
        cache.addToCache("key1", 1, true);

        assertThat(cache.getFromCache("key1")).isEqualTo(1);
        assertThat(cache.getFromCache("key2")).isNull();
    }

    @Test
    void shouldAddAndGetCacheOverwrite() throws CacheOperationException {
        cache.addToCache("key1", 1, true);
        cache.addToCache("key1", 2, true);

        assertThat(cache.getFromCache("key1")).isEqualTo(2);
    }

    @Test
    void shouldAddAndGetCacheNoOverwrite() throws CacheOperationException {
        cache.addToCache("key1", 1, true);

        assertThatExceptionOfType(OverwritingException.class)
                .isThrownBy(() -> cache.addToCache("key1", 2, false))
                .withMessage("Cache entry already exists for key: key1");
        assertThat(cache.getFromCache("key1")).isEqualTo(1);
    }

    @Test
    void shouldGetCacheName() {
        assertThat(cache.getCacheName()).isEqualTo("cacheName1");
    }

    @Test
    void shouldDeleteKeyValuePair() throws CacheOperationException {
        cache.addToCache("key1", 1, false);
        cache.deleteFromCache("key1");

        assertThat(cache.getFromCache("key1")).isNull();
    }

    @Test
    void shouldGetAllKeys() throws CacheOperationException {
        assertThat(cache.getAllKeys()).isEqualTo(Collections.emptySet());
        cache.addToCache("key1", 1, false);
        assertThat(cache.getAllKeys()).isEqualTo(Collections.singleton("key1"));
    }

    @Test
    void shouldThrowExceptionTryingToGetAllKeysWhenNoServiceAvailable() {
        CacheServiceLoader.shutdown();
        assertThatExceptionOfType(uk.gov.gchq.gaffer.core.exception.GafferRuntimeException.class)
                .isThrownBy(() -> cache.getAllKeys())
                .withMessage("Error getting all keys")
                .withStackTraceContaining("is not enabled, check it was initialised");
    }

    @Test
    void shouldConstructCacheWithCacheAndServiceName() {
        final String serviceName = "myService";
        final String cacheName = "myCache";
        // Using the default service
        Cache<String, Integer> tmpCache = new Cache<>(cacheName, serviceName);
        assertThat(tmpCache.getCacheName()).isEqualTo(cacheName);

        // Using new service with name "myService"
        CacheServiceLoader.shutdown();
        CacheServiceLoader.initialise(serviceName, CACHE_SERVICE_CLASS_STRING, new Properties());
        Cache<String, Integer> tmpCache2 = new Cache<>(cacheName, serviceName);
        assertThat(tmpCache2.getCacheName()).isEqualTo(cacheName);
    }

    @Test
    void shouldGetICache() {
        assertThat(cache.getCache().getClass().getName()).isEqualTo(CACHE_SERVICE_CLASS_STRING.replaceAll("Service", ""));
        CacheServiceLoader.shutdown();
        assertThat(cache.getCache()).isNull();
    }
}

