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

import org.apache.commons.jcs.access.exception.CacheException;
import org.apache.commons.jcs.engine.control.CompositeCacheManager;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.commonutil.exception.OverwritingException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class JcsCacheTest {

    private static JcsCache<String, Integer> cache;

    @BeforeAll
    static void setUp() throws CacheException {
        CompositeCacheManager manager = CompositeCacheManager.getInstance();
        cache = new JcsCache<>(manager.getCache("test"));
    }

    @BeforeEach
    void before() throws CacheOperationException {
        cache.clear();
    }


    @Test
    void shouldThrowAnExceptionIfEntryAlreadyExistsWhenUsingPutSafe() throws CacheOperationException {
        // given
        cache.put("test", 1);

        // then
        assertThatExceptionOfType(OverwritingException.class)
                .isThrownBy(() -> cache.putSafe("test", 1))
                .withMessage("Cache entry already exists for key: test");
    }

    @Test
    void shouldThrowExceptionWhenAddingNullKeyToCache() {
        assertThatExceptionOfType(CacheOperationException.class)
                .isThrownBy(() -> cache.put(null, 2))
                .extracting("message")
                .isNotNull();
    }

    @Test
    void shouldThrowExceptionIfAddingNullValue() {
        assertThatExceptionOfType(CacheOperationException.class)
                .isThrownBy(() -> cache.put("test", null))
                .extracting("message")
                .isNotNull();
    }

    @Test
    void shouldAddToCache() throws CacheOperationException {

        // when
        cache.put("key", 1);

        // then
        assertThat(cache.size()).isOne();
    }

    @Test
    void shouldReadFromCache() throws CacheOperationException {

        // when
        cache.put("key", 2);

        // then
        assertThat(cache.get("key")).isEqualTo(2);
    }

    @Test
    void shouldDeleteCachedEntries() throws CacheOperationException {

        // given
        cache.put("key", 3);

        // when
        cache.remove("key");

        // then
        assertThat(cache.size()).isZero();
    }

    @Test
    void shouldUpdateCachedEntries() throws CacheOperationException {

        // given
        cache.put("key", 4);

        // when
        cache.put("key", 5);

        // then
        assertThat(cache.size()).isOne();
        assertThat(cache.get("key")).isEqualTo(5);
    }

    @Test
    void shouldRemoveAllEntries() throws CacheOperationException {

        // given
        cache.put("key1", 1);
        cache.put("key2", 2);
        cache.put("key3", 3);

        // when
        cache.clear();

        // then
        assertThat(cache.size()).isZero();
    }

    @Test
    void shouldGetAllKeys() throws CacheOperationException {
        cache.put("test1", 1);
        cache.put("test2", 2);
        cache.put("test3", 3);

        assertThat(cache.size()).isEqualTo(3);
        assertThat(cache.getAllKeys()).contains("test1", "test2", "test3");
    }

    @Test
    void shouldGetAllValues() throws CacheOperationException {
        cache.put("test1", 1);
        cache.put("test2", 2);
        cache.put("test3", 3);
        cache.put("duplicate", 3);

        assertThat(cache.size()).isEqualTo(4);
        assertThat(cache.getAllValues()).hasSize(4);

        assertThat(cache.getAllValues()).contains(1, 2, 3);
    }
}
