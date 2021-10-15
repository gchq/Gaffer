/*
 * Copyright 2017-2021 Crown Copyright
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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.exception.SerialisationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class HashMapCacheTest {

    private HashMapCache<String, Integer> cache = new HashMapCache<>();

    @AfterEach
    public void after() {
        cache.clear();
    }

    @Test
    public void shouldAddKeyValuePairToCache() {
        cache.put("key", 1);
        assertThat(cache.size()).isOne();
    }

    @Test
    public void shouldGetEntryFromCacheUsingKey() {
        cache.put("key", 2);

        assertEquals(new Integer(2), cache.get("key"));
    }

    @Test
    public void shouldDeleteCachedEntriesByKeyName() {
        cache.put("key", 3);

        cache.remove("key");

        assertThat(cache.size()).isZero();
    }

    @Test
    public void putShouldOverriteEntriesWithDuplicateKeyName() {
        cache.put("key", 4);

        cache.put("key", 5);

        assertThat(cache.size()).isOne();
        assertEquals(new Integer(5), cache.get("key"));
    }

    @Test
    public void shouldClearAllEntries() {
        cache.put("key1", 1);
        cache.put("key2", 2);
        cache.put("key3", 3);

        cache.clear();

        assertThat(cache.size()).isZero();
    }

    @Test
    public void shouldGetAllKeys() {
        cache.put("test1", 1);
        cache.put("test2", 2);
        cache.put("test3", 3);

        assertThat(cache.size()).isEqualTo(3);
        assertThat(cache.getAllKeys()).contains("test1", "test2", "test3");
    }

    @Test
    public void shouldGetAllValues() {
        cache.put("test1", 1);
        cache.put("test2", 2);
        cache.put("test3", 3);
        cache.put("duplicate", 3);


        assertThat(cache.size()).isEqualTo(4);
        assertThat(cache.getAllValues())
                .hasSize(4)
                .contains(1, 2, 3, 3);
    }

    @DisplayName("Should cause JavaSerialisableException when serialisation flag is true")
    @Test
    public void shouldThrowRuntimeExceptionCausedByNonJavaSerialisableException() {
        final HashMapCache<String, Object> map = new HashMapCache<>(true);
        final String s = "hello";
        map.put("test1", s);

        class TempClass {
        }

        TempClass tempClass = new TempClass();

        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(() -> map.put("test1", tempClass))
                .withCauseInstanceOf(SerialisationException.class);
    }

    @DisplayName("Should not cause JavaSerialisableException when serialisation flag is false")
    @Test
    public void shouldNotThrowAnyExceptions() {
        final HashMapCache<String, Object> map = new HashMapCache<>(false);

        map.put("test1", "hello");

        class TempClass {
        }

        final TempClass tempClass = new TempClass();
        map.put("test1", tempClass);
    }
}
