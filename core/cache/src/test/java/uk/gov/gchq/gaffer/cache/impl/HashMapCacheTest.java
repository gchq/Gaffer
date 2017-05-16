/*
 * Copyright 2016-2017 Crown Copyright
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


import org.hamcrest.core.IsCollectionContaining;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class HashMapCacheTest {

    private HashMapCache<String, Integer> cache = new HashMapCache<>();

    @After
    public void after() {
        cache.clear();
    }

    @Test
    public void shouldAddToCache() {

        // when
        cache.put("key", 1);

        // then
        Assert.assertEquals(1, cache.size());
    }

    @Test
    public void shouldReadFromCache() {

        // when
        cache.put("key", 2);

        // then
        Assert.assertEquals(new Integer(2), cache.get("key"));
    }

    @Test
    public void shouldDeleteCachedEntries() {

        // given
        cache.put("key", 3);

        // when
        cache.remove("key");

        // then
        Assert.assertEquals(0, cache.size());
    }

    @Test
    public void shouldUpdateCachedEntries() {

        // given
        cache.put("key", 4);

        // when
        cache.put("key", 5);

        // then
        Assert.assertEquals(1, cache.size());
        Assert.assertEquals(new Integer(5), cache.get("key"));
    }

    @Test
    public void shouldRemoveAllEntries() {

        // given
        cache.put("key1", 1);
        cache.put("key2", 2);
        cache.put("key3", 3);

        // when
        cache.clear();

        // then
        Assert.assertEquals(0, cache.size());
    }

    @Test
    public void shouldGetAllKeys() {
        cache.put("test1", 1);
        cache.put("test2", 2);
        cache.put("test3", 3);

        assertEquals(3, cache.size());
        Assert.assertThat(cache.getAllKeys(), IsCollectionContaining.hasItems("test1", "test2", "test3"));
    }

    @Test
    public void shouldGetAllValues() {
        cache.put("test1", 1);
        cache.put("test2", 2);
        cache.put("test3", 3);
        cache.put("duplicate", 3);

        assertEquals(4, cache.size());
        assertEquals(4, cache.getAllValues().size());

        Assert.assertThat(cache.getAllValues(), IsCollectionContaining.hasItems(1, 2, 3));
    }
}
