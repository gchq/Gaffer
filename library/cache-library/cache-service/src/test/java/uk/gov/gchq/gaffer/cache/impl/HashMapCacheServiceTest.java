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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.cache.ICache;

public class HashMapCacheServiceTest {

    private HashMapCacheService service = new HashMapCacheService();

    private static final String CACHE_NAME = "test";

    @Before
    public void before() {
        service.initialise();
    }
    @After
    public void after() {
        service.shutdown();
    }

    @Test
    public void shouldReturnInstanceOfHashMapCache() {

        // when
        ICache cache = service.getCache(CACHE_NAME);

        // then
        assert (cache instanceof  HashMapCache);
    }

    @Test
    public void shouldCreateNewHashMapCacheIfOneDoesNotExist() {

        // when
        ICache cache = service.getCache(CACHE_NAME);

        // then
        Assert.assertEquals(0, cache.size());
    }

    @Test
    public void shouldReUseCacheIfOneExists() {

        // given
        ICache<String, Integer> cache = service.getCache(CACHE_NAME);
        cache.put("key", 1);

        // when
        ICache<String, Integer> sameCache = service.getCache(CACHE_NAME);

        // then
        Assert.assertEquals(1, sameCache.size());
        Assert.assertEquals(new Integer(1), sameCache.get("key"));

    }
}
