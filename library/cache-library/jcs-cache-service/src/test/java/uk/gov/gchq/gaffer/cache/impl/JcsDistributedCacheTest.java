/*
 * Copyright 2017-2019 Crown Copyright
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

import org.apache.commons.jcs.engine.control.CompositeCache;
import org.apache.commons.jcs.engine.control.CompositeCacheManager;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class JcsDistributedCacheTest {

    private static JcsCache<String, Integer> cache1;
    private static JcsCache<String, Integer> cache2;

    @BeforeClass
    public static void setUp() throws IOException {
        Properties cacheProperties = new Properties();
        cacheProperties.load(new FileInputStream("src/test/resources/distributed.ccf"));

        // create one cache
        CompositeCacheManager mgr = CompositeCacheManager.getUnconfiguredInstance();
        mgr.configure(cacheProperties);
        CompositeCache<String, Integer> tmp = new CompositeCache<>(mgr.getDefaultCacheAttributes(), mgr.getDefaultElementAttributes());
        cache1 = new JcsCache<>(tmp);

        // swap ports to create second cache forming a 2 node cluster
        cacheProperties.setProperty("jcs.auxiliary.LTCP.attributes.TcpServers", "localhost:1110");
        cacheProperties.setProperty("jcs.auxiliary.LTCP.attributes.TcpListenerPort", "1111");

        mgr = CompositeCacheManager.getUnconfiguredInstance();
        mgr.configure(cacheProperties);
        tmp = new CompositeCache<>(mgr.getDefaultCacheAttributes(), mgr.getDefaultElementAttributes());
        cache2 = new JcsCache<>(tmp);
    }

    @Before
    public void before() throws CacheOperationException {
        cache1.clear();
    }

    @Test
    public void shouldSendUpdatesToOtherNodes() throws CacheOperationException {
        // given cache is distributed

        // when
        cache1.put("test", 1);

        // then
        assertEquals(new Integer(1), cache2.get("test"));

    }

    @Test
    public void shouldBeAbleToRetrieveAllKeys() throws CacheOperationException {
        // given cache is distributed

        // when
        cache1.put("test1", 1);
        cache1.put("test2", 2);
        cache2.put("test3", 3);

        // then
        Set<String> keys = cache2.getAllKeys();
        assertEquals(3, keys.size());
        assert (keys.contains("test1"));
        assert (keys.contains("test2"));
        assert (keys.contains("test3"));
    }

    @Test
    public void shouldBeAbleToGetAllValues() throws CacheOperationException {
        // given cache is distributed

        // when
        cache1.put("test4", 4);
        cache1.put("test5", 5);
        cache2.put("test6", 6);

        // then
        Collection<Integer> keys = cache2.getAllValues();
        assertEquals(3, keys.size());
        assert (keys.contains(4));
        assert (keys.contains(5));
        assert (keys.contains(6));
    }

    @Test
    public void shouldOverwriteEntriesRemotely() throws CacheOperationException {
        // given cache is distributed

        // when
        cache1.put("test7", 7);
        cache1.put("test7", 8);

        // then
        assertEquals(new Integer(8), cache2.get("test7"));
        assertEquals(new Integer(8), cache1.get("test7"));

        // when
        cache2.put("test7", 7);

        // then
        assertEquals(new Integer(7), cache2.get("test7"));
        assertEquals(new Integer(7), cache1.get("test7"));
    }
}


