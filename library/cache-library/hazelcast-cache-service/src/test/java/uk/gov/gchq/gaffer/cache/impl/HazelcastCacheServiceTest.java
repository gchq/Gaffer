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

import org.apache.commons.io.FileUtils;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.cache.ICache;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.cache.util.CacheProperties;
import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HazelcastCacheServiceTest {

    private static HazelcastCacheService service = new HazelcastCacheService();
    private Properties cacheProperties = new Properties();
    private static final String CACHE_NAME = "test";

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @Before
    public void beforeEach() {
        cacheProperties.clear();
    }

    @After
    public void afterEach() {
        try {
            service.clearCache(CACHE_NAME);
        } catch (final Exception e) {
            // ignore errors
        }
    }

    @AfterClass
    public static void afterClass() {
        service.shutdown();
    }

    @Test
    public void shouldThrowAnExceptionWhenConfigFileIsMisConfigured() {
        service.shutdown();

        final String madeUpFile = "/made/up/file.xml";
        cacheProperties.setProperty(CacheProperties.CACHE_CONFIG_FILE, madeUpFile);

        try {
            service.initialise(cacheProperties);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains(madeUpFile));
        }
    }

    private void initialiseWithTestConfig() {
        final File file;
        try {
            file = tempFolder.newFile("hazelcast.xml");
            FileUtils.copyInputStreamToFile(StreamUtil.openStream(getClass(), "hazelcast.xml"), file);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        cacheProperties.setProperty(CacheProperties.CACHE_CONFIG_FILE, file.getAbsolutePath());
        service.initialise(cacheProperties);
    }

    @Test
    public void shouldAllowUserToConfigureCacheUsingConfigFilePath() {

        // given
        initialiseWithTestConfig();

        // when
        ICache<String, Integer> cache = service.getCache(CACHE_NAME);

        // then
        Assert.assertEquals(0, cache.size());
    }

    @Test
    public void shouldReUseCacheIfOneExists() throws CacheOperationException {

        // given
        initialiseWithTestConfig();
        ICache<String, Integer> cache = service.getCache(CACHE_NAME);
        cache.put("key", 1);

        // when
        ICache<String, Integer> sameCache = service.getCache(CACHE_NAME);

        // then
        Assert.assertEquals(1, sameCache.size());
        Assert.assertEquals(new Integer(1), sameCache.get("key"));
    }

    @Test
    public void shouldShareCachesBetweenServices() throws CacheOperationException {

        // given
        initialiseWithTestConfig();
        HazelcastCacheService service1 = new HazelcastCacheService();
        service1.initialise(cacheProperties);

        // when
        service1.getCache(CACHE_NAME).put("Test", 2);

        // then
        assertEquals(1, service.getCache(CACHE_NAME).size());
        assertEquals(2, service.getCache(CACHE_NAME).get("Test"));
    }

    @Test
    public void shouldAddEntriesToCache() throws CacheOperationException {
        initialiseWithTestConfig();

        service.putInCache(CACHE_NAME, "test", 1);

        assertEquals((Integer) 1, service.getFromCache(CACHE_NAME, "test"));
    }

    @Test
    public void shouldOnlyUpdateIfInstructed() throws CacheOperationException {
        initialiseWithTestConfig();
        service.putInCache(CACHE_NAME, "test", 1);

        try {
            service.putSafeInCache(CACHE_NAME, "test", 2);
            fail("Expected an exception");
        } catch (final CacheOperationException e) {
            assertEquals((Integer) 1, service.getFromCache(CACHE_NAME, "test"));
        }

        service.putInCache(CACHE_NAME, "test", 2);

        assertEquals((Integer) 2, service.getFromCache(CACHE_NAME, "test"));
    }

    @Test
    public void shouldBeAbleToDeleteCacheEntries() throws CacheOperationException {
        initialiseWithTestConfig();
        service.putInCache(CACHE_NAME, "test", 1);

        service.removeFromCache(CACHE_NAME, "test");
        assertEquals(0, service.sizeOfCache(CACHE_NAME));
    }

    @Test
    public void shouldBeAbleToClearCache() throws CacheOperationException {
        initialiseWithTestConfig();
        service.putInCache(CACHE_NAME, "test1", 1);
        service.putInCache(CACHE_NAME, "test2", 2);
        service.putInCache(CACHE_NAME, "test3", 3);


        service.clearCache(CACHE_NAME);

        assertEquals(0, service.sizeOfCache(CACHE_NAME));
    }

    @Test
    public void shouldGetAllKeysFromCache() throws CacheOperationException {
        initialiseWithTestConfig();
        service.putInCache(CACHE_NAME, "test1", 1);
        service.putInCache(CACHE_NAME, "test2", 2);
        service.putInCache(CACHE_NAME, "test3", 3);

        assertEquals(3, service.sizeOfCache(CACHE_NAME));
        assertThat(service.getAllKeysFromCache(CACHE_NAME), IsCollectionContaining.hasItems("test1", "test2", "test3"));
    }

    @Test
    public void shouldGetAllValues() throws CacheOperationException {
        initialiseWithTestConfig();
        service.putInCache(CACHE_NAME, "test1", 1);
        service.putInCache(CACHE_NAME, "test2", 2);
        service.putInCache(CACHE_NAME, "test3", 3);
        service.putInCache(CACHE_NAME, "duplicate", 3);

        assertEquals(4, service.sizeOfCache(CACHE_NAME));
        assertEquals(4, service.getAllValuesFromCache(CACHE_NAME).size());

        assertThat(service.getAllValuesFromCache(CACHE_NAME), IsCollectionContaining.hasItems(1, 2, 3));
    }
}
