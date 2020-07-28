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

import org.apache.commons.io.FileUtils;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import uk.gov.gchq.gaffer.cache.ICache;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.cache.util.CacheProperties;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.exception.OverwritingException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class HazelcastCacheServiceTest {

    private static HazelcastCacheService service = new HazelcastCacheService();
    private Properties cacheProperties = new Properties();
    private static final String CACHE_NAME = "test";

    @BeforeEach
    public void beforeEach() {
        cacheProperties.clear();
    }

    @AfterEach
    public void afterEach() {
        try {
            service.clearCache(CACHE_NAME);
        } catch (final Exception e) {
            // ignore errors
        }
    }

    @AfterAll
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

    private void initialiseWithTestConfig(final Path tempDir) {
        final Path file;
        try {
            file = Files.createFile(tempDir.resolve("hazelcast.xml"));
            FileUtils.copyInputStreamToFile(StreamUtil.openStream(getClass(), "hazelcast.xml"), file.toFile());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        cacheProperties.setProperty(CacheProperties.CACHE_CONFIG_FILE, file.toAbsolutePath().toString());
        service.initialise(cacheProperties);
    }

    @Test
    public void shouldAllowUserToConfigureCacheUsingConfigFilePath(@TempDir Path tempDir) {

        // given
        initialiseWithTestConfig(tempDir);

        // when
        ICache<String, Integer> cache = service.getCache(CACHE_NAME);

        // then
        assertEquals(0, cache.size());
    }

    @Test
    public void shouldReUseCacheIfOneExists(@TempDir Path tempDir)
            throws CacheOperationException {

        // given
        initialiseWithTestConfig(tempDir);
        ICache<String, Integer> cache = service.getCache(CACHE_NAME);
        cache.put("key", 1);

        // when
        ICache<String, Integer> sameCache = service.getCache(CACHE_NAME);

        // then
        assertEquals(1, sameCache.size());
        assertEquals(new Integer(1), sameCache.get("key"));
    }

    @Test
    public void shouldShareCachesBetweenServices(@TempDir Path tempDir)
            throws CacheOperationException {

        // given
        initialiseWithTestConfig(tempDir);
        HazelcastCacheService service1 = new HazelcastCacheService();
        service1.initialise(cacheProperties);

        // when
        service1.getCache(CACHE_NAME).put("Test", 2);

        // then
        assumeTrue(1 == service.getCache(CACHE_NAME).size(),
                "No caches found - probably due to error 'Network is unreachable'");
        assertEquals(2, service.getCache(CACHE_NAME).get("Test"));
    }

    @Test
    public void shouldAddEntriesToCache(@TempDir Path tempDir)
            throws CacheOperationException {
        initialiseWithTestConfig(tempDir);

        service.putInCache(CACHE_NAME, "test", 1);

        assertEquals((Integer) 1, service.getFromCache(CACHE_NAME, "test"));
    }

    @Test
    public void shouldOnlyUpdateIfInstructed(@TempDir Path tempDir)
            throws CacheOperationException {
        initialiseWithTestConfig(tempDir);
        service.putInCache(CACHE_NAME, "test", 1);

        try {
            service.putSafeInCache(CACHE_NAME, "test", 2);
            fail("Expected an exception");
        } catch (final OverwritingException e) {
            assertEquals((Integer) 1, service.getFromCache(CACHE_NAME, "test"));
        }

        service.putInCache(CACHE_NAME, "test", 2);

        assertEquals((Integer) 2, service.getFromCache(CACHE_NAME, "test"));
    }

    @Test
    public void shouldBeAbleToDeleteCacheEntries(@TempDir Path tempDir)
            throws CacheOperationException {
        initialiseWithTestConfig(tempDir);
        service.putInCache(CACHE_NAME, "test", 1);

        service.removeFromCache(CACHE_NAME, "test");
        assertEquals(0, service.sizeOfCache(CACHE_NAME));
    }

    @Test
    public void shouldBeAbleToClearCache(@TempDir Path tempDir)
            throws CacheOperationException {
        initialiseWithTestConfig(tempDir);
        service.putInCache(CACHE_NAME, "test1", 1);
        service.putInCache(CACHE_NAME, "test2", 2);
        service.putInCache(CACHE_NAME, "test3", 3);


        service.clearCache(CACHE_NAME);

        assertEquals(0, service.sizeOfCache(CACHE_NAME));
    }

    @Test
    public void shouldGetAllKeysFromCache(@TempDir Path tempDir)
            throws CacheOperationException {
        initialiseWithTestConfig(tempDir);
        service.putInCache(CACHE_NAME, "test1", 1);
        service.putInCache(CACHE_NAME, "test2", 2);
        service.putInCache(CACHE_NAME, "test3", 3);

        assertEquals(3, service.sizeOfCache(CACHE_NAME));
        assertThat(service.getAllKeysFromCache(CACHE_NAME), IsCollectionContaining.hasItems("test1", "test2", "test3"));
    }

    @Test
    public void shouldGetAllValues(@TempDir Path tempDir)
            throws CacheOperationException {
        initialiseWithTestConfig(tempDir);
        service.putInCache(CACHE_NAME, "test1", 1);
        service.putInCache(CACHE_NAME, "test2", 2);
        service.putInCache(CACHE_NAME, "test3", 3);
        service.putInCache(CACHE_NAME, "duplicate", 3);

        assertEquals(4, service.sizeOfCache(CACHE_NAME));
        assertEquals(4, service.getAllValuesFromCache(CACHE_NAME).size());

        assertThat(service.getAllValuesFromCache(CACHE_NAME), IsCollectionContaining.hasItems(1, 2, 3));
    }
}
