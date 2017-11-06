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

import com.google.common.util.concurrent.Uninterruptibles;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import uk.gov.gchq.gaffer.cache.ICache;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.cache.util.CacheProperties;
import uk.gov.gchq.gaffer.commonutil.exception.OverwritingException;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class JcsCacheServiceTest {

    private JcsCacheService service = new JcsCacheService();
    private static final String TEST_REGION = "test";
    private static final String ALTERNATIVE_TEST_REGION = "alternativeTest";
    private static final String AGE_OFF_REGION = "ageOff";
    private Properties serviceProps = new Properties();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void before() {
        serviceProps.clear();
    }

    @After
    public void after() throws CacheOperationException {
        service.clearCache(TEST_REGION);
        service.clearCache(ALTERNATIVE_TEST_REGION);
        service.clearCache(AGE_OFF_REGION);
    }

    @Test
    public void shouldUseDefaultConfigFileIfNoneIsSpecified() throws CacheOperationException {
        service.initialise(serviceProps);
        ICache<String, Integer> cache = service.getCache(TEST_REGION);
        cache.put("test", 1);
        cache.clear();
        // no exception thrown
    }

    @Test
    public void shouldThrowAnExceptionIfPathIsMisconfigured() {
        String badFileName = "/made/up/file/name";

        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(badFileName);

        serviceProps.setProperty(CacheProperties.CACHE_CONFIG_FILE, badFileName);

        service.initialise(serviceProps);
    }

    @Test
    public void shouldUsePropertyToConfigureJCS() throws CacheOperationException {
        // given
        String filePath = new File("src/test/resources/cache.ccf").getAbsolutePath();
        serviceProps.setProperty(CacheProperties.CACHE_CONFIG_FILE, filePath);
        service.initialise(serviceProps);
        // when
        ICache<String, Integer> cache = service.getCache(ALTERNATIVE_TEST_REGION);
        cache.put("test", 1);
        cache.clear();

        // then no exception
    }

    @Test
    public void shouldReUseCacheIfOneExists() throws CacheOperationException {

        // given
        service.initialise(serviceProps);
        ICache<String, Integer> cache = service.getCache(TEST_REGION);
        cache.put("key", 1);

        // when
        ICache<String, Integer> sameCache = service.getCache(TEST_REGION);

        // then
        assertEquals(1, sameCache.size());
        assertEquals(new Integer(1), sameCache.get("key"));

        cache.clear();

    }

    @Test
    public void shouldShareCachesBetweenServices() throws CacheOperationException {

        // given
        service.initialise(serviceProps);
        JcsCacheService service1 = new JcsCacheService();
        service1.initialise(serviceProps);

        // when
        ICache<String, Integer> cache = service1.getCache(TEST_REGION);
        cache.put("Test", 2);

        // then
        assertEquals(1, service.getCache(TEST_REGION).size());
        assertEquals(2, service.getCache(TEST_REGION).get("Test"));

        cache.clear();

    }

    @Test
    public void shouldAddEntriesToCache() throws CacheOperationException {
        service.initialise(serviceProps);
        service.putInCache(TEST_REGION, "test", 1);

        Assert.assertEquals((Integer) 1, service.getFromCache(TEST_REGION, "test"));
    }

    @Test
    public void shouldOnlyUpdateIfInstructed() throws CacheOperationException {
        service.initialise(serviceProps);
        service.putInCache(TEST_REGION, "test", 1);

        try {
            service.putSafeInCache(TEST_REGION, "test", 2);
            fail("Expected an exception");
        } catch (final OverwritingException e) {
            Assert.assertEquals((Integer) 1, service.getFromCache(TEST_REGION, "test"));
        }

        service.putInCache(TEST_REGION, "test", 2);

        Assert.assertEquals((Integer) 2, service.getFromCache(TEST_REGION, "test"));
    }

    @Test
    public void shouldBeAbleToDeleteCacheEntries() throws CacheOperationException {
        service.initialise(serviceProps);
        service.putInCache(TEST_REGION, "test", 1);

        service.removeFromCache(TEST_REGION, "test");
        Assert.assertEquals(0, service.sizeOfCache(TEST_REGION));
    }

    @Test
    public void shouldBeAbleToClearCache() throws CacheOperationException {
        service.initialise(serviceProps);
        service.putInCache(TEST_REGION, "test1", 1);
        service.putInCache(TEST_REGION, "test2", 2);
        service.putInCache(TEST_REGION, "test3", 3);


        service.clearCache(TEST_REGION);

        Assert.assertEquals(0, service.sizeOfCache(TEST_REGION));
    }

    @Test
    public void shouldGetAllKeysFromCache() throws CacheOperationException {
        service.initialise(serviceProps);
        service.putInCache(TEST_REGION, "test1", 1);
        service.putInCache(TEST_REGION, "test2", 2);
        service.putInCache(TEST_REGION, "test3", 3);

        Assert.assertEquals(3, service.sizeOfCache(TEST_REGION));
        assertThat(service.getAllKeysFromCache(TEST_REGION), IsCollectionContaining.hasItems("test1", "test2", "test3"));
    }

    @Test
    public void shouldGetAllValues() throws CacheOperationException {
        service.initialise(serviceProps);
        service.putInCache(TEST_REGION, "test1", 1);
        service.putInCache(TEST_REGION, "test2", 2);
        service.putInCache(TEST_REGION, "test3", 3);
        service.putInCache(TEST_REGION, "duplicate", 3);

        Assert.assertEquals(4, service.sizeOfCache(TEST_REGION));
        Assert.assertEquals(4, service.getAllValuesFromCache(TEST_REGION).size());

        assertThat(service.getAllValuesFromCache(TEST_REGION), IsCollectionContaining.hasItems(1, 2, 3));
    }

    @Test
    public void shouldAgeOffValues() throws CacheOperationException {
        // given
        String filePath = new File("src/test/resources/cache.ccf").getAbsolutePath();
        serviceProps.setProperty(CacheProperties.CACHE_CONFIG_FILE, filePath);
        service.initialise(serviceProps);

        // when
        service.putInCache(AGE_OFF_REGION, "test", 1);
        Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);

        // then
        assertNull(service.getFromCache(AGE_OFF_REGION, "test"));
    }

    @Test
    public void shouldAllowAgedOffValuesToBeReplaced() throws CacheOperationException {
        // given
        String filePath = new File("src/test/resources/cache.ccf").getAbsolutePath();
        serviceProps.setProperty(CacheProperties.CACHE_CONFIG_FILE, filePath);
        service.initialise(serviceProps);

        // when
        service.putInCache(AGE_OFF_REGION, "test", 1);
        Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS); // aged off
        service.putInCache(AGE_OFF_REGION, "test", 1);

        // then
        assertEquals((Integer) 1, service.getFromCache(AGE_OFF_REGION, "test"));
    }
}
