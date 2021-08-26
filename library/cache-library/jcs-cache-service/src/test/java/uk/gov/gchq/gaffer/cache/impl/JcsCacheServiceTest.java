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

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.cache.ICache;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.cache.util.CacheProperties;
import uk.gov.gchq.gaffer.commonutil.exception.OverwritingException;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class JcsCacheServiceTest {

    private JcsCacheService service = new JcsCacheService();
    private static final String TEST_REGION = "test";
    private static final String ALTERNATIVE_TEST_REGION = "alternativeTest";
    private static final String AGE_OFF_REGION = "ageOff";
    private Properties serviceProps = new Properties();

    @BeforeEach
    public void before() {
        serviceProps.clear();
    }

    @AfterEach
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
        String expected = String.format("Cannot create cache using config file %s", badFileName);

        serviceProps.setProperty(CacheProperties.CACHE_CONFIG_FILE, badFileName);

        assertThatIllegalArgumentException()
                .isThrownBy(() -> service.initialise(serviceProps))
                .withMessage(expected);
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
        assertThat(sameCache.size()).isOne();
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

        assertEquals((Integer) 1, service.getFromCache(TEST_REGION, "test"));
    }

    @Test
    public void shouldOnlyUpdateIfInstructed() throws CacheOperationException {
        service.initialise(serviceProps);
        service.putInCache(TEST_REGION, "test", 1);

        assertThatExceptionOfType(OverwritingException.class)
                .isThrownBy(() -> service.putSafeInCache(TEST_REGION, "test", 2));
        assertEquals((Integer) 1, service.getFromCache(TEST_REGION, "test"));

        service.putInCache(TEST_REGION, "test", 2);

        assertEquals((Integer) 2, service.getFromCache(TEST_REGION, "test"));
    }

    @Test
    public void shouldBeAbleToDeleteCacheEntries() throws CacheOperationException {
        service.initialise(serviceProps);
        service.putInCache(TEST_REGION, "test", 1);

        service.removeFromCache(TEST_REGION, "test");
        assertEquals(0, service.sizeOfCache(TEST_REGION));
    }

    @Test
    public void shouldBeAbleToClearCache() throws CacheOperationException {
        service.initialise(serviceProps);
        service.putInCache(TEST_REGION, "test1", 1);
        service.putInCache(TEST_REGION, "test2", 2);
        service.putInCache(TEST_REGION, "test3", 3);


        service.clearCache(TEST_REGION);

        assertEquals(0, service.sizeOfCache(TEST_REGION));
    }

    @Test
    public void shouldGetAllKeysFromCache() throws CacheOperationException {
        service.initialise(serviceProps);
        service.putInCache(TEST_REGION, "test1", 1);
        service.putInCache(TEST_REGION, "test2", 2);
        service.putInCache(TEST_REGION, "test3", 3);

        assertEquals(3, service.sizeOfCache(TEST_REGION));
        assertThat(service.getAllKeysFromCache(TEST_REGION)).contains("test1", "test2", "test3");
    }

    @Test
    public void shouldGetAllValues() throws CacheOperationException {
        service.initialise(serviceProps);
        service.putInCache(TEST_REGION, "test1", 1);
        service.putInCache(TEST_REGION, "test2", 2);
        service.putInCache(TEST_REGION, "test3", 3);
        service.putInCache(TEST_REGION, "duplicate", 3);

        assertEquals(4, service.sizeOfCache(TEST_REGION));
        assertEquals(4, service.getAllValuesFromCache(TEST_REGION).size());

        assertThat(service.getAllValuesFromCache(TEST_REGION)).contains(1, 2, 3);
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
        assertNull(service.getFromCache(AGE_OFF_REGION, "test"));

        service.putInCache(AGE_OFF_REGION, "test", 1);

        // then
        assertEquals((Integer) 1, service.getFromCache(AGE_OFF_REGION, "test"));
    }
}
