/*
 * Copyright 2020-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation.handler.named.cache;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.impl.JcsCacheService;
import uk.gov.gchq.gaffer.cache.util.CacheProperties;
import uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.user.StoreUser;
import uk.gov.gchq.gaffer.user.User;

import java.util.Properties;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.gov.gchq.gaffer.store.operation.handler.named.cache.NamedViewCache.NAMED_VIEW_CACHE_SERVICE_NAME;

public class NamedViewCacheBackwardCompatibilityTest {
    private static final String BACKWARDS_COMPATABILITY_2_0_0 = "backwards_compatability_2.0.0";
    public static final String GAFFER_2_0_0_CACHE_CACHE_CCF = "src/test/resources/gaffer-2.0.0-cache/cache.ccf";
    private static NamedViewCache viewCache;
    private static final User ADDING_USER = StoreUser.authUser();
    private static final String VIEW_NAME = "TestView";

    @BeforeAll
    public static void setUp() {
        CacheServiceLoader.shutdown();
        final Properties properties = new Properties();
        // Note that this config causes a binary resource file containing data to be loaded into the cache
        // This data includes ADDING_USER and VIEW_NAME
        properties.setProperty(CacheProperties.CACHE_CONFIG_FILE, GAFFER_2_0_0_CACHE_CACHE_CCF);
        CacheServiceLoader.initialise(NAMED_VIEW_CACHE_SERVICE_NAME, JcsCacheService.class.getName(), properties);
        viewCache = new NamedViewCache(BACKWARDS_COMPATABILITY_2_0_0);
    }

    @Test
    public void shouldReturnExpectedNamedViewDetailUsingCacheDataFromVersion2() throws Exception {
        final NamedViewDetail namedViewDetail = new NamedViewDetail.Builder()
                .name(VIEW_NAME)
                .description("standard View")
                .creatorId(ADDING_USER.getUserId())
                .writers(asList("writerAuth1", "writerAuth2"))
                .view(new View.Builder().build())
                .build();

        final NamedViewDetail namedViewDetailFromCacheVersion2_0 = viewCache.getNamedView(namedViewDetail.getName(), ADDING_USER);

        assertEquals(namedViewDetail.getOrDefaultReadAccessPredicate(), namedViewDetailFromCacheVersion2_0.getOrDefaultReadAccessPredicate());
        assertEquals(namedViewDetail.getOrDefaultWriteAccessPredicate(), namedViewDetailFromCacheVersion2_0.getOrDefaultWriteAccessPredicate());
    }
}
