/*
 * Copyright 2017 Crown Copyright
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

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.impl.HashMapCacheService;
import uk.gov.gchq.gaffer.cache.util.CacheProperties;
import uk.gov.gchq.gaffer.commonutil.exception.OverwritingException;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.named.operation.cache.exception.CacheOperationFailedException;
import uk.gov.gchq.gaffer.named.view.NamedView;

import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class NamedViewCacheTest {
    private static NamedViewCache cache;
    private static final String EXCEPTION_EXPECTED = "Exception expected";
    private NamedView standardNamedView = new NamedView.Builder().name("standardView").build();
    private NamedView alternativeNamedView = new NamedView.Builder().name("alternativeView").build();

    @BeforeClass
    public static void setUp() {
        Properties properties = new Properties();
        properties.setProperty(CacheProperties.CACHE_SERVICE_CLASS, HashMapCacheService.class.getName());
        CacheServiceLoader.initialise(properties);
        cache = new NamedViewCache();
    }

    @Before
    public void beforeEach() throws CacheOperationFailedException {
        cache.clearCache();
    }

    @Test
    public void shouldAddNamedView() throws CacheOperationFailedException {
        cache.addNamedView(standardNamedView, false);
        NamedView namedViewFromCache = cache.getNamedView(standardNamedView.getViewName());

        assertEquals(standardNamedView, namedViewFromCache);
    }

    @Test
    public void shouldThrowExceptionIfNamedViewAlreadyExists() throws CacheOperationFailedException {
        cache.addNamedView(standardNamedView, false);
        try {
            cache.addNamedView(standardNamedView, false);
            fail(EXCEPTION_EXPECTED);
        } catch (OverwritingException e) {
            assertTrue(e.getMessage().equals("Cache entry already exists for key: " + standardNamedView.getViewName()));
        }
    }

    @Test
    public void shouldThrowExceptionWhenDeletingIfKeyIsNull() throws CacheOperationFailedException {
        try {
            cache.deleteNamedView(null);
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("NamedView name cannot be null"));
        }
    }

    @Test
    public void shouldThrowExceptionWhenAddingIfKeyIsNull() throws CacheOperationFailedException {
        NamedView nullNameNamedView = new NamedView.Builder().name(null).build();
        try {
            cache.addNamedView(nullNameNamedView, false);
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("NamedView name cannot be null"));
        }
    }

    @Test
    public void shouldThrowExceptionWhenGettingIfKeyIsNull() throws CacheOperationFailedException {
        try {
            cache.getNamedView(null);
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("NamedView name cannot be null"));
        }
    }

    @Test
    public void shouldReturnEmptySetIfThereAreNoOperationsInTheCache() throws CacheOperationFailedException {
        CloseableIterable<NamedView> views = cache.getAllNamedViews();
        assertEquals(0, Iterables.size(views));
    }

    @Test
    public void shouldBeAbleToReturnFullExtendedOperationChain() throws CacheOperationFailedException {
        cache.addNamedView(standardNamedView, false);
        cache.addNamedView(alternativeNamedView, false);

        Set<NamedView> allViews = Sets.newHashSet(cache.getAllNamedViews());
        assertTrue(allViews.contains(standardNamedView));
        assertTrue(allViews.contains(alternativeNamedView));
        for (NamedView namedView : allViews) {
            System.out.println(namedView.toString());
        }
        assertEquals(2, allViews.size());
    }
}
