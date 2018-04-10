/*
 * Copyright 2017-2018 Crown Copyright
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.impl.HashMapCacheService;
import uk.gov.gchq.gaffer.cache.util.CacheProperties;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.exception.OverwritingException;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.named.operation.cache.exception.CacheOperationFailedException;
import uk.gov.gchq.gaffer.user.User;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class NamedViewCacheTest {
    private static NamedViewCache cache;
    private static final String GAFFER_USER = "gaffer user";
    private static final String ADVANCED_GAFFER_USER = "advanced gaffer user";
    private static final String ADMIN_AUTH = "admin auth";
    private static final String EMPTY_ADMIN_AUTH = "";
    private static final String EXCEPTION_EXPECTED = "Exception expected";
    private static final String STANDARD_VIEW_NAME = "standardView";
    private static final String ALTERNATIVE_VIEW_NAME = "alternativeView";
    private View standardView = new View.Builder().build();
    private View alternativeView = new View.Builder().edge(TestGroups.EDGE).build();
    private final User blankUser = new User();
    private User standardUser = new User.Builder().opAuths(GAFFER_USER).userId("123").build();
    private User userWithAdminAuth = new User.Builder().opAuths(ADMIN_AUTH).userId("adminUser").build();
    private User advancedUser = new User.Builder().opAuths(GAFFER_USER, ADVANCED_GAFFER_USER).userId("456").build();

    private NamedViewDetail standard = new NamedViewDetail.Builder()
            .name(STANDARD_VIEW_NAME)
            .description("standard View")
            .creatorId(standardUser.getUserId())
            .view(standardView)
            .build();

    private NamedViewDetail alternative = new NamedViewDetail.Builder()
            .name(ALTERNATIVE_VIEW_NAME)
            .description("alternative View")
            .creatorId(advancedUser.getUserId())
            .view(alternativeView)
            .build();

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
        cache.addNamedView(standard, false);
        NamedViewDetail namedViewFromCache = cache.getNamedView(standard.getName());

        assertEquals(standard, namedViewFromCache);
    }

    @Test
    public void shouldThrowExceptionIfNamedViewAlreadyExists() throws CacheOperationFailedException {
        cache.addNamedView(standard, false);
        try {
            cache.addNamedView(standard, false);
            fail(EXCEPTION_EXPECTED);
        } catch (OverwritingException e) {
            assertTrue(e.getMessage().equals("Cache entry already exists for key: " + STANDARD_VIEW_NAME));
        }
    }

    @Test
    public void shouldThrowExceptionWhenDeletingIfKeyIsNull() throws CacheOperationFailedException {
        try {
            cache.deleteNamedView(null);
            fail(EXCEPTION_EXPECTED);
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("NamedView name cannot be null"));
        }
    }

    @Test
    public void shouldThrowExceptionWhenGettingIfKeyIsNull() throws CacheOperationFailedException {
        try {
            cache.getNamedView(null);
            fail(EXCEPTION_EXPECTED);
        } catch (CacheOperationFailedException e) {
            assertTrue(e.getMessage().contains("NamedView name cannot be null"));
        }
    }

    @Test
    public void shouldRemoveNamedView() throws CacheOperationFailedException {
        cache.addNamedView(standard, false);
        cache.deleteNamedView(standard.getName());
    }

    @Test
    public void shouldReturnEmptySetIfThereAreNoOperationsInTheCache() throws CacheOperationFailedException {
        CloseableIterable<NamedViewDetail> views = cache.getAllNamedViews();
        assertEquals(0, Iterables.size(views));
    }

    @Test
    public void shouldBeAbleToReturnAllNamedViewsFromCache() throws CacheOperationFailedException {
        cache.addNamedView(standard, false);
        cache.addNamedView(alternative, false);

        Set<NamedViewDetail> allViews = Sets.newHashSet(cache.getAllNamedViews());

        assertTrue(allViews.contains(standard));
        assertTrue(allViews.contains(alternative));
        assertEquals(2, allViews.size());
    }

    @Test
    public void shouldAllowUsersWriteAccessToTheirOwnViews() throws CacheOperationFailedException {
        cache.addNamedView(standard, false, standardUser, EMPTY_ADMIN_AUTH);
        cache.addNamedView(new NamedViewDetail.Builder().name(STANDARD_VIEW_NAME).view("").build(), true, standardUser, EMPTY_ADMIN_AUTH);

        Assert.assertEquals("", cache.getNamedView(STANDARD_VIEW_NAME).getView());
    }

    @Test
    public void shouldThrowExceptionIfUnauthorisedUserTriesToOverwriteView() throws CacheOperationFailedException {
        cache.addNamedView(alternative, false, standardUser, EMPTY_ADMIN_AUTH);
        try {
            cache.addNamedView(standard, true, blankUser, EMPTY_ADMIN_AUTH);
        } catch (final CacheOperationFailedException e) {
            assertTrue(e.getMessage().contains("does not have permission to overwrite"));
        }
    }

    @Test
    public void shouldAllowUserToOverwriteViewWithPermission() throws CacheOperationFailedException {
        // Given
        NamedViewDetail namedViewDetailWithUsersAllowedToWrite = new NamedViewDetail.Builder()
                .name(ALTERNATIVE_VIEW_NAME)
                .description("alternative View")
                .creatorId(advancedUser.getUserId())
                .writers(Arrays.asList(GAFFER_USER))
                .view(alternativeView)
                .build();
        cache.addNamedView(namedViewDetailWithUsersAllowedToWrite, false, advancedUser, EMPTY_ADMIN_AUTH);

        // When
        cache.addNamedView(new NamedViewDetail.Builder().name(ALTERNATIVE_VIEW_NAME).view("").build(), true, standardUser, EMPTY_ADMIN_AUTH);

        // Then
        assertEquals("", cache.getNamedView(ALTERNATIVE_VIEW_NAME).getView());
    }

    @Test
    public void shouldThrowExceptionIfUnauthorisedUserTriesToDeleteView() throws CacheOperationFailedException {
        cache.addNamedView(standard, false, advancedUser, EMPTY_ADMIN_AUTH);
        try {
            cache.deleteNamedView(STANDARD_VIEW_NAME, standardUser, EMPTY_ADMIN_AUTH);
        } catch (final CacheOperationFailedException e) {
            assertTrue(e.getMessage().contains("does not have permission to delete named view"));
        }
    }

    @Test
    public void shouldAllowUserToDeleteViewWithNoPermissionsSet() throws CacheOperationFailedException {
        // Given
        NamedViewDetail namedViewDetailWithUsersAllowedToWrite = new NamedViewDetail.Builder()
                .name(ALTERNATIVE_VIEW_NAME)
                .description("alternative View")
                .view(alternativeView)
                .build();
        cache.addNamedView(namedViewDetailWithUsersAllowedToWrite, false);

        // When / Then - no exceptions
        cache.deleteNamedView(ALTERNATIVE_VIEW_NAME, standardUser, EMPTY_ADMIN_AUTH);
    }

    @Test
    public void shouldAllowUserToDeleteViewWithPermission() throws CacheOperationFailedException {
        // Given
        NamedViewDetail namedViewDetailWithUsersAllowedToWrite = new NamedViewDetail.Builder()
                .name(ALTERNATIVE_VIEW_NAME)
                .description("alternative View")
                .creatorId(advancedUser.getUserId())
                .writers(Arrays.asList(GAFFER_USER))
                .view(alternativeView)
                .build();
        cache.addNamedView(namedViewDetailWithUsersAllowedToWrite, false, advancedUser, EMPTY_ADMIN_AUTH);

        // When / Then - no exceptions
        cache.deleteNamedView(ALTERNATIVE_VIEW_NAME, standardUser, EMPTY_ADMIN_AUTH);
    }

    @Test
    public void shouldAllowUserToAddWithAdminAuth() throws CacheOperationFailedException {
        // Given
        cache.addNamedView(alternative, false, advancedUser, EMPTY_ADMIN_AUTH);

        NamedViewDetail alternativeWithADifferentView = new NamedViewDetail.Builder()
                .name(ALTERNATIVE_VIEW_NAME)
                .description("alternative View")
                .creatorId(standardUser.getUserId())
                .view(new View())
                .build();

        // When / Then - no exceptions
        cache.addNamedView(alternativeWithADifferentView, true, userWithAdminAuth, ADMIN_AUTH);
    }
}
