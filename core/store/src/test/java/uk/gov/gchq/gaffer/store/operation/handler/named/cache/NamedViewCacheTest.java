/*
 * Copyright 2016-2024 Crown Copyright
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

import com.google.common.collect.Sets;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.cache.impl.HashMapCacheService;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.exception.OverwritingException;
import uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.user.User;

import java.util.Arrays;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

public class NamedViewCacheTest {

    public static final String SUFFIX_CACHE_NAME = "suffix";
    private static NamedViewCache cache;
    private static final String GAFFER_USER_A = "gaffer user A";
    private static final String GAFFER_USER_B = "gaffer user B";

    private static final String ADMIN_AUTH = "admin auth";
    private static final String EMPTY_ADMIN_AUTH = "gasbggfdhj";

    private static final String VIEW_NAME_A = "viewA";
    private static final String VIEW_NAME_B = "viewB";

    private final View viewA = new View.Builder().build();
    private final View viewB = new View.Builder().edge(TestGroups.EDGE).build();

    private final User userA = new User.Builder().opAuths(GAFFER_USER_A).userId("123").build();
    private final User userB = new User.Builder().opAuths(GAFFER_USER_A, GAFFER_USER_B).userId("456").build();
    private final User userC = new User();
    private final User userWithAdminAuth = new User.Builder().opAuths(ADMIN_AUTH).userId("adminUser").build();

    private final NamedViewDetail viewDetailA = new NamedViewDetail.Builder()
            .name(VIEW_NAME_A)
            .description(VIEW_NAME_A)
            .creatorId(userA.getUserId())
            .view(viewA)
            .build();

    private final NamedViewDetail viewDetailB = new NamedViewDetail.Builder()
            .name(VIEW_NAME_B)
            .description(VIEW_NAME_B)
            .creatorId(userB.getUserId())
            .view(viewB)
            .build();

    @BeforeAll
    public static void setUp() {
        CacheServiceLoader.initialise(HashMapCacheService.class.getName());
        cache = new NamedViewCache(SUFFIX_CACHE_NAME);
    }

    @BeforeEach
    public void beforeEach() throws CacheOperationException {
        cache.clearCache();
    }

    @AfterAll
    public static void tearDown() {
        CacheServiceLoader.shutdown();
    }

    @Test
    public void shouldAddNamedView() throws CacheOperationException {
        cache.addNamedView(viewDetailA, false);
        final NamedViewDetail namedViewFromCache = cache.getNamedView(viewDetailA.getName(), userA);

        assertThat(namedViewFromCache).isEqualTo(viewDetailA);
    }

    @Test
    public void shouldThrowExceptionIfNamedViewAlreadyExists() throws CacheOperationException {
        cache.addNamedView(viewDetailA, false);
        assertThatExceptionOfType(OverwritingException.class)
            .isThrownBy(() -> cache.addNamedView(viewDetailA, false))
            .withMessageContaining(String.format("Cache entry already exists for key: %s", VIEW_NAME_A));
    }

    @Test
    public void shouldThrowExceptionWhenDeletingIfKeyIsNull() throws CacheOperationException {
        assertThatIllegalArgumentException().isThrownBy(() -> cache.deleteNamedView(null, userA))
                .withMessageContaining("NamedView name cannot be null");
    }

    @Test
    public void shouldThrowExceptionWhenGettingIfKeyIsNull() throws CacheOperationException {
        assertThatExceptionOfType(CacheOperationException.class).isThrownBy(() -> cache.getNamedView(null, userA))
                .withMessageContaining("NamedView name cannot be null");
    }

    @Test
    public void shouldRemoveNamedView() throws CacheOperationException {
        cache.addNamedView(viewDetailA, false);
        cache.deleteNamedView(viewDetailA.getName(), userA);
    }

    @Test
    public void shouldReturnEmptySetIfThereAreNoOperationsInTheCache() throws CacheOperationException {
        final Iterable<NamedViewDetail> views = cache.getAllNamedViews(userA);
        assertThat(views).hasSize(0);
    }

    @Test
    public void shouldBeAbleToReturnAllNamedViewsFromCache() throws CacheOperationException {
        cache.addNamedView(viewDetailA, false);
        cache.addNamedView(viewDetailB, false);

        final Set<NamedViewDetail> allViews = Sets.newHashSet(cache.getAllNamedViews(userA));

        assertThat(allViews)
                .contains(viewDetailA, viewDetailB)
                .hasSize(2);
    }

    @Test
    public void shouldAllowUsersWriteAccessToTheirOwnViews() throws CacheOperationException {
        cache.addNamedView(viewDetailA, false, userA, EMPTY_ADMIN_AUTH);
        cache.addNamedView(new NamedViewDetail.Builder().name(VIEW_NAME_A).view("").build(), true, userA, EMPTY_ADMIN_AUTH);

        assertThat(cache.getNamedView(VIEW_NAME_A, userA).getView()).isEqualTo("");
    }

    @Test
    public void shouldThrowExceptionIfUnauthorisedUserTriesToOverwriteView() throws CacheOperationException {
        cache.addNamedView(viewDetailA, false, userA, EMPTY_ADMIN_AUTH);
        assertThatExceptionOfType(CacheOperationException.class)
                .isThrownBy(() -> cache.addNamedView(viewDetailA, true, userC, EMPTY_ADMIN_AUTH))
                .withMessageContaining("does not have permission to overwrite");
    }

    @Test
    public void shouldAllowUserToOverwriteViewWithPermission() throws CacheOperationException {
        // Given
        final NamedViewDetail namedViewDetailWithUsersAllowedToWrite = new NamedViewDetail.Builder()
                .name(VIEW_NAME_B)
                .description(VIEW_NAME_B)
                .creatorId(userB.getUserId())
                .writers(Arrays.asList(GAFFER_USER_A))
                .view(viewB)
                .build();
        cache.addNamedView(namedViewDetailWithUsersAllowedToWrite, false, userB, EMPTY_ADMIN_AUTH);

        // When
        cache.addNamedView(new NamedViewDetail.Builder().name(VIEW_NAME_B).view("").build(), true, userA, EMPTY_ADMIN_AUTH);

        // Then
        assertThat(cache.getNamedView(VIEW_NAME_B, userA).getView()).isEqualTo("");
    }

    @Test
    public void shouldThrowExceptionIfUnauthorisedUserTriesToDeleteView() throws CacheOperationException {
        cache.addNamedView(viewDetailA, false, userB, EMPTY_ADMIN_AUTH);
        assertThatExceptionOfType(CacheOperationException.class)
                .isThrownBy(() -> cache.deleteNamedView(VIEW_NAME_A, userC, EMPTY_ADMIN_AUTH))
                .withMessageContaining("does not have permission to delete named view");
    }

    @Test
    public void shouldAllowUserToDeleteViewWithNoPermissionsSet() throws CacheOperationException {
        // Given
        final NamedViewDetail namedViewDetailWithUsersAllowedToWrite = new NamedViewDetail.Builder()
                .name(VIEW_NAME_B)
                .description(VIEW_NAME_B)
                .view(viewB)
                .build();
        cache.addNamedView(namedViewDetailWithUsersAllowedToWrite, false);

        // When / Then - no exceptions
        cache.deleteNamedView(VIEW_NAME_B, userA, EMPTY_ADMIN_AUTH);
    }

    @Test
    public void shouldAllowUserToDeleteViewWithPermission() throws CacheOperationException {
        // Given
        final NamedViewDetail namedViewDetailWithUsersAllowedToWrite = new NamedViewDetail.Builder()
                .name(VIEW_NAME_B)
                .description(VIEW_NAME_B)
                .creatorId(userB.getUserId())
                .writers(Arrays.asList(GAFFER_USER_A))
                .view(viewB)
                .build();
        cache.addNamedView(namedViewDetailWithUsersAllowedToWrite, false, userB, EMPTY_ADMIN_AUTH);

        // When / Then - no exceptions
        cache.deleteNamedView(VIEW_NAME_B, userA, EMPTY_ADMIN_AUTH);
    }

    @Test
    public void shouldAllowUserToAddWithAdminAuth() throws CacheOperationException {
        // Given
        cache.addNamedView(viewDetailB, false, userB, EMPTY_ADMIN_AUTH);

        final NamedViewDetail alternativeWithADifferentView = new NamedViewDetail.Builder()
                .name(VIEW_NAME_B)
                .description(VIEW_NAME_B)
                .creatorId(userA.getUserId())
                .view(new View())
                .build();

        // When / Then - no exceptions
        cache.addNamedView(alternativeWithADifferentView, true, userWithAdminAuth, ADMIN_AUTH);
    }
}
