/*
 * Copyright 2016 Crown Copyright
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

package gaffer.export;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import gaffer.commonutil.iterable.CloseableIterable;
import gaffer.user.User;
import org.apache.commons.lang.NotImplementedException;
import org.junit.Test;
import java.util.Arrays;


public class ExporterTest {

    @Test
    public void shouldThrowExceptionWhenAddIfExporterNotInitialisedWithUser() {
        // Given
        final String key = "key1";
        final Iterable<?> values = Arrays.asList("item1", "item2");
        final User user = new User();
        final Exporter exporter = new ExporterImpl();

        // When / Then
        try {
            exporter.add(key, values, user);
            fail("NotImplementedException expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionWhenAddIfUserIsNull() {
        // Given
        final String key = "key1";
        final Iterable<?> values = Arrays.asList("item1", "item2");
        final User user = new User();
        final Exporter exporter = new ExporterImpl();
        exporter.initialise(null, user);

        // When / Then
        try {
            exporter.add(key, values, null);
            fail("NotImplementedException expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionWhenAddIfDifferentUsers() {
        // Given
        final String key = "key1";
        final Iterable<?> values = Arrays.asList("item1", "item2");
        final User user1 = new User("user1");
        final User user2 = new User("user2");
        final Exporter exporter = new ExporterImpl();
        exporter.initialise(null, user1);

        // When / Then
        try {
            exporter.add(key, values, user2);
            fail("NotImplementedException expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldDelegateToInternalAdd() {
        // Given
        final String key = "key1";
        final Iterable<?> values = Arrays.asList("item1", "item2");
        final User user = new User();
        final Exporter exporter = new ExporterImpl();
        exporter.initialise(null, user);

        // When / Then
        try {
            exporter.add(key, values, user);
            fail("NotImplementedException expected");
        } catch (final NotImplementedException e) {
            assertEquals("_add(" + key + "," + values + "," + user + ")", e.getMessage());
        }
    }


    @Test
    public void shouldThrowExceptionWhenGetIfExporterNotInitialisedWithUser() {
        // Given
        final String key = "key1";
        final User user = new User();
        final int start = 0;
        final int end = 10;
        final Exporter exporter = new ExporterImpl();

        // When / Then
        try {
            exporter.get(key, user, start, end);
            fail("NotImplementedException expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionWhenGetIfUserIsNull() {
        // Given
        final String key = "key1";
        final User user = new User();
        final int start = 0;
        final int end = 10;
        final Exporter exporter = new ExporterImpl();
        exporter.initialise(null, user);

        // When / Then
        try {
            exporter.get(key, null, start, end);
            fail("NotImplementedException expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionWhenGetIfDifferentUsers() {
        // Given
        final String key = "key1";
        final User user1 = new User("user1");
        final User user2 = new User("user2");
        final int start = 0;
        final int end = 10;
        final Exporter exporter = new ExporterImpl();
        exporter.initialise(null, user1);

        // When / Then
        try {
            exporter.get(key, user2, start, end);
            fail("NotImplementedException expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldDelegateToInternalGet() {
        // Given
        final String key = "key1";
        final Iterable<?> values = Arrays.asList("item1", "item2");
        final User user = new User();
        final int start = 0;
        final int end = 10;
        final Exporter exporter = new ExporterImpl();
        exporter.initialise(null, user);

        // When / Then
        try {
            exporter.get(key, user, start, end);
            fail("NotImplementedException expected");
        } catch (final NotImplementedException e) {
            assertEquals("_get(" + key + "," + user + "," + start + "," + end + ")", e.getMessage());
        }
    }

    @Test
    public void shouldGetAndSetTimestamp() {
        // Given
        final Exporter exporter = new ExporterImpl();
        final long timestamp = 1000L;

        // When / Then
        exporter.setTimestamp(timestamp);
        assertEquals(timestamp, exporter.getTimestamp());
    }

    @Test
    public void shouldThrowExceptionIfUserNullWhenGettingExportName() {
        // Given
        final Exporter exporter = new ExporterImpl();

        // When / Then
        try {
            exporter.getUserTimestampedExportName();
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldGetExportName() {
        // Given
        final Exporter exporter = new ExporterImpl();
        final long timestamp = 1000L;
        final User user01 = new User("user01");
        exporter.initialise(null, user01);
        exporter.setTimestamp(timestamp);


        // When
        final String name = exporter.getUserTimestampedExportName();

        // Then
        assertEquals("user01_" + timestamp, name);
    }

    private static final class ExporterImpl extends Exporter {
        @Override
        protected void _add(final String key, final Iterable<?> values, final User user) {
            throw new NotImplementedException("_add(" + key + "," + values + "," + user + ")");
        }

        @Override
        protected CloseableIterable<?> _get(final String key, final User user, final int start, final int end) {
            throw new NotImplementedException("_get(" + key + "," + user + "," + start + "," + end + ")");
        }
    }
}
