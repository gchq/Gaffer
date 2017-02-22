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

package uk.gov.gchq.gaffer.export;

import org.apache.commons.lang.NotImplementedException;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.export.initialise.InitialiseExport;
import uk.gov.gchq.gaffer.user.User;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;


public class ExporterTest {

    @Test
    public void shouldThrowExceptionWhenAddIfExporterNotInitialisedWithUser() throws OperationException {
        // Given
        final Iterable<?> values = Arrays.asList("item1", "item2");
        final User user = new User();
        final ExporterImpl exporter = new ExporterImpl();

        // When / Then
        try {
            exporter.add(values, user, "executionId");
            fail("NotImplementedException expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionWhenAddIfUserIsNull() throws OperationException {
        // Given
        final Iterable<?> values = Arrays.asList("item1", "item2");
        final User user = new User();
        final ExporterImpl exporter = new ExporterImpl();
        final InitialiseExport initialiseExport = mock(InitialiseExport.class);
        given(initialiseExport.getKey()).willReturn("key");
        exporter.initialise(initialiseExport, null, user, "exportName");

        // When / Then
        try {
            exporter.add(values, null, "executionId");
            fail("NotImplementedException expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionWhenAddIfDifferentUsers() throws OperationException {
        // Given
        final Iterable<?> values = Arrays.asList("item1", "item2");
        final User user1 = new User("user1");
        final User user2 = new User("user2");
        final ExporterImpl exporter = new ExporterImpl();
        final InitialiseExport initialiseExport = mock(InitialiseExport.class);
        given(initialiseExport.getKey()).willReturn("key");
        exporter.initialise(initialiseExport, null, user1, "exportName");

        // When / Then
        try {
            exporter.add(values, user2, "executionId");
            fail("NotImplementedException expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldDelegateToInternalAdd() throws OperationException {
        // Given
        final Iterable<?> values = Arrays.asList("item1", "item2");
        final User user = new User();
        final ExporterImpl exporter = new ExporterImpl();
        final InitialiseExport initialiseExport = mock(InitialiseExport.class);
        given(initialiseExport.getKey()).willReturn("key");
        exporter.initialise(initialiseExport, null, user, "exportName");

        // When / Then
        try {
            exporter.add(values, user, "executionId");
            fail("NotImplementedException expected");
        } catch (final NotImplementedException e) {
            assertEquals("_add(" + values + "," + user + ")", e.getMessage());
        }
    }


    @Test
    public void shouldThrowExceptionWhenGetIfExporterNotInitialisedWithUser() throws OperationException {
        // Given
        final User user = new User();
        final int start = 0;
        final int end = 10;
        final ExporterImpl exporter = new ExporterImpl();

        // When / Then
        try {
            exporter.get(user, start, end);
            fail("NotImplementedException expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionWhenGetIfUserIsNull() throws OperationException {
        // Given
        final User user = new User();
        final int start = 0;
        final int end = 10;
        final ExporterImpl exporter = new ExporterImpl();
        final InitialiseExport initialiseExport = mock(InitialiseExport.class);
        given(initialiseExport.getKey()).willReturn("key");
        exporter.initialise(initialiseExport, null, user, "exportName");

        // When / Then
        try {
            exporter.get(null, start, end);
            fail("NotImplementedException expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionWhenGetIfDifferentUsers() throws OperationException {
        // Given
        final User user1 = new User("user1");
        final User user2 = new User("user2");
        final int start = 0;
        final int end = 10;
        final ExporterImpl exporter = new ExporterImpl();
        final InitialiseExport initialiseExport = mock(InitialiseExport.class);
        given(initialiseExport.getKey()).willReturn("key");
        exporter.initialise(initialiseExport, null, user1, "exportName");

        // When / Then
        try {
            exporter.get(user2, start, end);
            fail("NotImplementedException expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldDelegateToInternalGet() throws OperationException {
        // Given
        final User user = new User();
        final int start = 0;
        final int end = 10;
        final ExporterImpl exporter = new ExporterImpl();
        final InitialiseExport initialiseExport = mock(InitialiseExport.class);
        given(initialiseExport.getKey()).willReturn("key");
        exporter.initialise(initialiseExport, null, user, "exportName");

        // When / Then
        try {
            exporter.get(user, start, end);
            fail("NotImplementedException expected");
        } catch (final NotImplementedException e) {
            assertEquals("_get(" + user + "," + start + "," + end + ")", e.getMessage());
        }
    }

    @Test
    public void shouldGetAndSetTimestamp() {
        // Given
        final ExporterImpl exporter = new ExporterImpl();
        final long timestamp = 1000L;

        // When / Then
        exporter.setTimestamp(timestamp);
        assertEquals(timestamp, exporter.getTimestamp());
    }

    @Test
    public void shouldThrowExceptionIfUserNullWhenGettingExportName() {
        // Given
        final ExporterImpl exporter = new ExporterImpl();

        // When / Then
        try {
            exporter.getExportName();
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldGetExportName() {
        // Given
        final ExporterImpl exporter = new ExporterImpl();
        final long timestamp = 1000L;
        final String userId = "user01";
        final User user01 = new User(userId);
        final String key = "key";
        final InitialiseExport initialiseExport = mock(InitialiseExport.class);
        given(initialiseExport.getKey()).willReturn("key");
        exporter.initialise(initialiseExport, null, user01, "exportName");
        exporter.setTimestamp(timestamp);


        // When
        final String name = exporter.getExportName();

        // Then
        assertEquals(userId + "_" + timestamp + "_" + key, name);
    }

    private static final class ExporterImpl extends Exporter<Object, InitialiseExport> {
        @Override
        protected void _add(final Iterable<?> values, final User user) {
            throw new NotImplementedException("_add(" + values + "," + user + ")");
        }

        @Override
        protected CloseableIterable<?> _get(final User user, final int start, final int end) {
            throw new NotImplementedException("_get(" + user + "," + start + "," + end + ")");
        }
    }
}
