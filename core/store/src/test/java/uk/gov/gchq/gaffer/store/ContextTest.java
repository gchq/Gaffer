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

package uk.gov.gchq.gaffer.store;

import org.junit.Test;
import uk.gov.gchq.gaffer.export.Exporter;
import uk.gov.gchq.gaffer.user.User;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class ContextTest {
    @Test
    public void shouldConstructContextWithUser() {
        // Given
        final User user = new User();

        // When
        final Context context = new Context(user);

        // Then
        assertEquals(user, context.getUser());
        assertTrue(context.getExporters().isEmpty());
    }

    @Test
    public void shouldConstructContextWithUnknownUser() {
        // Given
        // When
        final Context context = new Context();

        // Then
        assertEquals(User.UNKNOWN_USER_ID, context.getUser().getUserId());
    }

    @Test
    public void shouldAddAndGetExporterWithKey() {
        // Given
        final Exporter exporter = mock(Exporter.class);
        final Context context = new Context();
        final String key = "key";
        given(exporter.getKey()).willReturn(key);

        // When
        context.addExporter(exporter);

        // Then
        assertSame(exporter, context.getExporter(key));
    }
}
