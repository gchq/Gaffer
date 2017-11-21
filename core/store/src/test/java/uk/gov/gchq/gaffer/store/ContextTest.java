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

package uk.gov.gchq.gaffer.store;

import org.junit.Test;

import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.export.Exporter;
import uk.gov.gchq.gaffer.user.User;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class ContextTest {
    @Test
    public void shouldConstructContextsWithTheSameUserAndGenerateDifferentJobIds() {
        // Given
        final User user = new User();

        // When
        final Context context1 = new Context(user);
        final Context context2 = new Context(user);

        // Then
        assertEquals(user, context1.getUser());
        assertEquals(user, context2.getUser());
        assertNotEquals(context1.getJobId(), context2.getJobId());
        assertTrue(context1.getExporters().isEmpty());
        assertTrue(context2.getExporters().isEmpty());
    }

    @Test
    public void shouldConstructContextWithUserAndJobId() {
        // Given
        final User user = new User();
        final String randomId = "randomId";

        // When
        final Context context = new Context.Builder()
                .user(user)
                .jobId(randomId)
                .build();

        // Then
        assertEquals(user, context.getUser());
        assertEquals(randomId, context.getJobId());
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
    public void shouldAddAndGetExporter() {
        // Given
        final Exporter exporter = mock(Exporter.class);
        final Context context = new Context();

        // When
        context.addExporter(exporter);

        // Then
        assertSame(exporter, context.getExporter(exporter.getClass()));
        assertSame(exporter, context.getExporter(Exporter.class));
    }

    @Test
    public void shouldSetAndGetOriginalOpChain() {
        // Given
        final OperationChain<?> opChain = mock(OperationChain.class);
        final Context context = new Context();

        // When
        context.setOriginalOpChain(opChain);

        // Then
        assertSame(opChain, context.getOriginalOpChain());
    }
}
