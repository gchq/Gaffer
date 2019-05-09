/*
 * Copyright 2016-2019 Crown Copyright
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

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
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
    public void shouldConstructContextWithUser() {
        // Given
        final User user = new User();

        // When
        final Context context = new Context.Builder()
                .user(user)
                .build();

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
    public void shouldThrowExceptionIfUserIsNull() {
        // Given
        final User user = null;

        // When / Then
        try {
            new Context(user);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertEquals("User is required", e.getMessage());
        }
    }

    @Test
    public void shouldConstructContextWithContext() {
        // Given
        final Context context = new Context.Builder()
                .user(new User())
                .build();
        final Exporter exporter = mock(Exporter.class);
        context.addExporter(exporter);
        final OperationChain opChain = mock(OperationChain.class);
        final OperationChain opChainClone = mock(OperationChain.class);
        given(opChain.shallowClone()).willReturn(opChainClone);
        context.setOriginalOpChain(opChain);
        context.setConfig("key", "value");

        // When
        final Context clone = new Context(context);

        // Then
        assertSame(context.getUser(), clone.getUser());
        assertNotEquals(context.getJobId(), clone.getJobId());
        assertNotSame(context.getOriginalOpChain(), clone.getOriginalOpChain());
        assertSame(opChainClone, clone.getOriginalOpChain());
        assertEquals(1, clone.getExporters().size());
        assertSame(exporter, clone.getExporters().iterator().next());
        assertEquals(context.getConfig("key"), clone.getConfig("key"));
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

    @Test
    public void shouldShallowCloneContext() {
        // Given
        final User user = new User("user");
        final String testConf = "testConf";
        final Context context = new Context.Builder()
                .user(user)
                .config(testConf, "testConfVal")
                .variable("testVar", "testVarVal")
                .build();

        // When
        Context clonedContext = context.shallowClone();

        // Then
        assertNotSame(context, clonedContext);
        assertEquals(context.getUser(), clonedContext.getUser());
        assertEquals(context.getExporters().toString(),
                clonedContext.getExporters().toString());
        assertEquals(context.getConfig(testConf), clonedContext.getConfig(testConf));
        assertEquals(context.getVariables(), clonedContext.getVariables());
    }

    @Test
    public void shouldAddVariables() {
        // Given
        final User user = new User("user");
        final String testConf = "testConf";
        final Context context = new Context.Builder()
                .user(user)
                .config(testConf, "testConfVal")
                .build();
        // When
        context.setVariable("testVar", "testVarVal");
        context.setVariable("testVar2", "testVarVal2");

        // Then
        assertFalse(context.getVariables().isEmpty());
        assertEquals(context.getVariable("testVar"), "testVarVal");
        assertEquals(context.getVariable("testVar2"), "testVarVal2");
    }

    @Test
    public void shouldAddVariables2() {
        // Given
        final User user = new User("user");
        final String testConf = "testConf";
        final Context context = new Context.Builder()
                .user(user)
                .config(testConf, "testConfVal")
                .build();
        // When
        context.addVariables(Collections.singletonMap("testVar", "testVarVal"));
        context.addVariables(Collections.singletonMap("testVar2", "testVarVal2"));

        // Then
        assertFalse(context.getVariables().isEmpty());
        assertEquals(context.getVariable("testVar"), "testVarVal");
        assertEquals(context.getVariable("testVar2"), "testVarVal2");
    }
}
