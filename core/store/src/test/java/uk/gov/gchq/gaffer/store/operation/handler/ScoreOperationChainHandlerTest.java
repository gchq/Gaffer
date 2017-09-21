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

package uk.gov.gchq.gaffer.store.operation.handler;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.Limit;
import uk.gov.gchq.gaffer.operation.impl.ScoreOperationChain;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.operation.declaration.OperationDeclarations;
import uk.gov.gchq.gaffer.user.User;

import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class ScoreOperationChainHandlerTest {
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldLoadFromScoreOperationChainDeclarationFile() throws SerialisationException {
        final InputStream s = StreamUtil.openStream(getClass(), "TestScoreOperationChainDeclaration.json");
        final OperationDeclarations deserialised = JSONSerialiser.deserialise(s, OperationDeclarations.class);

        assertEquals(1, deserialised.getOperations().size());
        assert (deserialised.getOperations().get(0).getHandler() instanceof ScoreOperationChainHandler);
    }

    @Test
    public void shouldExecuteScoreChainOperation() throws OperationException {
        // Given
        final ScoreOperationChainHandler operationHandler = new ScoreOperationChainHandler();

        final Context context = mock(Context.class);
        final Store store = mock(Store.class);
        final User user = mock(User.class);
        final ScoreOperationChain scoreOperationChain = mock(ScoreOperationChain.class);

        StoreProperties storeProperties = new StoreProperties();

        final GetAdjacentIds op1 = mock(GetAdjacentIds.class);
        final GetElements op2 = mock(GetElements.class);
        final OperationChain opChain = new OperationChain(Arrays.asList(op1, op2));
        final Integer expectedResult = 2;

        given(context.getUser()).willReturn(user);
        Set<String> opAuths = new HashSet<>();
        opAuths.add("TEST_USER");
        given(user.getOpAuths()).willReturn(opAuths);
        given(scoreOperationChain.getOperationChain()).willReturn(opChain);
        given(store.getProperties()).willReturn(storeProperties);

        // When
        final Object result = operationHandler.doOperation(
                new ScoreOperationChain.Builder()
                        .operationChain(opChain)
                        .build(),
                context, store);

        // Then
        assertSame(expectedResult, result);
    }

    @Test
    public void shouldExecuteScoreChainOperationForNestedOperationChain() throws OperationException {
        // Given
        final ScoreOperationChainHandler operationHandler = new ScoreOperationChainHandler();

        final Context context = mock(Context.class);
        final Store store = mock(Store.class);
        final User user = mock(User.class);
        final ScoreOperationChain scoreOperationChain = mock(ScoreOperationChain.class);

        StoreProperties storeProperties = new StoreProperties();

        final GetAdjacentIds op1 = mock(GetAdjacentIds.class);
        final GetElements op2 = mock(GetElements.class);
        final Limit op3 = mock(Limit.class);
        final OperationChain opChain1 = new OperationChain(Arrays.asList(op1, op2));
        final OperationChain opChain = new OperationChain(Arrays.asList(opChain1, op3));
        final Integer expectedResult = 3;

        given(context.getUser()).willReturn(user);
        Set<String> opAuths = new HashSet<>();
        opAuths.add("TEST_USER");
        given(user.getOpAuths()).willReturn(opAuths);
        given(scoreOperationChain.getOperationChain()).willReturn(opChain);
        given(store.getProperties()).willReturn(storeProperties);

        // When
        final Object result = operationHandler.doOperation(
                new ScoreOperationChain.Builder()
                        .operationChain(opChain)
                        .build(),
                context, store);

        // Then
        assertSame(expectedResult, result);
    }

    @Test
    public void shouldSetAndGetAuthScores() {
        // Given
        final ScoreOperationChainHandler handler = new ScoreOperationChainHandler();
        final Map<String, Integer> authScores = new HashMap<>();
        authScores.put("auth1", 1);
        authScores.put("auth2", 2);
        authScores.put("auth3", 3);

        // When
        handler.setAuthScores(authScores);
        final Map<String, Integer> result = handler.getAuthScores();

        // Then
        assertEquals(authScores, result);
    }

    @Test
    public void shouldSetAndGetOpScores() {
        // Given
        final ScoreOperationChainHandler handler = new ScoreOperationChainHandler();
        final LinkedHashMap<Class<? extends Operation>, Integer> opScores = new LinkedHashMap<>();
        opScores.put(Operation.class, 1);
        opScores.put(GetElements.class, 2);
        opScores.put(GetAllElements.class, 3);

        // When
        handler.setOpScores(opScores);
        final Map<Class<? extends Operation>, Integer> result = handler.getOpScores();

        // Then
        assertEquals(opScores, result);
    }

    @Test
    public void shouldSetAndGetOpScoresAsStrings() throws ClassNotFoundException {
        // Given
        final ScoreOperationChainHandler handler = new ScoreOperationChainHandler();
        final LinkedHashMap<String, Integer> opScores = new LinkedHashMap<>();
        opScores.put(Operation.class.getName(), 1);
        opScores.put(GetElements.class.getName(), 2);
        opScores.put(GetAllElements.class.getName(), 3);

        // When
        handler.setOpScoresFromStrings(opScores);
        final Map<String, Integer> result = handler.getOpScoresAsStrings();

        // Then
        assertEquals(opScores, result);
    }

    @Test
    public void shouldPassValidationOfOperationScores() throws ClassNotFoundException {
        // Given
        final ScoreOperationChainHandler handler = new ScoreOperationChainHandler();
        final LinkedHashMap<String, Integer> opScores = new LinkedHashMap<>();
        opScores.put(Operation.class.getName(), 1);
        opScores.put(GetElements.class.getName(), 2);
        opScores.put(GetAllElements.class.getName(), 3);

        // When
        handler.setOpScoresFromStrings(opScores);

        // Then - no exceptions
    }

    @Test
    public void shouldFailValidationOfOperationScores() throws ClassNotFoundException {
        // Given
        final ScoreOperationChainHandler handler = new ScoreOperationChainHandler();
        final LinkedHashMap<String, Integer> opScores = new LinkedHashMap<>();
        opScores.put(GetElements.class.getName(), 2);
        opScores.put(GetAllElements.class.getName(), 3);
        opScores.put(Operation.class.getName(), 1);

        // When / Then
        try {
            handler.setOpScoresFromStrings(opScores);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Operation scores are configured incorrectly."));
        }
    }
}