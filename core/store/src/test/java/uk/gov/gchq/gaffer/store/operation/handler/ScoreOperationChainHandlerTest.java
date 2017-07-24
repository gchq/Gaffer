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
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.ScoreOperationChain;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.operationdeclaration.OperationDeclarations;
import uk.gov.gchq.gaffer.user.User;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;


public class ScoreOperationChainHandlerTest {
    private final JSONSerialiser json = new JSONSerialiser();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldLoadFromScoreOperationChainDeclarationFile() throws SerialisationException {
        final InputStream s = StreamUtil.openStream(getClass(), "TestScoreOperationChainDeclaration.json");
        final OperationDeclarations deserialised = json.deserialise(s, OperationDeclarations.class);

        assertEquals(1, deserialised.getOperations().size());
        assert (deserialised.getOperations().get(0).getHandler() instanceof ScoreOperationChainHandler);
    }

    @Test
    public void shouldExecuteScoreChainOperation
            () throws OperationException {
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
}