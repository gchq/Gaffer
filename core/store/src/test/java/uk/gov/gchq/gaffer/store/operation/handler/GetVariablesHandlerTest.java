/*
 * Copyright 2018 Crown Copyright
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

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.GetVariables;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class GetVariablesHandlerTest {
    private final Store store = mock(Store.class);
    private final String key1 = "key1";
    private final String val1 = "val1";
    private final String key2 = "key2";
    private final String val2 = "val2";
    private final String key3 = "key3";
    private final String val3 = "val3";


    @Test
    public void shouldGetAllVariableValuesWhenAllPresent() throws OperationException {
        final Context context = mock(Context.class);
        given(context.getVariable(key1)).willReturn(val1);
        given(context.getVariable(key2)).willReturn(val2);
        given(context.getVariable(key3)).willReturn(val3);

        final GetVariables op = new GetVariables.Builder().variableNames(Arrays.asList(key1, key2, key3)).build();

        final GetVariablesHandler handler = new GetVariablesHandler();

        Map<String, Object> resultMap = handler.doOperation(op, context, store);

        assertEquals(ImmutableMap.of(key1, val1, key2, val2, key3, val3), resultMap);
    }

    @Test
    public void shouldReturnEmptyMapWhenNoValuesPresent() throws OperationException {
        final Context context = mock(Context.class);
        given(context.getVariable(key1)).willReturn(null);
        given(context.getVariable(key2)).willReturn(null);
        given(context.getVariable(key3)).willReturn(null);

        Map expected = new HashMap<>();
        expected.put(key1, null);
        expected.put(key2, null);
        expected.put(key3, null);

        final GetVariables op = new GetVariables.Builder().variableNames(Arrays.asList(key1, key2, key3)).build();

        final GetVariablesHandler handler = new GetVariablesHandler();

        Map<String, Object> resultMap = handler.doOperation(op, context, store);

        assertEquals(expected, resultMap);
    }

    @Test
    public void shouldReturnPartiallyFilledMapWhenSomeValuesPresent() throws OperationException {
        final Context context = mock(Context.class);
        given(context.getVariable(key1)).willReturn(val1);
        given(context.getVariable(key2)).willReturn(null);
        given(context.getVariable(key3)).willReturn(val3);

        Map expected = new HashMap<>();
        expected.put(key1, val1);
        expected.put(key2, null);
        expected.put(key3, val3);

        final GetVariables op = new GetVariables.Builder().variableNames(Arrays.asList(key1, key2, key3)).build();

        final GetVariablesHandler handler = new GetVariablesHandler();

        Map<String, Object> resultMap = handler.doOperation(op, context, store);

        assertEquals(expected, resultMap);
    }
}
