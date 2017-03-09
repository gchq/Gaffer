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

package uk.gov.gchq.koryphe.tuple.function;

import org.junit.Test;
import uk.gov.gchq.koryphe.function.mock.MockFunction;
import uk.gov.gchq.koryphe.tuple.Tuple;
import uk.gov.gchq.koryphe.tuple.mask.TupleMask;
import uk.gov.gchq.koryphe.tuple.n.Tuple2;
import uk.gov.gchq.koryphe.tuple.n.value.Value2;
import util.JsonSerialiser;
import java.io.IOException;
import java.util.function.Function;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TupleFunctionTest {
    @Test
    public void testSingleFunctionTransformation() {
        String input1 = "input1";
        String output1a = "output1a";
        String output1b = "output1b";
        Tuple2<String, String> output1 = new Value2<>(output1a, output1b);

        Tuple<String> tuple = mock(Tuple.class);

        // set up a function
        TupleFunction<String, String, Tuple2<String, String>> function = new TupleFunction<>();
        Function<String, Tuple2<String, String>> function1 = mock(Function.class);
        TupleMask<String, String> inputAdapter = mock(TupleMask.class);
        TupleMask<String, Tuple2<String, String>> outputAdapter = mock(TupleMask.class);
        given(inputAdapter.select(tuple)).willReturn(input1);
        given(function1.apply(input1)).willReturn(output1);
        function.setFunction(function1);
        function.setSelection(inputAdapter);
        function.setProjection(outputAdapter);

        // apply it
        function.apply(tuple);

        // check it was called as expected
        verify(inputAdapter, times(1)).select(tuple);
        verify(function1, times(1)).apply(input1);
        verify(outputAdapter, times(1)).setContext(tuple);
        verify(outputAdapter, times(1)).project(output1);
    }

    @Test
    public void testMultiTupleTransformation() {
        String input = "input";
        String output = "output";

        TupleFunction<String, String, String> function = new TupleFunction<>();

        //create some tuples
        int times = 5;
        Tuple<String>[] tuples = new Tuple[times];
        for (int i = 0; i < times; i++) {
            tuples[i] = mock(Tuple.class);
        }

        // set up a function to transform the tuples
        Function<String, String> function1 = mock(Function.class);
        TupleMask<String, String> inputAdapter = mock(TupleMask.class);
        TupleMask<String, String> outputAdapter = mock(TupleMask.class);
        function.setFunction(function1);
        function.setSelection(inputAdapter);
        function.setProjection(outputAdapter);
        for (int i = 0; i < times; i++) {
            given(inputAdapter.select(tuples[i])).willReturn(input + i);
            given(function1.apply(input + i)).willReturn(output + i);
        }

        // apply transformations
        for (int i = 0; i < times; i++) {
            function.apply(tuples[i]);
        }

        // check expected calls
        for (int i = 0; i < times; i++) {
            verify(inputAdapter, times(1)).select(tuples[i]);
            verify(function1, times(1)).apply(input + i);
            verify(outputAdapter, times(1)).setContext(tuples[i]);
            verify(outputAdapter, times(1)).project(output + i);
        }
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        TupleFunction<String, Object, Object> function = new TupleFunction<>();
        MockFunction mockFunction = new MockFunction();
        function.setFunction(mockFunction);
        TupleMask<String, Object> inputAdapter = new TupleMask<>("a");
        TupleMask<String, Object> outputAdapter = new TupleMask<>("b");
        function.setSelection(inputAdapter);
        function.setProjection(outputAdapter);

        String json = JsonSerialiser.serialise(function);
        TupleFunction<String, Object, Object> deserialisedFunction = JsonSerialiser.deserialise(json, TupleFunction.class);
        assertNotSame(function, deserialisedFunction);

        Function<Object, Object> functionCopy = deserialisedFunction.getFunction();
        assertNotSame(function, functionCopy);
        assertTrue(functionCopy instanceof MockFunction);

        TupleMask<String, Object> inputAdapterCopy = deserialisedFunction.getSelection();
        TupleMask<String, Object> outputAdapterCopy = deserialisedFunction.getProjection();
        assertNotSame(inputAdapter, inputAdapterCopy);
        assertTrue(inputAdapterCopy instanceof TupleMask);
        assertNotSame(outputAdapter, outputAdapterCopy);
        assertTrue(outputAdapterCopy instanceof TupleMask);
    }
}
