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
import uk.gov.gchq.koryphe.function.MockFunction;
import uk.gov.gchq.koryphe.tuple.Tuple;
import uk.gov.gchq.koryphe.tuple.TupleInputAdapter;
import uk.gov.gchq.koryphe.tuple.TupleOutputAdapter;
import uk.gov.gchq.koryphe.tuple.n.Tuple2;
import uk.gov.gchq.koryphe.util.JsonSerialiser;
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
        Tuple2<String, String> output1 = new Tuple2<>(output1a, output1b);

        Tuple<String> tuple = mock(Tuple.class);

        // set up a function
        TupleAdaptedFunction<String, String, Tuple2<String, String>> function = new TupleAdaptedFunction<>();

        Function<String, Tuple2<String, String>> function1 = mock(Function.class);
        TupleInputAdapter<String, String> inputAdapter = mock(TupleInputAdapter.class);
        TupleOutputAdapter<String, Tuple2<String, String>> outputAdapter = mock(TupleOutputAdapter.class);

        given(inputAdapter.apply(tuple)).willReturn(input1);
        given(function1.apply(input1)).willReturn(output1);

        function.setFunction(function1);
        function.setInputAdapter(inputAdapter);
        function.setOutputAdapter(outputAdapter);

        // apply it
        function.apply(tuple);

        // check it was called as expected
        verify(inputAdapter, times(1)).apply(tuple);
        verify(function1, times(1)).apply(input1);
        verify(outputAdapter, times(1)).apply(output1, tuple);
    }

    @Test
    public void testMultiTupleTransformation() {
        String input = "input";
        String output = "output";

        TupleAdaptedFunction<String, String, String> function = new TupleAdaptedFunction<>();

        //create some tuples
        int times = 5;
        Tuple<String>[] tuples = new Tuple[times];
        for (int i = 0; i < times; i++) {
            tuples[i] = mock(Tuple.class);
        }

        // set up a function to transform the tuples
        Function<String, String> function1 = mock(Function.class);
        TupleInputAdapter<String, String> inputAdapter = mock(TupleInputAdapter.class);
        TupleOutputAdapter<String, String> outputAdapter = mock(TupleOutputAdapter.class);
        function.setFunction(function1);
        function.setInputAdapter(inputAdapter);
        function.setOutputAdapter(outputAdapter);
        for (int i = 0; i < times; i++) {
            given(inputAdapter.apply(tuples[i])).willReturn(input + i);
            given(function1.apply(input + i)).willReturn(output + i);
        }

        // apply transformations
        for (int i = 0; i < times; i++) {
            function.apply(tuples[i]);
        }

        // check expected calls
        for (int i = 0; i < times; i++) {
            verify(inputAdapter, times(1)).apply(tuples[i]);
            verify(function1, times(1)).apply(input + i);
            verify(outputAdapter, times(1)).apply(output + i, tuples[i]);
        }
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        TupleAdaptedFunction<String, Object, String> function = new TupleAdaptedFunction<>();
        MockFunction mockFunction = new MockFunction();
        function.setFunction(mockFunction);
        TupleInputAdapter<String, Object> inputAdapter = new TupleInputAdapter<>();
        TupleOutputAdapter<String, String> outputAdapter = new TupleOutputAdapter<>();
        function.setInputAdapter(inputAdapter);
        function.setOutputAdapter(outputAdapter);

        String json = JsonSerialiser.serialise(function);
        TupleAdaptedFunction<String, Object, String> deserialisedFunction = JsonSerialiser
                .deserialise(json, TupleAdaptedFunction.class);
        assertNotSame(function, deserialisedFunction);

        Function<Object, String> functionCopy = deserialisedFunction.getFunction();
        assertNotSame(function, functionCopy);
        assertTrue(functionCopy instanceof MockFunction);

        TupleInputAdapter<String, Object> inputAdapterCopy = deserialisedFunction
                .getInputAdapter();
        TupleOutputAdapter<String, String> outputAdapterCopy = deserialisedFunction
                .getOutputAdapter();
        assertNotSame(inputAdapter, inputAdapterCopy);
        assertTrue(inputAdapterCopy instanceof Function);
        assertNotSame(outputAdapter, outputAdapterCopy);
        assertTrue(outputAdapterCopy instanceof TupleOutputAdapter);
    }
}
