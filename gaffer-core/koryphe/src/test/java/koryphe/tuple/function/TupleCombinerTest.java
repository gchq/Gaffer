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

package koryphe.tuple.function;

import koryphe.function.combine.Combiner;
import koryphe.function.mock.MockCombiner;
import koryphe.tuple.Tuple;
import koryphe.tuple.mask.TupleMask;
import org.junit.Test;
import util.JsonSerialiser;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

public class TupleCombinerTest {
    @Test
    public void testTupleCombination() {
        String[] inputs = new String[]{"input1", "input2", "input3"};
        Set<String> output1 = new HashSet<>(Arrays.asList("input1"));
        Set<String> output2 = new HashSet<>(Arrays.asList("input1", "input2"));
        Set<String> output3 = new HashSet<>(Arrays.asList("input1", "input2", "input3"));
        List<Set<String>> outputsArray = new ArrayList<>();
        outputsArray.addAll(Arrays.asList(output1, output2, output3));
        Object[] outputs = outputsArray.toArray();

        TupleCombiner<String, String, Set<String>> combiner = new TupleCombiner<>();
        Tuple<String>[] tuples = new Tuple[]{mock(Tuple.class), mock(Tuple.class), mock(Tuple.class)};

        // set up the function
        Combiner<String, Set<String>> function1 = mock(Combiner.class);
        TupleMask<String, String> inputMask = mock(TupleMask.class);
        TupleMask<String, Set<String>> outputMask = mock(TupleMask.class);
        combiner.setFunction(function1);
        combiner.setSelection(inputMask);
        combiner.setProjection(outputMask);
        Tuple<String> state = null;
        for (int i = 0; i < tuples.length; i++) {
            Set<String> previousOutput = null;
            given(inputMask.select(tuples[i])).willReturn(inputs[i]);
            if (i > 0) {
                previousOutput = (Set<String>) outputs[i - 1];
                given(outputMask.select(state)).willReturn(previousOutput);
            }
            given(function1.execute(inputs[i], previousOutput)).willReturn((Set<String>) outputs[i]);
            given(outputMask.project(outputs[i])).willReturn(tuples[0]);
            state = combiner.execute(tuples[i], state);
        }

        // check the expected calls
        verify(outputMask, times(tuples.length - 1)).select(tuples[0]);
        for (int i = 0; i < tuples.length; i++) {
            String in1 = inputs[i];
            Set<String> in2 = null;
            if (i > 0) {
                in2 = (Set<String>) outputs[i - 1];

            }
            verify(inputMask, times(1)).select(tuples[i]);
            verify(function1, times(1)).execute(in1, in2);
            verify(outputMask, times(1)).project(outputs[i]);
        }
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        TupleCombiner<String, String, Set<String>> combiner = new TupleCombiner<>();
        MockCombiner function = new MockCombiner();
        TupleMask<String, String> inputMask = new TupleMask<>("a");
        TupleMask<String, Set<String>> outputMask = new TupleMask<>("b");
        combiner.setSelection(inputMask);
        combiner.setProjection(outputMask);
        combiner.setFunction(function);

        String json = JsonSerialiser.serialise(combiner);
        TupleCombiner<String, String, Set<String>> deserialisedCombiner = JsonSerialiser.deserialise(json, TupleCombiner.class);

        // check deserialisation
        assertNotSame(combiner, deserialisedCombiner);

        Combiner<String, Set<String>> deserialisedFunction = deserialisedCombiner.getFunction();
        assertNotSame(function, deserialisedFunction);
        assertTrue(deserialisedFunction instanceof MockCombiner);

        TupleMask<String, String> deserialisedInputMask = deserialisedCombiner.getSelection();
        assertNotSame(inputMask, deserialisedInputMask);
        assertTrue(deserialisedInputMask instanceof TupleMask);
        TupleMask<String, Set<String>> deserialisedOutputMask = deserialisedCombiner.getProjection();
        assertNotSame(outputMask, deserialisedOutputMask);
        assertTrue(deserialisedOutputMask instanceof TupleMask);
    }
}
