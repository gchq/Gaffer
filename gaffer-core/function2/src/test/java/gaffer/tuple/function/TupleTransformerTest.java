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

package gaffer.tuple.function;

import gaffer.function2.StatelessFunction;
import gaffer.function2.mock.MockTransform;
import gaffer.tuple.MapTuple;
import gaffer.tuple.Tuple;
import gaffer.tuple.function.context.FunctionContext;
import gaffer.tuple.view.Reference;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TupleTransformerTest {
    @Test
    public void testSingleInputTransformation() {
        TupleTransformer<String> transformer = new TupleTransformer<>();
        FunctionContext<StatelessFunction, String> fc = new FunctionContext<>();
        fc.setSelection(new Reference<String>("a"));
        fc.setProjection(new Reference<String>("b", "c"));
        MockTransform function = new MockTransform();
        Object output = "testOutput";
        function.setOutput(output);
        fc.setFunction(function);
        transformer.addFunction(fc);

        Tuple<String> inputTuple = new MapTuple<>();
        Object input = "testInput";
        inputTuple.put("a", input);

        Tuple<String> outputTuple = transformer.execute(inputTuple);

        assertEquals(input, outputTuple.get("a"));
        assertEquals(input, outputTuple.get("b"));
        assertEquals(output, outputTuple.get("c"));
    }
}
