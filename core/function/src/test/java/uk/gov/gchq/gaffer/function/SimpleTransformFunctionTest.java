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

package uk.gov.gchq.gaffer.function;

import uk.gov.gchq.gaffer.function.annotation.Inputs;
import uk.gov.gchq.gaffer.function.annotation.Outputs;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class SimpleTransformFunctionTest extends TransformFunctionTest {

    @Override
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // Given
        final ExampleSimpleTransformFunction function = new ExampleSimpleTransformFunction();

        // When
        final String json = serialise(function);

        // Then
        assertEquals("{\"class\":\"uk.gov.gchq.gaffer.function.SimpleTransformFunctionTest$ExampleSimpleTransformFunction\"}", json);
    }

    @Override
    protected Class<? extends Function> getFunctionClass() {
        return ExampleSimpleTransformFunction.class;
    }

    @Override
    protected TransformFunction getInstance() {
        return new ExampleSimpleTransformFunction();
    }

    @Inputs(String.class)
    @Outputs(String.class)
    private class ExampleSimpleTransformFunction extends SimpleTransformFunction<String> {

        ExampleSimpleTransformFunction() {
            // Empty
        }

        @Override
        protected String _transform(final String input) {
            return input;
        }

        @Override
        public TransformFunction statelessClone() {
            return new ExampleSimpleTransformFunction();
        }
    }
}
