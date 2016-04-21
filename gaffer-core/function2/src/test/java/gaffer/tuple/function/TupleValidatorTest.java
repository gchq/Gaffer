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

import gaffer.function2.Validator;
import gaffer.function2.mock.MockValidator;
import gaffer.tuple.MapTuple;
import gaffer.tuple.Tuple;
import gaffer.tuple.function.context.FunctionContext;
import gaffer.tuple.view.Reference;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TupleValidatorTest {
    @Test
    public void testSimpleValidation() {
        TupleValidator<String> validator = new TupleValidator<>();
        FunctionContext<Validator, String> fc = new FunctionContext<>();
        fc.setSelection(new Reference<String>("a"));
        fc.setFunction(new MockValidator());
        validator.addFunction(fc);

        Tuple<String> input = new MapTuple<>();
        input.put("a", Boolean.TRUE);

        assertTrue(validator.execute(input));

        input.put("a", Boolean.FALSE);

        assertFalse(validator.execute(input));
    }
}
