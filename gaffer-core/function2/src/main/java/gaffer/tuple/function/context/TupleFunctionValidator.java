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

package gaffer.tuple.function.context;

import gaffer.tuple.Tuple;

import java.util.List;

/**
 * Tests {@link gaffer.tuple.function.context.FunctionContext}s against a {@link gaffer.tuple.Tuple} of Classes to check
 * whether the types are compatible with the input and output required.
 */
public final class TupleFunctionValidator {
    private TupleFunctionValidator() { }

    public static boolean validateInput(final List<? extends FunctionContext> contexts, final Object schemaTuple) {
        if (!(schemaTuple instanceof Tuple)) {
            throw new IllegalArgumentException("Tuple functions must be supplied with tuple of types.");
        }
        Tuple tuple = (Tuple) schemaTuple;
        boolean valid = true;
        for (FunctionContext context : contexts) {
            Object selected = context.select(tuple);
            valid = valid && context.getFunction().validateInput(selected);
        }
        return valid;
    }

    public static boolean validateOutput(final List<? extends FunctionContext> contexts, final Object schemaTuple) {
        if (!(schemaTuple instanceof Tuple)) {
            throw new IllegalArgumentException("Tuple functions must be supplied with tuple of types.");
        }
        Tuple tuple = (Tuple) schemaTuple;
        boolean valid = true;
        for (FunctionContext context : contexts) {
            Object selected = context.getProjectionView().select(tuple);
            valid = valid && context.getFunction().validateOutput(selected);
        }
        return valid;
    }
}
