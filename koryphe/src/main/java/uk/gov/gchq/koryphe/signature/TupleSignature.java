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

package uk.gov.gchq.koryphe.signature;

import uk.gov.gchq.koryphe.ValidationResult;
import java.util.Arrays;

/**
 * An <code>TupleSignature</code> is the type metadata for a tuple of values.
 */
public class TupleSignature extends Signature {
    private final Object input;
    private final Class[] classes;
    private final SingletonSignature[] types;

    TupleSignature(final Object input, final Class[] classes) {
        this.input = input;
        this.classes = classes;
        types = new SingletonSignature[classes.length];
        int i = 0;
        for (final Class clazz : classes) {
            types[i++] = new SingletonSignature(input, clazz);
        }
    }

    @Override
    public ValidationResult assignable(final boolean reverse, final Class<?>... arguments) {
        final ValidationResult result = new ValidationResult();
        if (types.length != arguments.length) {
            result.addError("Incompatible number of types. " + input.getClass() + ": " + Arrays.toString(types)
                    + ", arguments: " + Arrays.toString(arguments));
            return result;
        }

        int i = 0;
        for (final Class type : arguments) {
            result.add(types[i].assignable(reverse, type));
            i++;
        }
        return result;
    }

    @Override
    public Class[] getClasses() {
        return Arrays.copyOf(classes, classes.length);
    }
}
