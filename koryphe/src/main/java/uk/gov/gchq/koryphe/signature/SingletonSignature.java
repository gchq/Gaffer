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
 * A <code>SingletonSignature</code> is the type metadata for a single instance of a specific type.
 */
public class SingletonSignature extends Signature {
    private final Object input;
    private Class<?> type;

    SingletonSignature(final Object input, final Class type) {
        this.input = input;
        this.type = type;
    }

    @Override
    public ValidationResult assignable(final boolean reverse, final Class<?>... arguments) {
        final ValidationResult result = new ValidationResult();
        if (type == null) {
            result.addError("Type could not be extracted from function " + input.getClass());
            return result;
        }

        if (arguments.length != 1 || null == arguments[0]) {
            result.addError("Incompatible number of types. " + input.getClass() + ": [" + type
                    + "], arguments: " + Arrays.toString(arguments));
            return result;
        }

        final boolean isAssignable;
        if (reverse) {
            isAssignable = arguments[0].isAssignableFrom(type);
        } else {
            isAssignable = type.isAssignableFrom(arguments[0]);
        }

        if (!isAssignable) {
            result.addError("Incompatible types. " + input.getClass() + ": [" + type
                    + "], arguments: " + Arrays.toString(arguments));
        }
        return result;
    }

    @Override
    public Class[] getClasses() {
        return new Class[]{type};
    }
}
