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

/**
 * An <code>IterableSignature</code> is the type metadata for an {@link Iterable} of values.
 */
public class TupleSignature extends Signature {
    private final Class[] classes;
    private final Signature[] types;

    TupleSignature(final Class[] classes) {
        this.classes = classes;
        types = new Signature[classes.length];
        int i = 0;
        for (Class clazz : classes) {
            types[i++] = new SingletonSignature(clazz);
        }

    }

    @Override
    public boolean assignable(final Object argument, final boolean reverse) {
        if (!(argument instanceof Object[])) {
            return false;
        }

        final Object[] arguments = ((Object[]) argument);
        if (types.length != arguments.length) {
            return false;
        }

        int i = 0;
        for (Object type : arguments) {
            boolean compatible = types[i].assignable(type, reverse);
            i++;
            if (!compatible) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Class[] getClasses() {
        return classes;
    }
}
