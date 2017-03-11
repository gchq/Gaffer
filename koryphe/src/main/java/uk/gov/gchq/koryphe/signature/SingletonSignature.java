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
 * A <code>SingletonSignature</code> is the type metadata for a single instance of a specific type.
 */
public class SingletonSignature extends Signature {
    private Class type;

    /**
     * Create a <code>SingletonSignature</code> with the given {@link Class}.
     *
     * @param type Class to test for.
     */
    SingletonSignature(final Class type) {
        this.type = type;
    }

    @Override
    public boolean assignable(final Object argument, final boolean reverse) {
        if ((argument instanceof Object[])) {
            final Object[] arguments = ((Object[]) argument);
            return 1 == arguments.length && assignable(arguments[0], reverse);
        }

        if (argument instanceof Class) {
            if (reverse) {
                return type == null ? false : ((Class) argument).isAssignableFrom(type);
            } else {
                return type == null ? false : type.isAssignableFrom((Class) argument);
            }
        } else {
            return false;
        }
    }

    @Override
    public Class[] getClasses() {
        return new Class[]{type};
    }
}
