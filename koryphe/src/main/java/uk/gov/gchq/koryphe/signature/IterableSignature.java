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
public class IterableSignature extends Signature {
    private Signature iterableSignature;

    /**
     * Create an <code>IterableSignature</code> for values with the given {@link Signature}.
     * @param iterableSignature Signature of the iterable values.
     */
    IterableSignature(final Signature iterableSignature) {
        this.iterableSignature = iterableSignature;
    }

    @Override
    public boolean assignable(final Object arguments, final boolean reverse) {
        if (arguments instanceof Iterable) {
            for (Object type : (Iterable) arguments) {
                boolean compatible = iterableSignature.assignable(type, reverse);
                if (!compatible) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }
}
