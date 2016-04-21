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

package gaffer.function2.signature;

import gaffer.tuple.Tuple;

import java.util.List;

/**
 * A <code>TupleNSignature</code> is the type metadata for a tuple of fixed length and types. For example, a
 * <code>Tuple3&lt;String, Integer, Long&gt;</code> is represented by a <code>TupleNSignature</code> containing a list
 * of 3 {@link SingletonSignature}s, for the String, Integer and Long values it contains.
 */
public class TupleNSignature extends Signature {
    private List<Signature> signatures;

    /**
     * Create a <code>TupleNSignature</code> with the given {@link Signature}s.
     * @param signatures Signatures of the Tuple values.
     */
    TupleNSignature(final List<Signature> signatures) {
        this.signatures = signatures;
    }

    @Override
    public boolean assignable(final Object arguments, final boolean reverse) {
        if (arguments instanceof Tuple) {
            int i = 0;
            for (Object argument : (Iterable) arguments) {
                if (signatures.size() == i) {
                    return false; // too many args
                }
                Signature signature = signatures.get(i++);
                boolean compatible = signature.assignable(argument, reverse);
                if (!compatible) {
                    return false;
                }
            }
            if (signatures.size() > i) {
                return false; // too few args
            }
            return true;
        } else {
            return false;
        }
    }
}
