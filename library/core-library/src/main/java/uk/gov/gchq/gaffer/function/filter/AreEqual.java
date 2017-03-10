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
package uk.gov.gchq.gaffer.function.filter;

import uk.gov.gchq.koryphe.predicate.KoryphePredicate2;

/**
 * An <code>AreEqual</code> is a {@link java.util.function.BiPredicate} that returns true if the two input objects
 * are equal.
 */
public class AreEqual extends KoryphePredicate2<Object, Object> {
    /**
     * @param input1 input 1
     * @param input2 input 2
     * @return true if either the input array is null, it has only 1 item or the first 2 items are equal. Otherwise false.
     */
    @Override
    public boolean test(final Object input1, final Object input2) {
        if (null == input1) {
            return null == input2;
        }

        return input1.equals(input2);
    }
}
