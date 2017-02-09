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

import uk.gov.gchq.gaffer.function.FilterFunction;
import uk.gov.gchq.gaffer.function.annotation.Inputs;

/**
 * An <code>AreEqual</code> is a {@link FilterFunction} that returns true if the two input objects
 * are equal.
 */
@Inputs({ Object.class, Object.class })
public class AreEqual extends FilterFunction {

    @Override
    public AreEqual statelessClone() {
        return new AreEqual();
    }

    /**
     * @param input the input objects
     * @return true if either the input array is null, it has only 1 item or the first 2 items are equal. Otherwise false.
     */
    @Override
    public boolean isValid(final Object[] input) {
        if (null == input) {
            return true;
        }

        int inputLength = input.length;
        if (inputLength < 2) {
            return true;
        }

        if (null == input[0]) {
            return null == input[1];
        }

        return input[0].equals(input[1]);
    }
}
