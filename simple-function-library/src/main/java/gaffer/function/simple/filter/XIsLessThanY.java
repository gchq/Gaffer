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
package gaffer.function.simple.filter;

import gaffer.function.FilterFunction;
import gaffer.function.annotation.Inputs;

/**
 * This class checks that the first value is less than the second value. Both values must be the same class.
 */
@Inputs({Comparable.class, Comparable.class})
public class XIsLessThanY extends FilterFunction {
    public XIsLessThanY statelessClone() {
        return new XIsLessThanY();
    }

    @Override
    protected boolean _isValid(final Object[] input) {
        return !(null == input
                || input.length != 2
                || null == input[0]
                || null == input[1]
                || !(input[0] instanceof Comparable)
                || input[0].getClass() != input[1].getClass())
                && ((Comparable) input[0]).compareTo(input[1]) < 0;

    }
}
