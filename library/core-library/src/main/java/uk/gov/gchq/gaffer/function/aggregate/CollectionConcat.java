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
package uk.gov.gchq.gaffer.function.aggregate;

import uk.gov.gchq.koryphe.binaryoperator.KorpheBinaryOperator;
import java.util.Collection;
import java.util.function.BinaryOperator;

/**
 * An <code>CollectionConcat</code> is a {@link BinaryOperator} that concatenates
 * {@link Collection}s together.
 */
public class CollectionConcat<T> extends KorpheBinaryOperator<Collection<T>> {

    @Override
    public Collection<T> apply(final Collection<T> input1, final Collection<T> input2) {
        if (null == input1) {
            return input2;
        }

        if (null == input2) {
            return input1;
        }

        final Collection<T> result;
        try {
            result = input1.getClass().newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new IllegalArgumentException("Unable to create new instance of " + input1.getClass().getName()
                    + ". This collection aggregator can only be used on collections with a default constructor.", e);
        }

        result.addAll(input1);
        result.addAll(input2);

        return result;
    }
}
