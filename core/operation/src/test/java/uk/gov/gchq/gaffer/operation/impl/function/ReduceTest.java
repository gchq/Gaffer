/*
 * Copyright 2018-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.impl.function;

import com.google.common.collect.Sets;

import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.impl.Reduce;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;

import java.util.Arrays;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

public class ReduceTest extends OperationTest<Reduce> {
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final Iterable<Integer> input = Arrays.asList(1, 2, 3);

        final Reduce<Integer> reduce = new Reduce.Builder<Integer>()
                .input(input)
                .identity(0)
                .aggregateFunction(new Sum())
                .build();

        // Then
        assertNotNull(reduce.getInput());
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final Iterable<Integer> input = Arrays.asList(1, 2, 3);

        final Reduce<Integer> reduce = new Reduce.Builder<Integer>()
                .input(input)
                .identity(0)
                .aggregateFunction(new Sum())
                .build();

        // When
        final Reduce<Integer> clone = reduce.shallowClone();

        // Then
        assertNotSame(reduce, clone);
        assertEquals(new Integer(1), clone.getInput().iterator().next());
    }

    @Override
    protected Reduce getTestObject() {
        return new Reduce();
    }

    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet("aggregateFunction");
    }
}
