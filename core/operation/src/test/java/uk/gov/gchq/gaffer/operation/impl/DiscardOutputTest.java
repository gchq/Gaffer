/*
 * Copyright 2016-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.impl;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.operation.OperationTest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertNotSame;

public class DiscardOutputTest extends OperationTest<DiscardOutput> {

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        final DiscardOutput discardOutput = new DiscardOutput.Builder().input("1").build();

        assertThat(discardOutput.getInput(), is(nullValue()));
    }

    @Test
    @Override
    public void shouldShallowCloneOperation() {
        final DiscardOutput op = getTestObject();

        final DiscardOutput clone = op.shallowClone();

        assertNotSame(op, clone);
    }

    @Override
    protected DiscardOutput getTestObject() {
        return new DiscardOutput();
    }
}
