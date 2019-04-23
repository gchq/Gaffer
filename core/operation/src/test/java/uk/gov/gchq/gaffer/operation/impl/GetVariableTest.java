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

package uk.gov.gchq.gaffer.operation.impl;

import uk.gov.gchq.gaffer.operation.OperationTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class GetVariableTest extends OperationTest<GetVariable> {
    private final String varName = "varName";

    @Override
    public void builderShouldCreatePopulatedOperation() {
        final GetVariable getVariableOp = new GetVariable.Builder().variableName(varName).build();

        assertEquals(varName, getVariableOp.getVariableName());
    }

    @Override
    public void shouldShallowCloneOperation() {
        final GetVariable op = new GetVariable.Builder().variableName(varName).build();

        final GetVariable opClone = op.shallowClone();

        assertNotEquals(op, opClone);
        assertEquals(op.getVariableName(), opClone.getVariableName());
    }

    @Override
    protected GetVariable getTestObject() {
        return new GetVariable();
    }
}
