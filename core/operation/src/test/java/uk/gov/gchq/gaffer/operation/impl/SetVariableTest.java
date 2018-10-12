/*
 * Copyright 2018 Crown Copyright
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
import static org.junit.Assert.assertNotSame;

public class SetVariableTest extends OperationTest<SetVariable> {
    private final String varName = "varName";
    private final int varVal = 4;
    @Override
    public void builderShouldCreatePopulatedOperation() {
        final SetVariable setVariableOp = getTestObject();

        assertEquals(varName, setVariableOp.getVariableName());
        assertEquals(varVal, setVariableOp.getInput());
    }

    @Override
    public void shouldShallowCloneOperation() {
        final SetVariable op = getTestObject();

        final SetVariable opClone = op.shallowClone();

        assertNotSame(op, opClone);
        assertEquals(op.getVariableName(), opClone.getVariableName());
        assertEquals(op.getInput(), opClone.getInput());
    }

    @Override
    protected SetVariable getTestObject() {
        return new SetVariable.Builder()
                .variableName("varName")
                .input(varVal)
                .build();
    }
}
