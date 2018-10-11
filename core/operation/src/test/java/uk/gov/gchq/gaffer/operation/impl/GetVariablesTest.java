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

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class GetVariablesTest extends OperationTest<GetVariables> {
    final List<String> variableNames = Arrays.asList("var1", "var2", "var3");

    @Override
    public void builderShouldCreatePopulatedOperation() {
        GetVariables getVariablesOp = getTestObject();

        assertEquals(3, getVariablesOp.getVariableNames().size());
        assertTrue(getVariablesOp.getVariableNames().containsAll(variableNames));
    }

    @Override
    public void shouldShallowCloneOperation() {
        GetVariables op = getTestObject();

        GetVariables opClone = op.shallowClone();

        assertNotSame(op, opClone);
        assertEquals(op.getVariableNames(), opClone.getVariableNames());

    }

    @Override
    protected GetVariables getTestObject() {
        return new GetVariables.Builder()
                .variableNames(variableNames)
                .build();
    }
}
