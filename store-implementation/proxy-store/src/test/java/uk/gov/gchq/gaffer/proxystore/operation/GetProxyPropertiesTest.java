/*
 * Copyright 2021 Crown Copyright
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
package uk.gov.gchq.gaffer.proxystore.operation;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class GetProxyPropertiesTest extends OperationTest<GetProxyProperties> {

    private static final String A = "a";
    private static final String ONE = "1";

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        //given
        GetProxyProperties op = getTestObject();

        //when
        String actual = op.getOption(A);

        //then
        assertEquals(ONE, actual);
    }

    @Test
    @Override
    public void shouldShallowCloneOperation() {
        GetProxyProperties testObject = getTestObject();
        Operation operation = testObject.shallowClone();
        assertEquals(testObject, operation);
        assertFalse(testObject == operation);
    }

    @Override
    protected GetProxyProperties getTestObject() {
        String expected = ONE;
        String a = A;
        return new GetProxyProperties.Builder().option(a, expected).build();
    }
}
