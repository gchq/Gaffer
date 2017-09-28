/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore;

import org.junit.Assert;
import org.junit.Test;

import uk.gov.gchq.gaffer.operation.OperationException;

import static org.junit.Assert.assertEquals;

public class FederatedAccessExceptionTest {

    private static final String HELLO = "hello";
    private static final String OPERATION = "operation";

    @Test
    public void shouldThrow() throws Exception {
        try {
            throw new FederatedAccessException();
        } catch (FederatedAccessException e) {
            Assert.assertNull(e.getMessage());
        }
    }

    @Test
    public void shouldThrowWithString() throws Exception {
        try {
            throw new FederatedAccessException(HELLO);
        } catch (FederatedAccessException e) {
            assertEquals(HELLO, e.getMessage());
        }
    }

    @Test
    public void shouldThrowWithStringCause() throws Exception {
        try {
            throw new FederatedAccessException(HELLO, new OperationException(OPERATION));
        } catch (FederatedAccessException e) {
            assertEquals(HELLO, e.getMessage());
            assertEquals(OPERATION, e.getCause().getMessage());
        }
    }

    @Test
    public void shouldThrowWithCause() throws Exception {
        try {
            throw new FederatedAccessException(new OperationException(OPERATION));
        } catch (FederatedAccessException e) {
            assertEquals(OPERATION, e.getCause().getMessage());
        }
    }

    @Test
    public void shouldThrowWithOther() throws Exception {
        try {
            throw new FederatedAccessException(HELLO, new OperationException(OPERATION), false, false);
        } catch (FederatedAccessException e) {
            assertEquals(HELLO, e.getMessage());
        }
    }


}