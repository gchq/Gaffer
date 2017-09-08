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
    @Test
    public void shouldThrow() throws Exception {
        try {
            new FederatedAccessException();
            Assert.fail("Exception not thrown");
        } catch (FederatedAccessException e) {
            Assert.assertNull(e.getMessage());
        }
    }

    @Test
    public void shouldThrowWithString() throws Exception {
        final String hello = "hello";
        try {
            new FederatedAccessException(hello);
            Assert.fail("Exception not thrown");
        } catch (FederatedAccessException e) {
            assertEquals(hello, e.getMessage());
        }
    }

    @Test
    public void shouldThrowWithStringCause() throws Exception {
        final String hello = "hello";
        final String operation = "operation";
        try {
            new FederatedAccessException(hello, new OperationException(operation));
            Assert.fail("Exception not thrown");
        } catch (FederatedAccessException e) {
            assertEquals(hello, e.getMessage());
            assertEquals(operation, e.getCause().getMessage());
        }
    }

    @Test
    public void shouldThrowWithCause() throws Exception {
        final String operation = "operation";
        try {
            new FederatedAccessException(new OperationException(operation));
            Assert.fail("Exception not thrown");
        } catch (FederatedAccessException e) {
            assertEquals(operation, e.getCause().getMessage());
        }
    }

    @Test
    public void shouldThrowWithOther() throws Exception {
        final String hello = "hello";
        try {
            new FederatedAccessException(hello, new OperationException("operation"), false, false);
            Assert.fail("Exception not thrown");
        } catch (FederatedAccessException e) {
            assertEquals(hello, e.getMessage());
        }
    }


}