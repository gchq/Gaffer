/*
 * Copyright 2017-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.operation.impl.export.set.ExportToSet;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OperationUtilTest {
    @Test
    public void shouldGetInputOutputTypes() {
        final GetElements operation = new GetElements();

        final Class<?> inputType = OperationUtil.getInputType(operation);
        assertEquals(Iterable.class, inputType);

        final Class<?> outputType = OperationUtil.getOutputType(operation);
        assertEquals(CloseableIterable.class, outputType);
    }

    @Test
    public void shouldCheckGenericInputOutputTypes() {
        final ExportToSet operation = new ExportToSet();

        final Class<?> inputType = OperationUtil.getInputType(operation);
        assertEquals(OperationUtil.UnknownGenericType.class, inputType);

        final Class<?> outputType = OperationUtil.getOutputType(operation);
        assertEquals(OperationUtil.UnknownGenericType.class, outputType);
    }


    @Test
    public void shouldValidateOutputInputTypes() {
        assertTrue(OperationUtil.isValid(Iterable.class, Iterable.class).isValid());
        assertTrue(OperationUtil.isValid(CloseableIterable.class, Iterable.class).isValid());
        assertTrue(OperationUtil.isValid(Iterable.class, Object.class).isValid());
        assertTrue(OperationUtil.isValid(OperationUtil.UnknownGenericType.class, Object.class).isValid());
        assertTrue(OperationUtil.isValid(Object.class, OperationUtil.UnknownGenericType.class).isValid());
        assertTrue(OperationUtil.isValid(OperationUtil.UnknownGenericType.class, OperationUtil.UnknownGenericType.class).isValid());

        assertFalse(OperationUtil.isValid(Object.class, CloseableIterable.class).isValid());
    }
}

