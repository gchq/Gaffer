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

package uk.gov.gchq.gaffer.commonutil.function;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.koryphe.function.FunctionTest;

import java.io.IOException;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ToSingletonTreeSetTest extends FunctionTest<ToSingletonTreeSet> {

    @Test
    void shouldCreateATreeSetWithSingleObjectInside() {
        // Given
        final ToSingletonTreeSet toSingletonTreeSet = new ToSingletonTreeSet();
        TreeSet<Object> expected = new TreeSet<>();
        expected.add("input");
        // When
        TreeSet<Object> result = toSingletonTreeSet.apply("input");
        // Then
        assertEquals(expected, result);
    }

    @Override
    protected Class[] getExpectedSignatureInputClasses() {
        return new Class[]{Object.class};
    }

    @Override
    protected Class[] getExpectedSignatureOutputClasses() {
        return new Class[]{TreeSet.class};
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // TODO

    }

    @Override
    protected ToSingletonTreeSet getInstance() {
        return new ToSingletonTreeSet();
    }

    @Override
    protected Iterable getDifferentInstancesOrNull() {
        return null;
    }
}
