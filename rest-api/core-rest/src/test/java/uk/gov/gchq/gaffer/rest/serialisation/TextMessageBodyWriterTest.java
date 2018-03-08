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
package uk.gov.gchq.gaffer.rest.serialisation;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import java.io.IOException;
import java.io.OutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TextMessageBodyWriterTest {
    @Test
    public void shouldHandleAllObjectTypes() {
        // Given
        final TextMessageBodyWriter writer = new TextMessageBodyWriter();

        // When / Then
        assertTrue(writer.isWriteable(null, null, null, null));
        assertTrue(writer.isWriteable(String.class, null, null, null));
        assertTrue(writer.isWriteable(Object.class, null, null, null));
    }

    @Test
    public void shouldReturnSize0() {
        // Given
        final TextMessageBodyWriter writer = new TextMessageBodyWriter();

        // When
        final long size = writer.getSize(null, null, null, null, null);

        // Then
        assertEquals(0L, size);
    }

    @Test
    public void shouldSerialiseObjectToJsonAndWrite() throws IOException {
        // Given
        final TextMessageBodyWriter writer = new TextMessageBodyWriter();
        final Object object = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex("vertex1")
                .property(TestPropertyNames.COUNT, 1)
                .build();
        final OutputStream outputStream = mock(OutputStream.class);

        // When
        writer.writeTo(object, null, null, null, null, null, outputStream);

        // Then
        InOrder inOrder = Mockito.inOrder(outputStream);
        final ArgumentCaptor<byte[]> bytesCaptor = ArgumentCaptor.forClass(byte[].class);
        inOrder.verify(outputStream).write(bytesCaptor.capture());
        inOrder.verify(outputStream).flush();
        inOrder.verify(outputStream).close();

        JsonAssert.assertEquals(JSONSerialiser.serialise(object), bytesCaptor.getValue());
    }
}
