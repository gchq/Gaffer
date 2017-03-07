/*
 * Copyright 2016 Crown Copyright
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

package uk.gov.gchq.gaffer.jsonSerialisation;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.serialisation.ParameterisedTestObject;
import uk.gov.gchq.gaffer.serialisation.SimpleTestObject;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class JSONSerialiserTest {

    private JSONSerialiser serialiser = null;

    @Before
    public void setupTest() throws SerialisationException {
        serialiser = new JSONSerialiser();
    }

    @Test
    public void shouldConstructWithCustomObjectMapper() throws IOException {
        // Given
        final ObjectMapper objectMapper = mock(ObjectMapper.class);

        // When
        final JSONSerialiser jsonSerialiser = new JSONSerialiser(objectMapper);

        // Then
        assertSame(objectMapper, jsonSerialiser.getMapper());
    }

    @Test
    public void testPrimitiveSerialisation() throws IOException {
        byte[] b = serialiser.serialise(2);
        Object o = serialiser.deserialise(b, Object.class);
        assertEquals(Integer.class, o.getClass());
        assertEquals(2, o);
    }

    @Test
    public void canHandleUnParameterisedDAO() throws SerialisationException {
        assertTrue(serialiser.canHandle(SimpleTestObject.class));
    }

    @Test
    public void testDAOSerialisation() throws SerialisationException {
        SimpleTestObject test = new SimpleTestObject();
        test.setX("Test");
        byte[] b = serialiser.serialise(test);
        Object o = serialiser.deserialise(b, SimpleTestObject.class);
        assertEquals(SimpleTestObject.class, o.getClass());
        assertEquals("Test", ((SimpleTestObject) o).getX());
    }

    @Test
    public void shouldNotPrettyPrintByDefaultWhenSerialising() throws SerialisationException {
        SimpleTestObject test = new SimpleTestObject();
        test.setX("TestValue1");
        byte[] bytes = serialiser.serialise(test);
        assertEquals("{\"x\":\"TestValue1\"}", new String(bytes));
    }

    @Test
    public void shouldPrettyPrintWhenSerialisingAndSetToPrettyPrint() throws SerialisationException {
        SimpleTestObject test = new SimpleTestObject();
        test.setX("TestValue1");
        byte[] bytes = serialiser.serialise(test, true);
        JsonUtil.assertEquals(String.format("{%n  \"x\" : \"TestValue1\"%n}"), new String(bytes));
    }

    @Test
    public void canHandleParameterisedDAO() throws SerialisationException {
        assertTrue(serialiser.canHandle(ParameterisedTestObject.class));
    }

    @Test
    public void testParameterisedDAOSerialisation() throws SerialisationException {
        ParameterisedTestObject<Integer> test = new ParameterisedTestObject<>();
        test.setX("Test");
        test.setK(2);
        byte[] b = serialiser.serialise(test);
        Object o = serialiser.deserialise(b, ParameterisedTestObject.class);
        assertEquals(ParameterisedTestObject.class, o.getClass());
        assertEquals("Test", ((ParameterisedTestObject) o).getX());
        assertEquals(Integer.class, ((ParameterisedTestObject) o).getK().getClass());
        assertEquals(2, ((ParameterisedTestObject) o).getK());
    }

    @Test
    public void testParameterisedDAOTypeRefDeserialisation() throws SerialisationException {
        ParameterisedTestObject<Integer> test = new ParameterisedTestObject<>();
        test.setX("Test");
        test.setK(2);
        byte[] b = serialiser.serialise(test);
        ParameterisedTestObject<Integer> o = serialiser.deserialise(b, new TypeReference<ParameterisedTestObject<Integer>>() {
        });
        assertEquals("Test", o.getX());
        assertEquals(Integer.valueOf(2), o.getK());
    }

    @Test
    public void testParameterisedDeserialisationOfComplexObject() throws SerialisationException {
        SimpleTestObject test = new SimpleTestObject();
        test.setX("Test");
        byte[] b = serialiser.serialise(test);
        SimpleTestObject o = serialiser.deserialise(b, SimpleTestObject.class);
        assertEquals(SimpleTestObject.class, o.getClass());
        assertEquals("Test", o.getX());
    }

    @Test
    public void testParameterisedDeserialisationOfParameterisedComplexObject() throws SerialisationException {
        ParameterisedTestObject<Integer> test = new ParameterisedTestObject<>();
        test.setX("Test");
        test.setK(2);
        byte[] b = serialiser.serialise(test);
        ParameterisedTestObject o = serialiser.deserialise(b, ParameterisedTestObject.class);
        assertEquals(ParameterisedTestObject.class, o.getClass());
        assertEquals("Test", o.getX());
        assertEquals(Integer.class, o.getK().getClass());
        assertEquals(2, o.getK());
    }


    @Test(expected = SerialisationException.class)
    public void testParameterisedDeserialisationOfComplexObjectToIncorrectType() throws SerialisationException {
        SimpleTestObject test = new SimpleTestObject();
        test.setX("Test");
        byte[] b = serialiser.serialise(test);
        serialiser.deserialise(b, Integer.class);
    }

    @Test
    public void shouldSerialiseObjectWithoutFieldX() throws Exception {
        // Given
        final SimpleTestObject obj = new SimpleTestObject();

        // When
        final String json = new String(serialiser.serialise(obj, "x"), CommonConstants.UTF_8);

        // Then
        assertFalse(json.contains("x"));
    }

    @Test
    public void shouldSerialiseObjectWithFieldX() throws Exception {
        // Given
        final SimpleTestObject obj = new SimpleTestObject();

        // When
        final String json = new String(serialiser.serialise(obj), CommonConstants.UTF_8);

        // Then
        assertTrue(json.contains("x"));
    }
}
