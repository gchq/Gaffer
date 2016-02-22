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

package gaffer.jsonSerialisation;

import gaffer.exception.SerialisationException;
import gaffer.jsonserialisation.JSONSerialiser;
import gaffer.serialisation.test.ParameterisedTestObject;
import gaffer.serialisation.test.SimpleTestObject;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JSONSerialiserTest {

    private JSONSerialiser serialiser = null;

    @Before
    public void setupTest() throws SerialisationException {
        serialiser = new JSONSerialiser();
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
        assertEquals("{\n  \"x\" : \"TestValue1\"\n}", new String(bytes));
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

}
