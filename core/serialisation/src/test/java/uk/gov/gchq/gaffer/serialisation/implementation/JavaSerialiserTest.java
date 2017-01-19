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

package uk.gov.gchq.gaffer.serialisation.implementation;

import org.junit.Test;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ParameterisedTestObject;
import uk.gov.gchq.gaffer.serialisation.SimpleTestObject;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JavaSerialiserTest {

    final private JavaSerialiser SERIALISER = new JavaSerialiser();

    @Test
    public void testPrimitiveSerialisation() throws SerialisationException {
        final byte[] b = SERIALISER.serialise(2);
        final Object o = SERIALISER.deserialise(b);
        assertEquals(Integer.class, o.getClass());
        assertEquals(2, o);
    }

    @Test
    public void canHandleUnParameterisedDAO() throws SerialisationException {
        assertTrue(SERIALISER.canHandle(SimpleTestObject.class));
    }

    @Test
    public void testDAOSerialisation() throws SerialisationException {
        final SimpleTestObject test = new SimpleTestObject();
        test.setX("Test");
        final byte[] b = SERIALISER.serialise(test);
        final Object o = SERIALISER.deserialise(b);
        assertEquals(SimpleTestObject.class, o.getClass());
        assertEquals("Test", ((SimpleTestObject) o).getX());
    }

    @Test
    public void canHandleParameterisedDAO() throws SerialisationException {
        assertTrue(SERIALISER.canHandle(ParameterisedTestObject.class));
    }

    @Test
    public void testParameterisedDAOSerialisation() throws SerialisationException {
        final ParameterisedTestObject<Integer> test = new ParameterisedTestObject();
        test.setX("Test");
        test.setK(2);
        final byte[] b = SERIALISER.serialise(test);
        final Object o = SERIALISER.deserialise(b);
        assertEquals(ParameterisedTestObject.class, o.getClass());
        assertEquals("Test", ((ParameterisedTestObject) o).getX());
        assertEquals(Integer.class, ((ParameterisedTestObject) o).getK().getClass());
        assertEquals(2, ((ParameterisedTestObject) o).getK());
    }

    @Test
    public void testParameterisedDeserialisationOfSimpleObject() throws SerialisationException {
        final byte[] b = SERIALISER.serialise(2);
        final Integer o = SERIALISER.deserialise(b, Integer.class);
        assertEquals(Integer.class, o.getClass());
        assertEquals(0, o.compareTo(2));
    }

    @Test
    public void testParameterisedDeserialisationOfComplexObject() throws SerialisationException {
        SimpleTestObject test = new SimpleTestObject();
        test.setX("Test");
        byte[] b = SERIALISER.serialise(test);
        SimpleTestObject o = SERIALISER.deserialise(b, SimpleTestObject.class);
        assertEquals(SimpleTestObject.class, o.getClass());
        assertEquals("Test", o.getX());
    }

    @Test
    public void testParameterisedDeserialisationOfParameterisedComplexObject() throws SerialisationException {
        final ParameterisedTestObject<Integer> test = new ParameterisedTestObject();
        test.setX("Test");
        test.setK(2);
        final byte[] b = SERIALISER.serialise(test);
        final ParameterisedTestObject o = SERIALISER.deserialise(b, ParameterisedTestObject.class);
        assertEquals(ParameterisedTestObject.class, o.getClass());
        assertEquals("Test", o.getX());
        assertEquals(Integer.class, o.getK().getClass());
        assertEquals(2, o.getK());
    }


    @Test(expected = ClassCastException.class)
    public void testParameterisedDeserialisationOfComplexObjectToIncorrectType() throws SerialisationException {
        final SimpleTestObject test = new SimpleTestObject();
        test.setX("Test");
        final byte[] b = SERIALISER.serialise(test);
        final Integer o = SERIALISER.deserialise(b, Integer.class);
    }

}
