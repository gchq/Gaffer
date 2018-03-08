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

package uk.gov.gchq.gaffer.commonutil.elementvisibilityutil;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * This test class is copied from org.apache.accumulo.core.security.AuthorizationsTest.
 */
public class AuthorisationsTest {

    @Test
    public void testEncodeDecode() {
        Authorisations a = new Authorisations("a", "abcdefg", "hijklmno", ",");
        byte[] array = a.getAuthorisationsArray();
        Authorisations b = new Authorisations(array);
        assertEquals(a, b);

        // test encoding empty auths
        a = new Authorisations();
        array = a.getAuthorisationsArray();
        b = new Authorisations(array);
        assertEquals(a, b);

        // test encoding multi-byte auths
        a = new Authorisations("五", "b", "c", "九");
        array = a.getAuthorisationsArray();
        b = new Authorisations(array);
        assertEquals(a, b);
    }

    @Test
    public void testSerialization() {
        Authorisations a1 = new Authorisations("a", "b");
        Authorisations a2 = new Authorisations("b", "a");

        assertEquals(a1, a2);
        assertEquals(a1.serialise(), a2.serialise());
    }

    @Test
    public void testDefensiveAccess() {
        Authorisations expected = new Authorisations("foo", "a");
        Authorisations actual = new Authorisations("foo", "a");

        // foo to goo; test defensive iterator
        for (byte[] bytes : actual) {
            bytes[0]++;
        }
        assertArrayEquals(expected.getAuthorisationsArray(), actual.getAuthorisationsArray());

        // test defensive getter and serializer
        actual.getAuthorisations().get(0)[0]++;
        assertArrayEquals(expected.getAuthorisationsArray(), actual.getAuthorisationsArray());
        assertEquals(expected.serialise(), actual.serialise());
    }

    // This should throw ReadOnlyBufferException, but THRIFT-883 requires that the ByteBuffers themselves not be read-only
    // @Test(expected = ReadOnlyBufferException.class)
    @Test
    public void testReadOnlyByteBuffer() {
        Authorisations expected = new Authorisations("foo");
        Authorisations actual = new Authorisations("foo");

        assertArrayEquals(expected.getAuthorisationsArray(), actual.getAuthorisationsArray());
        actual.getAuthorisationsBB().get(0).array()[0]++;
        assertArrayEquals(expected.getAuthorisationsArray(), actual.getAuthorisationsArray());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testUnmodifiableList() {
        Authorisations expected = new Authorisations("foo");
        Authorisations actual = new Authorisations("foo");

        assertArrayEquals(expected.getAuthorisationsArray(), actual.getAuthorisationsArray());
        actual.getAuthorisationsBB().add(ByteBuffer.wrap(new byte[]{'a'}));
    }
}
