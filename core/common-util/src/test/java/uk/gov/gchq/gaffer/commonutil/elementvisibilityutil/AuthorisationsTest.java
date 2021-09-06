/*
 * Copyright 2017-2021 Crown Copyright
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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * This test class is copied from org.apache.accumulo.core.security.AuthorizationsTest.
 */
public class AuthorisationsTest {

    @Test
    public void testEncodeDecode() {
        final Authorisations a = new Authorisations("a", "abcdefg", "hijklmno", ",");
        final byte[] array = a.getAuthorisationsArray();
        final Authorisations b = new Authorisations(array);

        assertEquals(a, b);
    }

    @Test
    public void testEncodeEmptyAuthorisations() {
        final Authorisations a = new Authorisations();
        final byte[] array = a.getAuthorisationsArray();
        final Authorisations b = new Authorisations(array);

        assertEquals(a, b);
    }

    @Test
    public void testEncodeMultiByteAuthorisations() {
        final Authorisations a = new Authorisations("五", "b", "c", "九");
        final byte[] array = a.getAuthorisationsArray();
        final Authorisations b = new Authorisations(array);

        assertEquals(a, b);
    }

    @Test
    public void testSerialization() {
        final Authorisations a1 = new Authorisations("a", "b");
        final Authorisations a2 = new Authorisations("b", "a");

        assertEquals(a1, a2);
        assertEquals(a1.serialise(), a2.serialise());
    }

    @Test
    public void testDefensiveAccess() {
        final Authorisations expected = new Authorisations("foo", "a");
        final Authorisations actual = new Authorisations("foo", "a");

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
        final Authorisations expected = new Authorisations("foo");
        final Authorisations actual = new Authorisations("foo");

        assertArrayEquals(expected.getAuthorisationsArray(), actual.getAuthorisationsArray());

        actual.getAuthorisationsBB().get(0).array()[0]++;
        assertArrayEquals(expected.getAuthorisationsArray(), actual.getAuthorisationsArray());
    }

    @Test
    public void testUnmodifiableList() {
        final Authorisations expected = new Authorisations("foo");
        final Authorisations actual = new Authorisations("foo");

        assertArrayEquals(expected.getAuthorisationsArray(), actual.getAuthorisationsArray());

        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> {
            actual.getAuthorisationsBB().add(ByteBuffer.wrap(new byte[] {'a'}));
        });
    }
}
