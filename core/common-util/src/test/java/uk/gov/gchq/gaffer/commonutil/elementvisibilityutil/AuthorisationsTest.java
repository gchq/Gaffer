/*
 * Copyright 2017-2024 Crown Copyright
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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * This test class is copied from org.apache.accumulo.core.security.AuthorizationsTest.
 */
class AuthorisationsTest {

    @Test
    void testEncodeDecode() {
        final Authorisations a = new Authorisations("a", "abcdefg", "hijklmno", ",");
        final byte[] array = a.getAuthorisationsArray();
        final Authorisations b = new Authorisations(array);

        assertThat(b).isEqualTo(a);
    }

    @Test
    void testEncodeEmptyAuthorisations() {
        final Authorisations a = new Authorisations();
        final byte[] array = a.getAuthorisationsArray();
        final Authorisations b = new Authorisations(array);

        assertThat(b).isEqualTo(a);
    }

    @Test
    void testEncodeMultiByteAuthorisations() {
        final Authorisations a = new Authorisations("五", "b", "c", "九");
        final byte[] array = a.getAuthorisationsArray();
        final Authorisations b = new Authorisations(array);

        assertThat(b).isEqualTo(a);
    }

    @Test
    void testSerialization() {
        final Authorisations a1 = new Authorisations("a", "b");
        final Authorisations a2 = new Authorisations("b", "a");

        assertThat(a2).isEqualTo(a1);
        assertThat(a2.serialise()).isEqualTo(a1.serialise());
    }

    @Test
    void testDefensiveAccess() {
        final Authorisations expected = new Authorisations("foo", "a");
        final Authorisations actual = new Authorisations("foo", "a");

        // foo to goo; test defensive iterator
        for (byte[] bytes : actual) {
            bytes[0]++;
        }
        assertThat(actual.getAuthorisationsArray()).isEqualTo(expected.getAuthorisationsArray());

        // test defensive getter and serializer
        actual.getAuthorisations().get(0)[0]++;

        assertThat(actual.getAuthorisationsArray()).isEqualTo(expected.getAuthorisationsArray());
        assertThat(actual.serialise()).isEqualTo(expected.serialise());
    }

    // This should throw ReadOnlyBufferException, but THRIFT-883 requires that the ByteBuffers themselves not be read-only
    // @Test(expected = ReadOnlyBufferException.class)
    @Test
    void testReadOnlyByteBuffer() {
        final Authorisations expected = new Authorisations("foo");
        final Authorisations actual = new Authorisations("foo");

        assertThat(actual.getAuthorisationsArray()).isEqualTo(expected.getAuthorisationsArray());

        actual.getAuthorisationsBB().get(0).array()[0]++;
        assertThat(actual.getAuthorisationsArray()).isEqualTo(expected.getAuthorisationsArray());
    }

    @Test
    void testUnmodifiableList() {
        final Authorisations expected = new Authorisations("foo");
        final Authorisations actual = new Authorisations("foo");

        assertThat(actual.getAuthorisationsArray()).isEqualTo(expected.getAuthorisationsArray());

        final ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[] {'a'});
        final List<ByteBuffer> getAuthBB = actual.getAuthorisationsBB();

        assertThatExceptionOfType(UnsupportedOperationException.class)
            .isThrownBy(() -> getAuthBB.add(byteBuffer));
    }

    @Test
    void testToString() {
        final Authorisations a = new Authorisations("a", "abcdefg", "hijklmno");

        assertThat(a).hasToString("a,hijklmno,abcdefg");
    }
}
