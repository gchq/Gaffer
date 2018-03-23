/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.commonutil;

import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class CloseableUtilTest {
    @Test
    public void shouldCloseACloseable() throws IOException {
        // Given
        final Closeable closeable = mock(Closeable.class);

        // When
        CloseableUtil.close(closeable);

        // Then
        verify(closeable).close();
    }

    @Test
    public void shouldCloseAllCloseables() throws IOException {
        // Given
        final Closeable closeable1 = mock(Closeable.class);
        final Closeable closeable2 = mock(Closeable.class);
        final Object nonCloseable = mock(Object.class);

        // When
        CloseableUtil.close(closeable1, nonCloseable, closeable2);

        // Then
        verify(closeable1).close();
        verify(closeable2).close();
    }

    @Test
    public void shouldNotThrowExceptionForNullObject() throws IOException {
        // Given
        final Object obj = null;

        // When
        CloseableUtil.close(obj);

        // Then - no exceptions
    }

    @Test
    public void shouldNotThrowExceptionForNonCloseableObject() throws IOException {
        // Given
        final Object obj = "Some string";

        // When
        CloseableUtil.close(obj);

        // Then - no exceptions
    }
}
