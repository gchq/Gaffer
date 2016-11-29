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

package uk.gov.gchq.gaffer.commonutil.iterable;

import com.google.common.collect.Lists;
import org.junit.Test;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class EmptyCloseableIterableTest {

    @Test
    public void shouldBeEmpty() {
        // Given
        final EmptyClosableIterable iterable = new EmptyClosableIterable();

        // When
        final List list = Lists.newArrayList(iterable);

        // Then
        assertTrue(list.isEmpty());
    }
}
