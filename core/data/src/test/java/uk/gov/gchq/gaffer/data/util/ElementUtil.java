/*
 * Copyright 2016-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.data.util;

import com.google.common.collect.Lists;

import uk.gov.gchq.gaffer.data.element.id.ElementId;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public final class ElementUtil {

    private ElementUtil() {
        // Private to avoid instantiation
    }

    public static void assertElementEquals(final Iterable<? extends ElementId> expected, final Iterable<? extends ElementId> result) {
        assertElementEquals(expected, result, false);
    }

    public static void assertElementEquals(final Iterable<? extends ElementId> expected, final Iterable<? extends ElementId> result, final boolean ignoreDuplicates) {
        final List<ElementId> expectedCache = Lists.newArrayList(expected);
        final List<ElementId> resultCache = Lists.newArrayList(result);
        if (ignoreDuplicates) {
            assertThat(resultCache).hasSameElementsAs(expectedCache);
        } else {
            assertThat(resultCache).containsExactlyInAnyOrderElementsOf(expectedCache);
        }
    }

    public static void assertElementEqualsIncludingMatchedVertex(final Iterable<? extends ElementId> expected, final Iterable<? extends ElementId> result) {
        assertElementEqualsIncludingMatchedVertex(expected, result, false);
    }

    public static void assertElementEqualsIncludingMatchedVertex(final Iterable<? extends ElementId> expected, final Iterable<? extends ElementId> result, final boolean ignoreDuplicates) {
        final List<ElementId> expectedCache = Lists.newArrayList(expected);
        final List<ElementId> resultCache = Lists.newArrayList(result);
        if (ignoreDuplicates) {
            assertThat(resultCache)
                .usingRecursiveFieldByFieldElementComparator()
                .hasSameElementsAs(expectedCache);
        } else {
            assertThat(resultCache)
                .usingRecursiveFieldByFieldElementComparator()
                .containsExactlyInAnyOrderElementsOf(expectedCache);
        }
    }
}
