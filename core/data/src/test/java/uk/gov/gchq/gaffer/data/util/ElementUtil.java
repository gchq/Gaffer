/*
 * Copyright 2016-2019 Crown Copyright
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
import uk.gov.gchq.gaffer.data.element.id.EntityId;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
        try {
            assertEquals(expectedCache, resultCache);
        } catch (final AssertionError err) {
            final List<ElementId> expectedList = Lists.newArrayList(expectedCache);
            final List<ElementId> resultList = Lists.newArrayList(resultCache);
            if (ignoreDuplicates) {
                expectedList.removeAll(resultCache);
                resultList.removeAll(expectedCache);
            } else {
                for (final ElementId element : resultCache) {
                    expectedList.remove(element);
                }
                for (final ElementId element : expectedCache) {
                    resultList.remove(element);
                }
            }

            final Comparator<ElementId> elementComparator = (element1, element2) -> {
                final String elementStr1 = null == element1 ? "" : element1.toString();
                final String elementStr2 = null == element2 ? "" : element2.toString();
                return elementStr1.compareTo(elementStr2);
            };
            expectedList.sort(elementComparator);
            resultList.sort(elementComparator);

            final List<ElementId> missingEntities = new ArrayList<>();
            final List<ElementId> missingEdges = new ArrayList<>();
            for (final ElementId element : expectedList) {
                if (element instanceof EntityId) {
                    missingEntities.add(element);
                } else {
                    missingEdges.add(element);
                }
            }

            final List<ElementId> incorrectEntities = new ArrayList<>();
            final List<ElementId> incorrectEdges = new ArrayList<>();
            for (final ElementId element : resultList) {
                if (element instanceof EntityId) {
                    incorrectEntities.add(element);
                } else {
                    incorrectEdges.add(element);
                }
            }

            assertTrue("\nMissing entities:\n(" + missingEntities.size() + ") " + missingEntities.toString()
                            + "\nUnexpected entities:\n(" + incorrectEntities.size() + ") " + incorrectEntities.toString()
                            + "\nMissing edges:\n(" + missingEdges.size() + ")" + missingEdges.toString()
                            + "\nUnexpected edges:\n(" + incorrectEdges.size() + ")" + incorrectEdges.toString(),
                    missingEntities.isEmpty() && incorrectEntities.isEmpty()
                            && missingEdges.isEmpty() && incorrectEdges.isEmpty());
        }
    }
}
