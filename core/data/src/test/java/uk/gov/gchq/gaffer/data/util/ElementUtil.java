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

package uk.gov.gchq.gaffer.data.util;

import com.google.common.collect.Lists;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.comparison.ElementComparator;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class ElementUtil {
    public static void assertElementEquals(final Iterable<? extends Element> expected, final Iterable<? extends Element> result) {
        final List<Element> expectedCache = Lists.newArrayList(expected);
        final List<Element> resultCache = Lists.newArrayList(result);
        try {
            assertEquals(expectedCache, resultCache);
        } catch (final AssertionError err) {
            final List<Element> expectedList = Lists.newArrayList(expectedCache);
            final List<Element> resultList = Lists.newArrayList(resultCache);
            for (final Element element : resultCache) {
                expectedList.remove(element);
            }
            for (final Element element : expectedCache) {
                resultList.remove(element);
            }

            final ElementComparator elementComparator = (element1, element2) -> {
                final String elementStr1 = null == element1 ? "" : element1.toString();
                final String elementStr2 = null == element2 ? "" : element2.toString();
                return elementStr1.compareTo(elementStr2);
            };
            expectedList.sort(elementComparator);
            resultList.sort(elementComparator);

            final List<Element> missingEntities = new ArrayList<>();
            final List<Element> missingEdges = new ArrayList<>();
            for (final Element element : expectedList) {
                if (element instanceof Entity) {
                    missingEntities.add(element);
                } else {
                    missingEdges.add(element);
                }
            }

            final List<Element> incorrectEntities = new ArrayList<>();
            final List<Element> incorrectEdges = new ArrayList<>();
            for (final Element element : resultList) {
                if (element instanceof Entity) {
                    incorrectEntities.add(element);
                } else {
                    incorrectEdges.add(element);
                }
            }

            assertThat("\nMissing entities:\n" + missingEntities.toString()
                            + "\nUnexpected entities:\n" + incorrectEntities.toString()
                            + "\nMissing edges:\n" + missingEdges.toString()
                            + "\nUnexpected edges:\n" + incorrectEdges.toString(),
                    expectedCache, containsInAnyOrder(resultCache.toArray()));
        }
    }
}
