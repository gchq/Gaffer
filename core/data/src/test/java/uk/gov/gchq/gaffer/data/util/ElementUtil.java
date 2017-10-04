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
import com.google.common.collect.Sets;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.comparison.ElementComparator;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class ElementUtil {
    public static void assertElementEquals(final Iterable<? extends Element> expected, final Iterable<? extends Element> result) {
        final Set<Element> expectedSet = Sets.newHashSet(expected);
        final Set<Element> resultSet = Sets.newHashSet(result);
        try {
            assertEquals(expectedSet, resultSet);
        } catch (final AssertionError err) {
            final List<Element> expectedList = Lists.newArrayList(expectedSet);
            final List<Element> resultList = Lists.newArrayList(resultSet);
            expectedList.removeAll(resultSet);
            resultList.removeAll(expectedSet);

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

            assertEquals("\nMissing entities:\n" + missingEntities.toString()
                            + "\nUnexpected entities:\n" + incorrectEntities.toString()
                            + "\nMissing edges:\n" + missingEdges.toString()
                            + "\nUnexpected edges:\n" + incorrectEdges.toString(),
                    expectedSet, resultSet);
        }
    }
}
