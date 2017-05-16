/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.impl.compare;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.comparison.ElementComparator;
import java.util.Collections;
import java.util.Set;

/**
 * An <code>ElementComparison</code> operation is an operation which is used
 * to make comparisons between elements.
 * <p>
 * Elements comparisons are delegated to the provided {@link uk.gov.gchq.gaffer.data.element.comparison.ElementComparator}.
 */
public interface ElementComparison {
    default Set<Pair<String, String>> getComparableGroupPropertyPairs() {
        final ElementComparator comparator = getComparator();
        if (null != comparator) {
            return comparator.getComparableGroupPropertyPairs();
        }

        return Collections.emptySet();
    }

    ElementComparator getComparator();
}
