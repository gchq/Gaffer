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

package uk.gov.gchq.gaffer.data.element.comparison;

import uk.gov.gchq.gaffer.data.element.Element;

public class ElementObjectComparator extends ElementComparator {
    private static final long serialVersionUID = -6238106025243239083L;

    @Override
    public int compare(final Element o1, final Element o2) {
        if (null != comparator) {
            return comparator.compare(o1, o2);
        }
        throw new IllegalArgumentException("Must provide a comparator instance.");
    }
}
