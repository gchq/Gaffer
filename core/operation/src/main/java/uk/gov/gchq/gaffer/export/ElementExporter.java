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

package uk.gov.gchq.gaffer.export;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.user.User;

public abstract class ElementExporter<CONFIG> extends Exporter<CONFIG> {
    @Override
    protected void _add(final Iterable<?> values, final User user) {
        addElements((Iterable<Element>) values, user);
    }

    @Override
    protected CloseableIterable<?> _get(final User user,
                                        final int start, final int end) {
        return getElements(user, start, end);
    }

    protected abstract void addElements(final Iterable<Element> elements, final User user);

    protected abstract CloseableIterable<Element> getElements(final User user, final int start, final int end);
}
