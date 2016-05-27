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

package gaffer.export;

import gaffer.commonutil.iterable.CloseableIterable;
import gaffer.data.IsElementValidator;
import gaffer.data.TransformIterable;
import gaffer.data.element.Element;
import gaffer.user.User;

public abstract class ElementExporter extends Exporter {
    @Override
    protected void _add(final String key, final Iterable<Object> values, final User user) {
        addElements(key, new ElementExtractor(values), user);
    }

    @Override
    protected CloseableIterable<Object> _get(final String key, final User user,
                                             final int start, final int end) {
        return (CloseableIterable) getElements(key, user, start, end);
    }

    protected abstract void addElements(final String key, final Iterable<Element> elements, final User user);

    protected abstract CloseableIterable<Element> getElements(final String key, final User user, final int start, final int end);

    protected static final class ElementExtractor extends TransformIterable<Object, Element> {
        protected ElementExtractor(final Iterable<Object> input) {
            super(input, new IsElementValidator());
        }

        @Override
        protected Element transform(final Object item) {
            return (Element) item;
        }
    }
}
