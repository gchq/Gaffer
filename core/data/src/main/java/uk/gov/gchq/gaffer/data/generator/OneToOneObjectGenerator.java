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

package uk.gov.gchq.gaffer.data.generator;

import uk.gov.gchq.gaffer.commonutil.iterable.TransformIterable;
import uk.gov.gchq.gaffer.data.element.Element;

/**
 * A {@code OneToOneObjectGenerator} takes an input {@link Element} and converts it into
 * an output object.
 *
 * @param <OBJ> the type of the output object
 */
public interface OneToOneObjectGenerator<OBJ> extends ObjectGenerator<OBJ> {
    @Override
    default Iterable<? extends OBJ> apply(final Iterable<? extends Element> elements) {
        return new TransformIterable<Element, OBJ>(elements) {
            @Override
            protected OBJ transform(final Element element) {
                return _apply(element);
            }
        };
    }

    /**
     * @param element the element to convert
     * @return the generated domain object
     */
    OBJ _apply(final Element element);
}
