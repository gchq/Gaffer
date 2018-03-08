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
 * A {@code OneToOneElementGenerator} takes an input object and converts it into
 * an {@link Element}.
 *
 * @param <OBJ> the type of the input object
 */
public interface OneToOneElementGenerator<OBJ> extends ElementGenerator<OBJ> {
    @Override
    default Iterable<? extends Element> apply(final Iterable<? extends OBJ> domainObjects) {
        return new TransformIterable<OBJ, Element>(domainObjects) {
            @Override
            protected Element transform(final OBJ item) {
                return _apply(item);
            }
        };
    }

    /**
     * @param domainObject the domain object to convert
     * @return the generated {@link uk.gov.gchq.gaffer.data.element.Element}
     */
    Element _apply(final OBJ domainObject);
}
