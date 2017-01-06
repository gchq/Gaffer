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

package uk.gov.gchq.gaffer.data.generator;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import uk.gov.gchq.gaffer.data.element.Element;

/**
 * An <code>ElementGenerator</code> converts domain objects to {@link uk.gov.gchq.gaffer.data.element.Element}s and vice versa.
 * <p>
 * Implementations should be JSON serialisable.
 *
 * @param <OBJ> the type of domain object
 */
@JsonTypeInfo(use = Id.CLASS, include = As.PROPERTY, property = "class")
public interface ElementGenerator<OBJ> {
    /**
     * @param domainObjects an {@link java.lang.Iterable} of domain objects to convert
     * @return an {@link java.lang.Iterable} of {@link uk.gov.gchq.gaffer.data.element.Element}
     */
    Iterable<Element> getElements(final Iterable<OBJ> domainObjects);

    /**
     * @param elements an {@link java.lang.Iterable} of {@link uk.gov.gchq.gaffer.data.element.Element}s to convert
     * @return an {@link java.lang.Iterable} of domain objects
     */
    Iterable<OBJ> getObjects(final Iterable<Element> elements);
}
