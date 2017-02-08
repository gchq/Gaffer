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

package uk.gov.gchq.gaffer.data.element;

import java.io.Serializable;

/**
 * This interface is used by the lazy loader classes to load {@link uk.gov.gchq.gaffer.data.element.Element}'s identifiers and
 * properties.
 *
 * @see uk.gov.gchq.gaffer.data.element.LazyEntity
 * @see uk.gov.gchq.gaffer.data.element.LazyEdge
 * @see LazyProperties
 */
public interface ElementValueLoader extends Serializable {
    /**
     * @param name the property name to extract
     * @return the property value with the given name
     */
    Object getProperty(final String name);

    /**
     * @param idType the {@link uk.gov.gchq.gaffer.data.element.IdentifierType} to extract
     * @return the identifier value with the given {@link uk.gov.gchq.gaffer.data.element.IdentifierType}
     */
    Object getIdentifier(final IdentifierType idType);
}
