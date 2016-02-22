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

package gaffer.data.elementdefinition;

import java.io.Serializable;
import java.util.Collection;

/**
 * An <code>ElementDefinition</code> contains the definition for an {@link gaffer.data.element.Element} type.
 * Element definitions should have a collection of property names that are valid for the group.
 */
public interface ElementDefinition extends Serializable {
    /**
     * Validates this {@link gaffer.data.elementdefinition.ElementDefinition}.
     *
     * @return true if valid, otherwise false.
     */
    boolean validate();

    /**
     * @param propertyName the property name to check
     * @return true if this {@link gaffer.data.elementdefinition.ElementDefinition} contains the provided property name
     */
    boolean containsProperty(final String propertyName);

    /**
     * @return the property names in this {@link gaffer.data.elementdefinition.ElementDefinition}
     */
    Collection<String> getProperties();
}
