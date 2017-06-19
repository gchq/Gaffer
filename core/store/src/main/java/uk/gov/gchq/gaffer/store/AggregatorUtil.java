/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.store;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;

import java.util.Set;
import java.util.function.Function;

public class AggregatorUtil {

    private AggregatorUtil() {
        // do not instantiate
    }

    /**
     * Creates a Function that takes and element as input and outputs an element that consists of
     * the Group-by values, the Identifiers and the Group. These act as a key and can be used in a
     * Collector.
     *
     * @return an Element which makes up a unique Identifier. The set is composed of the input Element's
     * Group-By, Identifiers and the Group name.
     */
    public static Function<Element, Element> createGroupByFunction(Schema schema) {
        return element -> {
            Element key =  element.emptyClone();

            final String group = element.getGroup();
            final SchemaElementDefinition elDef = schema.getElement(group);

            if (elDef == null) {
                throw new RuntimeException("received element " + element + " which belongs to a " +
                        "group not found in the schema");
            }

            Set<String> groupBy = elDef.getGroupBy();

            for (final String property : groupBy) {
                key.putProperty(property, element.getProperty(property));
            }

            return key;
        };
    }
}
