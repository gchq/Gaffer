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

package uk.gov.gchq.gaffer.accumulostore.utils;

import org.apache.accumulo.core.iterators.OptionDescriber.IteratorOptions;
import java.util.HashMap;
import java.util.Map;

public class IteratorOptionsBuilder {

    private static final String VIEW_DESCRIPTION = "Required: The json serialised form of a view";
    private static final String SCHEMA_DESCRIPTION = "Required: The json serialised form of the schema";
    private static final String ACCUMULO_ELEMENT_CONVERTER_CLASS_DESCRIPTION = "Required: The element converter class to be used for key/value conversion";

    public IteratorOptions options;
    public Map<String, String> namedOptions = new HashMap<String, String>();

    public IteratorOptionsBuilder(final IteratorOptions options) {
        this.options = options;
        this.namedOptions = options.getNamedOptions();
    }

    public IteratorOptionsBuilder(final String name, final String description) {
        this.options = new IteratorOptions(name, description, null, null);
    }

    public IteratorOptionsBuilder addNamedOption(final String optionName, final String optionDescription) {
        namedOptions.put(optionName, optionDescription);
        return this;
    }

    public IteratorOptionsBuilder addViewNamedOption() {
        return addNamedOption(AccumuloStoreConstants.VIEW, VIEW_DESCRIPTION);
    }

    public IteratorOptionsBuilder addSchemaNamedOption() {
        return addNamedOption(AccumuloStoreConstants.SCHEMA, SCHEMA_DESCRIPTION);
    }

    public IteratorOptionsBuilder addElementConverterClassNamedOption() {
        return addNamedOption(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS, ACCUMULO_ELEMENT_CONVERTER_CLASS_DESCRIPTION);
    }

    public IteratorOptionsBuilder setIteratorName(final String iteratorName) {
        options.setName(iteratorName);
        return this;
    }

    public IteratorOptionsBuilder setIteratorDescription(final String iteratorDescription) {
        options.setDescription(iteratorDescription);
        return this;
    }

    public IteratorOptions build() {
        if (!namedOptions.isEmpty()) {
            options.setNamedOptions(namedOptions);
        }
        return options;
    }

}
