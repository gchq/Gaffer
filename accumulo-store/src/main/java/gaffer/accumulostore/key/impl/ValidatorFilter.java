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

package gaffer.accumulostore.key.impl;

import gaffer.accumulostore.utils.AccumuloStoreConstants;
import gaffer.data.elementdefinition.schema.exception.SchemaException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

/**
 * The ValidatorFilter will filter out {@link gaffer.data.element.Element}s
 * based on the validator functions given in the {@link DataSchema} that is passed to this iterator.
 * <p>
 * If a {@link gaffer.function.FilterFunction} returns false then the Element is removed.
 */
public class ValidatorFilter extends ElementFilter {
    @Override
    public IteratorOptions describeOptions() {
        final Map<String, String> namedOptions = new HashMap<>();
        namedOptions.put(AccumuloStoreConstants.DATA_SCHEMA, "A serialised data schema");
        namedOptions.put(AccumuloStoreConstants.STORE_SCHEMA, "A serialised store schema");
        return new IteratorOptions(AccumuloStoreConstants.DATA_SCHEMA,
                "Only returns elements that are valid",
                namedOptions, null);
    }

    @Override
    protected ElementValidator getElementValidator(final Map<String, String> options) {
        if (!options.containsKey(AccumuloStoreConstants.DATA_SCHEMA)) {
            throw new IllegalArgumentException("Must specify the " + AccumuloStoreConstants.DATA_SCHEMA);
        }

        try {
            return new ElementValidator(DataSchema.fromJson(options.get(AccumuloStoreConstants.DATA_SCHEMA).getBytes(AccumuloStoreConstants.UTF_8_CHARSET)));
        } catch (UnsupportedEncodingException e) {
            throw new SchemaException("Unable to deserialise data schema from JSON", e);
        }
    }
}
