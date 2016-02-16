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

import gaffer.accumulostore.utils.Constants;
import gaffer.data.ElementValidator;
import gaffer.data.elementdefinition.schema.DataSchema;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

/**
 * The ExpiratorFilter will filter out {@link gaffer.data.element.Element}s
 * based on the expiration functions given in the {@link DataSchema} that is passed to this iterator.
 * <p>
 * If a {@link gaffer.function.FilterFunction} returns false then the Element is removed.
 */
public class ExpiratorFilter extends ElementFilter {
    @Override
    public IteratorOptions describeOptions() {
        final Map<String, String> namedOptions = new HashMap<>();
        namedOptions.put(Constants.DATA_SCHEMA, "A serialised data schema");
        namedOptions.put(Constants.STORE_SCHEMA, "A serialised store schema");
        return new IteratorOptions(Constants.DATA_SCHEMA,
                "Only returns elements that are valid",
                namedOptions, null);
    }

    @Override
    protected ElementValidator getElementValidator(final Map<String, String> options) throws UnsupportedEncodingException {
        if (!options.containsKey(Constants.DATA_SCHEMA)) {
            throw new IllegalArgumentException("Must specify the " + Constants.DATA_SCHEMA);
        }

        return new ElementValidator(DataSchema.fromJson(options.get(Constants.DATA_SCHEMA).getBytes(Constants.UTF_8_CHARSET)), true);
    }
}
