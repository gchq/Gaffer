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

package uk.gov.gchq.gaffer.accumulostore.key.impl;

import uk.gov.gchq.gaffer.accumulostore.key.AbstractElementFilter;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.store.ElementValidator;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

/**
 * The ValidatorFilter will filter out {@link uk.gov.gchq.gaffer.data.element.Element}s
 * based on the validator functions given in the {@link Schema} that is passed to this iterator.
 * <p>
 * If a {@link uk.gov.gchq.gaffer.function.FilterFunction} returns false then the Element is removed.
 */
public class ValidatorFilter extends AbstractElementFilter {
    @Override
    public IteratorOptions describeOptions() {
        final Map<String, String> namedOptions = new HashMap<>();
        namedOptions.put(AccumuloStoreConstants.SCHEMA, "A serialised schema");
        return new IteratorOptions(AccumuloStoreConstants.SCHEMA,
                "Only returns elements that are valid",
                namedOptions, null);
    }

    @Override
    protected boolean validate(final Element element) {
        return validator.validate(element);
    }

    @Override
    protected ElementValidator getElementValidator(final Map<String, String> options) {
        if (!options.containsKey(AccumuloStoreConstants.SCHEMA)) {
            throw new IllegalArgumentException("Must specify the " + AccumuloStoreConstants.SCHEMA);
        }

        try {
            return new ElementValidator(Schema.fromJson(options.get(AccumuloStoreConstants.SCHEMA).getBytes(CommonConstants.UTF_8)), false);
        } catch (UnsupportedEncodingException e) {
            throw new SchemaException("Unable to deserialise schema from JSON", e);
        }
    }
}
