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

package uk.gov.gchq.gaffer.hbasestore.filter;

import org.apache.hadoop.hbase.exceptions.DeserializationException;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.store.ElementValidator;
import uk.gov.gchq.gaffer.store.schema.Schema;

/**
 * The ValidatorFilter will filter out elements
 * based on the validator functions given in the {@link Schema} that is passed to this iterator.
 * <p>
 * If a {@link uk.gov.gchq.gaffer.function.FilterFunction} returns false then the Element is removed.
 */
public class ValidatorFilter extends AbstractElementFilter {
    public ValidatorFilter(final Schema schema) {
        super(schema, new ElementValidator(schema));
    }

    public static ValidatorFilter parseFrom(final byte[] pbBytes) throws DeserializationException {
        final FilterProperties props;
        try {
            props = JSON_SERIALISER.deserialise(pbBytes, FilterProperties.class);
        } catch (SerialisationException e) {
            throw new DeserializationException(e);
        }

        return new ValidatorFilter(props.getSchema());
    }
}
