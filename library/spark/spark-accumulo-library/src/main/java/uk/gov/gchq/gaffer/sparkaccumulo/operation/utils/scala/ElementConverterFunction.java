/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.utils.scala;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.TraversableOnce;
import scala.collection.mutable.ArrayBuffer;
import scala.runtime.AbstractFunction1;

import uk.gov.gchq.gaffer.accumulostore.key.AccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;

public class ElementConverterFunction extends AbstractFunction1<Element, TraversableOnce<Tuple2<Key, Value>>> implements Serializable {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ElementConverterFunction.class);
    private static final long serialVersionUID = 2359835481339851648L;
    private transient Schema schema;
    private final byte[] schemaBytes;
    private transient AccumuloElementConverter keyConverter;
    private final Class<? extends AccumuloElementConverter> keyConverterClass;

    public ElementConverterFunction(final Schema schema, final AccumuloElementConverter keyConverter) {
        this.schema = schema;
        schemaBytes = schema.toCompactJson();
        this.keyConverter = keyConverter;
        keyConverterClass = keyConverter.getClass();
    }

    @Override
    public TraversableOnce<Tuple2<Key, Value>> apply(final Element element) {
        if (null == schema) {
            schema = Schema.fromJson(schemaBytes);
        }
        if (null == keyConverter) {
            try {
                keyConverter = keyConverterClass.getConstructor(Schema.class).newInstance(schema);
            } catch (final InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                throw new RuntimeException("Unable to instantiate key converter: " + keyConverterClass.getName(), e);
            }
        }
        final ArrayBuffer<Tuple2<Key, Value>> buf = new ArrayBuffer<>();
        Pair<Key, Key> keys = new Pair<>();
        Value value = null;
        try {
            keys = keyConverter.getKeysFromElement(element);
            value = keyConverter.getValueFromElement(element);
        } catch (final AccumuloElementConversionException e) {
            LOGGER.error(e.getMessage(), e);
        }
        final Key first = keys.getFirst();
        if (null != first) {
            buf.$plus$eq(new Tuple2<>(first, value));
        }
        final Key second = keys.getSecond();
        if (null != second) {
            buf.$plus$eq(new Tuple2<>(second, value));
        }
        return buf;
    }
}
