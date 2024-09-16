/*
 * Copyright 2016-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.accumulostore.operation.hdfs.mapper;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import uk.gov.gchq.gaffer.accumulostore.key.AccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.job.factory.AccumuloSampleDataForSplitPointsJobFactory;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.hdfs.operation.mapper.GafferMapper;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;

/**
 * Mapper class used for estimating the split points to ensure even distribution of
 * data in Accumulo after initial insert.
 */
public class SampleDataForSplitPointsMapper<KEY_IN, VALUE_IN> extends GafferMapper<KEY_IN, VALUE_IN, Key, Value> {

    private float proportionToSample;
    private AccumuloElementConverter elementConverter;

    @Override
    protected void setup(final Context context) {
        super.setup(context);
        proportionToSample = context.getConfiguration().getFloat(AccumuloSampleDataForSplitPointsJobFactory.PROPORTION_TO_SAMPLE, 0.001f);
        final Schema schema;
        schema = Schema.fromJson(context.getConfiguration()
                .get(AccumuloSampleDataForSplitPointsJobFactory.SCHEMA).getBytes(StandardCharsets.UTF_8));
        try {
            elementConverter = Class
                    .forName(context.getConfiguration().get(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS))
                    .asSubclass(AccumuloElementConverter.class)
                    .getConstructor(Schema.class)
                    .newInstance(schema);
        } catch (final ClassNotFoundException | InstantiationException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            throw new IllegalArgumentException("Element converter could not be created: "
                    + context.getConfiguration().get(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS), e);
        }
    }

    @Override
    protected void map(final Element element, final Context context) throws IOException, InterruptedException {
        if (Math.random() < proportionToSample) {
            context.getCounter("Split points", "Number sampled").increment(1L);
            final Pair<Key, Key> keyPair;
            try {
                keyPair = elementConverter.getKeysFromElement(element);
            } catch (final AccumuloElementConversionException e) {
                throw new IllegalArgumentException(e.getMessage(), e);
            }

            final Value value;
            try {
                value = elementConverter.getValueFromElement(element);
            } catch (final AccumuloElementConversionException e) {
                throw new IllegalArgumentException(e.getMessage(), e);
            }
            context.write(keyPair.getFirst(), value);
            if (null != keyPair.getSecond()) {
                context.write(keyPair.getSecond(), value);
            }
        } else {
            context.getCounter("Split points", "Number not sampled").increment(1L);
        }
    }

}
