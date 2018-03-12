/*
 * Copyright 2016-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.hdfs.operation.mapper;

import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.hdfs.operation.mapper.generator.MapperGenerator;
import uk.gov.gchq.gaffer.store.ElementValidator;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.serialisation.json.SimpleClassNameIdResolver;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import static uk.gov.gchq.gaffer.hdfs.operation.handler.job.factory.AddElementsFromHdfsJobFactory.MAPPER_GENERATOR;
import static uk.gov.gchq.gaffer.hdfs.operation.handler.job.factory.AddElementsFromHdfsJobFactory.SCHEMA;
import static uk.gov.gchq.gaffer.hdfs.operation.handler.job.factory.AddElementsFromHdfsJobFactory.VALIDATE;

/**
 * An {@code GafferMapper} is a {@link Mapper} that uses a
 * {@link MapperGenerator} to map the input data into
 * {@link Element}s and then delegates the elements to an abstract map method to serialise them
 * and add them to the job map context.
 *
 * @param <KEY_IN>    type of input key
 * @param <VALUE_IN>  type of input value
 * @param <KEY_OUT>   type of output key
 * @param <VALUE_OUT> type of output value
 */
public abstract class GafferMapper<KEY_IN, VALUE_IN, KEY_OUT, VALUE_OUT> extends Mapper<KEY_IN, VALUE_IN, KEY_OUT, VALUE_OUT> {
    private static final Logger LOGGER = LoggerFactory.getLogger(GafferMapper.class);
    private MapperGenerator<KEY_IN, VALUE_IN> mapperGenerator;
    private boolean doValidation;
    private ElementValidator elementValidator;
    protected Schema schema;

    @Override
    protected void setup(final Context context) {
        doValidation = Boolean.parseBoolean(context.getConfiguration().get(VALIDATE));
        try {
            schema = Schema.fromJson(context.getConfiguration().get(SCHEMA).getBytes(CommonConstants.UTF_8));
        } catch (final UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        elementValidator = new ElementValidator(schema);

        final String generatorClass = context.getConfiguration().get(MAPPER_GENERATOR);
        try {
            mapperGenerator = Class.forName(SimpleClassNameIdResolver.getClassName(generatorClass)).asSubclass(MapperGenerator.class).newInstance();
        } catch (final ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new IllegalArgumentException("Element generator could be created: " + generatorClass, e);
        }
    }

    @Override
    protected void map(final KEY_IN keyIn, final VALUE_IN valueIn, final Context context) throws IOException, InterruptedException {
        for (final Element element : mapperGenerator.getElements(keyIn, valueIn, context)) {
            if (!doValidation || isValid(element)) {
                map(element, context);
            } else {
                LOGGER.warn("Element {} did not validate.", element);
                context.getCounter("Bulk import", "Invalid element count").increment(1L);
            }
        }
    }

    protected boolean isValid(final Element element) {
        return elementValidator.validateWithSchema(element);
    }

    protected abstract void map(final Element element, final Context context) throws IOException, InterruptedException;
}
