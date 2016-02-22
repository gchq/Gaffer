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
package gaffer.operation.simple.hdfs.handler.mapper;

import gaffer.data.ElementValidator;
import gaffer.data.element.Element;
import gaffer.data.elementdefinition.schema.DataSchema;
import gaffer.operation.simple.hdfs.handler.AddElementsFromHdfsJobFactory;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;


/**
 * An <code>AbstractAddElementsFromHdfsMapper</code> is a {@link org.apache.hadoop.mapreduce.Mapper} that uses a
 * {@link gaffer.operation.simple.hdfs.handler.mapper.MapperGenerator} to map the input data into
 * {@link gaffer.data.element.Element}s and then delegates the elements to an abstract map method to serialise them
 * and add them to the job map context.
 *
 * @param <KEY_IN>    type of input key
 * @param <VALUE_IN>  type of input value
 * @param <KEY_OUT>   type of output key
 * @param <VALUE_OUT> type of output value
 */
public abstract class AbstractAddElementsFromHdfsMapper<KEY_IN, VALUE_IN, KEY_OUT, VALUE_OUT> extends Mapper<KEY_IN, VALUE_IN, KEY_OUT, VALUE_OUT> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractAddElementsFromHdfsMapper.class);
    private MapperGenerator<KEY_IN, VALUE_IN> mapperGenerator;

    private boolean doValidation;

    private ElementValidator elementValidator;

    protected void setup(final Context context) {
        doValidation = Boolean.parseBoolean(context.getConfiguration().get(AddElementsFromHdfsJobFactory.VALIDATE));

        final DataSchema dataSchema;
        try {
            dataSchema = DataSchema.fromJson(context.getConfiguration().get(AddElementsFromHdfsJobFactory.DATA_SCHEMA).getBytes(AddElementsFromHdfsJobFactory.UTF_8_CHARSET));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        elementValidator = new ElementValidator(dataSchema);

        final String generatorClass = context.getConfiguration().get(AddElementsFromHdfsJobFactory.MAPPER_GENERATOR);
        try {
            mapperGenerator = Class.forName(generatorClass).asSubclass(MapperGenerator.class).newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new IllegalArgumentException("Element generator could be created: " + generatorClass, e);
        }
    }

    protected void map(final KEY_IN keyIn, final VALUE_IN valueIn, final Context context) throws IOException, InterruptedException {
        for (Element element : mapperGenerator.getElements(keyIn, valueIn, context)) {
            if (!doValidation || isValid(element)) {
                map(element, context);
            } else {
                LOGGER.warn("Element " + element + " did not validate.");
            }
        }
    }

    protected boolean isValid(final Element element) {
        return elementValidator.validate(element);
    }


    protected abstract void map(final Element element, final Context context) throws IOException, InterruptedException;
}
