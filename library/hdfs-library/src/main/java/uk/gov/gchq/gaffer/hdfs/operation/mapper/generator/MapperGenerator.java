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
package uk.gov.gchq.gaffer.hdfs.operation.mapper.generator;

import org.apache.hadoop.mapreduce.MapContext;
import uk.gov.gchq.gaffer.data.element.Element;

/**
 * A <code>MapperGenerator</code> is used by the Hadoop job {@link org.apache.hadoop.mapreduce.Mapper} to convert the
 * input key and value to an {@link Iterable} of {@link Element}s
 *
 * @param <KEY_IN>   the input key for the mapper
 * @param <VALUE_IN> the input value for the mapper
 * @see AvroMapperGenerator
 * @see TextMapperGenerator
 */
public interface MapperGenerator<KEY_IN, VALUE_IN> {
    Iterable<Element> getElements(final KEY_IN keyIn, final VALUE_IN valueIn, final MapContext<KEY_IN, VALUE_IN, ?, ?> context);
}
