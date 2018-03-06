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
package uk.gov.gchq.gaffer.hdfs.operation.mapper.generator;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.MapContext;

import uk.gov.gchq.gaffer.data.element.Element;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * An {@code AvroMapperGenerator} is a {@link MapperGenerator} that
 * can handle Avro input data and convert it into an {@link Iterable} of {@link Element}s.
 */
public class AvroMapperGenerator<OBJ> implements MapperGenerator<AvroKey<OBJ>, NullWritable> {
    private final List<OBJ> singleItemList = new ArrayList<>(1);
    private Function<Iterable<? extends OBJ>, Iterable<? extends Element>> elementGenerator;

    public AvroMapperGenerator() {
    }

    public AvroMapperGenerator(final Function<Iterable<? extends OBJ>, Iterable<? extends Element>> elementGenerator) {
        this.elementGenerator = elementGenerator;
    }

    @Override
    public Iterable<? extends Element> getElements(final AvroKey<OBJ> keyIn, final NullWritable valueIn, final MapContext<AvroKey<OBJ>, NullWritable, ?, ?> context) {
        singleItemList.clear();
        singleItemList.add(keyIn.datum());
        return elementGenerator.apply(singleItemList);
    }

    public Function<Iterable<? extends OBJ>, Iterable<? extends Element>> getElementGenerator() {
        return elementGenerator;
    }

    public void setElementGenerator(final Function<Iterable<? extends OBJ>, Iterable<? extends Element>> elementGenerator) {
        this.elementGenerator = elementGenerator;
    }
}
