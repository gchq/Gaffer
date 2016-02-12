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

import gaffer.data.element.Element;
import gaffer.data.generator.ElementGenerator;
import gaffer.data.generator.OneToOneElementGenerator;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.MapContext;

import java.util.ArrayList;
import java.util.List;

/**
 * An <code>AvroMapperGenerator</code> is an {@link gaffer.operation.simple.hdfs.handler.mapper.MapperGenerator} that
 * can handle Avro input data and convert it into an {@link java.lang.Iterable} of {@link gaffer.data.element.Element}s.
 */
public class AvroMapperGenerator<OBJ> implements MapperGenerator<AvroKey<OBJ>, NullWritable> {
    private final List<OBJ> singleItemList = new ArrayList<>(1);
    private ElementGenerator<OBJ> elementGenerator;

    public AvroMapperGenerator() {
    }

    public AvroMapperGenerator(final ElementGenerator<OBJ> elementGenerator) {
        this.elementGenerator = elementGenerator;
    }

    @Override
    public Iterable<Element> getElements(final AvroKey<OBJ> keyIn, final NullWritable valueIn, final MapContext<AvroKey<OBJ>, NullWritable, ?, ?> context) {
        singleItemList.clear();
        singleItemList.add(keyIn.datum());
        return elementGenerator.getElements(singleItemList);
    }

    public ElementGenerator<OBJ> getElementGenerator() {
        return elementGenerator;
    }

    public void setElementGenerator(final OneToOneElementGenerator<OBJ> elementGenerator) {
        this.elementGenerator = elementGenerator;
    }
}
