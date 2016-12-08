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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MapContext;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.generator.ElementGenerator;
import java.util.ArrayList;
import java.util.List;

/**
 * An <code>TextMapperGenerator</code> is an {@link MapperGenerator} that
 * can handle text input data and convert it into an {@link Iterable} of {@link Element}s.
 */
public class TextMapperGenerator implements MapperGenerator<LongWritable, Text> {
    private final List<String> singleItemList = new ArrayList<>(1);
    private ElementGenerator<String> elementGenerator;

    public TextMapperGenerator() {
    }

    public TextMapperGenerator(final ElementGenerator<String> elementGenerator) {
        this.elementGenerator = elementGenerator;
    }

    @Override
    public Iterable<Element> getElements(final LongWritable keyIn, final Text valueIn, final MapContext<LongWritable, Text, ?, ?> context) {
        singleItemList.clear();
        singleItemList.add(valueIn.toString());
        return elementGenerator.getElements(singleItemList);
    }

    public ElementGenerator<String> getElementGenerator() {
        return elementGenerator;
    }

    public void setElementGenerator(final ElementGenerator<String> elementGenerator) {
        this.elementGenerator = elementGenerator;
    }
}
