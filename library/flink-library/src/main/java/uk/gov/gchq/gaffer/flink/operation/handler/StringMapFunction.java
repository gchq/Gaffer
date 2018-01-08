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
package uk.gov.gchq.gaffer.flink.operation.handler;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.generator.OneToManyElementGenerator;
import uk.gov.gchq.gaffer.data.generator.OneToOneElementGenerator;

import java.util.Collections;
import java.util.function.Function;

/**
 * Implementation of {@link FlatMapFunction} to allow CSV strings representing {@link Element}s
 * to be mapped to Element objects.
 */
public class StringMapFunction extends GafferMapFunction<String> {

    public StringMapFunction() {

    }


    public StringMapFunction(final Class<? extends Function<Iterable<? extends String>, Iterable<? extends Element>>> generatorClassName) {
        super(generatorClassName);
    }

    @Override
    public DeserializationSchema<String> getSerialisationType() {
        return new SimpleStringSchema();
    }
}
