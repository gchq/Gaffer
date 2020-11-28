/*
 * Copyright 2017-2020 Crown Copyright
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
import uk.gov.gchq.gaffer.flink.operation.handler.serialisation.ByteArraySchema;
import uk.gov.gchq.gaffer.operation.impl.add.EndOfElementsIndicator;

import java.util.Collections;
import java.util.function.Function;

/**
 * Implementation of {@link FlatMapFunction} to allow CSV strings representing {@link Element}s
 * to be mapped to Element objects.
 */
public class GafferMapFunction<T> implements FlatMapFunction<T, Element> {
    private static final long serialVersionUID = -2338397824952911347L;
    private static final Class NO_END_OF_ELEMENTS_INDICATOR = null;

    private Class<? extends Function<Iterable<? extends T>, Iterable<? extends Element>>> generatorClassName;

    @SuppressFBWarnings(value = "SE_BAD_FIELD", justification = "The constructor forces this to be serializable")
    private transient Function<Iterable<? extends T>, Iterable<? extends Element>> elementGenerator;
    private transient EndOfElementsIndicator<T> endOfElementsIndicator;

    private DeserializationSchema<T> serialisationType;

    public GafferMapFunction() {
        serialisationType = (DeserializationSchema) new SimpleStringSchema();
    }

    public GafferMapFunction(final Class<T> consumeAs, final Class<? extends Function<Iterable<? extends T>, Iterable<? extends Element>>> generatorClassName) {
        this(consumeAs, generatorClassName, NO_END_OF_ELEMENTS_INDICATOR);
    }

    public GafferMapFunction(final Class<T> consumeAs, final Class<? extends Function<Iterable<? extends T>, Iterable<? extends Element>>> generatorClassName, final Class<? extends EndOfElementsIndicator<T>> endOfElementsIndicator) {
        this.generatorClassName = generatorClassName;
        try {
            this.elementGenerator = generatorClassName.newInstance();
        } catch (final InstantiationException | IllegalAccessException e) {
            throw new IllegalArgumentException("Unable to instantiate generator: " + generatorClassName.getName()
                    + " It must have a default constructor.", e);
        }

        if (null != endOfElementsIndicator) {
            try {
                this.endOfElementsIndicator = endOfElementsIndicator.newInstance();
            } catch (final InstantiationException | IllegalAccessException e) {
                throw new IllegalArgumentException("Unable to instantiate endOfElementsIndicator: " + endOfElementsIndicator.getName()
                        + " It must have a default constructor.", e);
            }
        }

        setConsumeAs(consumeAs);
    }

    public void setConsumeAs(final Class<T> consumeAs) {

        if (null == consumeAs || String.class == consumeAs) {
            serialisationType = (DeserializationSchema) (endOfElementsIndicator == null ? new SimpleStringSchema() : new StringDeserializationSchema((EndOfElementsIndicator<String>) endOfElementsIndicator));
        } else if (byte[].class == consumeAs) {
            serialisationType = (DeserializationSchema) (endOfElementsIndicator == null ? new ByteArraySchema() : new ByteArrayDeserializationSchema((EndOfElementsIndicator<byte[]>) endOfElementsIndicator));
        } else {
            throw new IllegalArgumentException("This Flink handler cannot consume records as " + consumeAs + ". You must use either byte[] or String.");
        }
    }

    private static class StringDeserializationSchema extends SimpleStringSchema {
        private EndOfElementsIndicator<String> endOfElementsIndicator;

        StringDeserializationSchema(final EndOfElementsIndicator<String> endOfElementsIndicator) {
            this.endOfElementsIndicator = endOfElementsIndicator;
        }

        @Override
        public boolean isEndOfStream(final String item) {
            return endOfElementsIndicator == null ? false : endOfElementsIndicator.isEndOfElements(item);
        }
    }

    private static class ByteArrayDeserializationSchema extends ByteArraySchema {
        private EndOfElementsIndicator<byte[]> endOfElementsIndicator;

        ByteArrayDeserializationSchema(final EndOfElementsIndicator<byte[]> endOfElementsIndicator) {
            this.endOfElementsIndicator = endOfElementsIndicator;
        }

        @Override
        public boolean isEndOfStream(final byte[] item) {
            return endOfElementsIndicator == null ? false : endOfElementsIndicator.isEndOfElements(item);
        }
    }

    @Override
    public void flatMap(final T item, final Collector<Element> out) throws Exception {
        if (null == out) {
            throw new IllegalArgumentException("Element collector is required");
        }

        if (null == elementGenerator) {
            elementGenerator = generatorClassName.newInstance();
        }

        if (elementGenerator instanceof OneToOneElementGenerator) {
            out.collect(((OneToOneElementGenerator<T>) elementGenerator)._apply(item));
        } else if (elementGenerator instanceof OneToManyElementGenerator) {
            final Iterable<Element> elements = ((OneToManyElementGenerator<T>) elementGenerator)._apply(item);
            if (null != elements) {
                elements.forEach(out::collect);
            }
        } else {
            final Iterable<? extends Element> elements = (elementGenerator).apply(Collections.singleton(item));
            if (null != elements) {
                elements.forEach(out::collect);
            }
        }
    }

    public DeserializationSchema<T> getSerialisationType() {
        return serialisationType;
    }
}
