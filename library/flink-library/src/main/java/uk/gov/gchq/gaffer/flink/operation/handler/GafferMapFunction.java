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
import org.apache.flink.api.common.functions.MapFunction;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.generator.OneToManyElementGenerator;
import uk.gov.gchq.gaffer.data.generator.OneToOneElementGenerator;

import java.util.Collections;
import java.util.function.Function;

/**
 * Implementation of {@link MapFunction} to allow CSV strings representing {@link Element}s
 * to be mapped to Element objects.
 */
public class GafferMapFunction implements MapFunction<String, Iterable<? extends Element>> {
    private static final long serialVersionUID = -2338397824952911347L;
    public static final Class<Iterable<? extends Element>> RETURN_CLASS = (Class) Iterable.class;

    private Class<? extends Function<Iterable<? extends String>, Iterable<? extends Element>>> generatorClassName;

    @SuppressFBWarnings(value = "SE_BAD_FIELD", justification = "The constructor forces this to be serializable")
    private transient Function<Iterable<? extends String>, Iterable<? extends Element>> elementGenerator;

    public GafferMapFunction(final Class<? extends Function<Iterable<? extends String>, Iterable<? extends Element>>> generatorClassName) {
        this.generatorClassName = generatorClassName;
        try {
            this.elementGenerator = generatorClassName.newInstance();
        } catch (final InstantiationException | IllegalAccessException e) {
            throw new IllegalArgumentException("Unable to instantiate generator: " + generatorClassName.getName()
                    + " It must have a default constructor.", e);
        }
    }

    @Override
    public Iterable<? extends Element> map(final String csv) throws Exception {
        if (null == elementGenerator) {
            elementGenerator = generatorClassName.newInstance();
        }

        if (elementGenerator instanceof OneToOneElementGenerator) {
            return Collections.singleton(((OneToOneElementGenerator) elementGenerator)._apply(csv));
        }

        if (elementGenerator instanceof OneToManyElementGenerator) {
            return ((OneToManyElementGenerator) elementGenerator)._apply(csv);
        }

        return elementGenerator.apply(Collections.singleton(csv));
    }
}
