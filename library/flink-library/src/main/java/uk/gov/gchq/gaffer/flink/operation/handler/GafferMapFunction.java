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

import java.io.Serializable;
import java.util.Collections;
import java.util.function.Function;

public class GafferMapFunction implements MapFunction<String, Iterable<? extends Element>> {
    private static final long serialVersionUID = -2338397824952911347L;

    private Class<? extends Function> generatorClassName;

    @SuppressFBWarnings(value = "SE_BAD_FIELD", justification = "The constructor forces this to be serializable")
    private transient Function<Iterable<? extends String>, Iterable<? extends Element>> elementGenerator;

    public <T extends Function<Iterable<? extends String>, Iterable<? extends Element>> & Serializable> GafferMapFunction(final T elementGenerator) {
        this.generatorClassName = elementGenerator.getClass();
        this.elementGenerator = elementGenerator;
    }

    @Override
    public Iterable<? extends Element> map(final String csv) throws Exception {
        if (null == elementGenerator) {
            elementGenerator = generatorClassName.newInstance();
        }
        return elementGenerator.apply(Collections.singleton(csv));
    }

    public static Class<Iterable<? extends Element>> getReturnClass() {
        return (Class) Iterable.class;
    }
}
