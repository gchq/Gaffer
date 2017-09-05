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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.Validatable;
import uk.gov.gchq.gaffer.store.Store;

@SuppressFBWarnings(value = "SE_TRANSIENT_FIELD_NOT_RESTORED", justification = "There are null checks that will initialise the fields")
public class GafferSink extends RichSinkFunction<Iterable<? extends Element>> {
    private static final long serialVersionUID = 1569145256866410621L;
    private final GafferAdder adder;

    public GafferSink(final Validatable validatable, final Store store) {
        this(new GafferAdder(validatable, store));
    }

    public GafferSink(final GafferAdder adder) {
        this.adder = adder;
    }

    @Override
    public void open(final Configuration parameters) throws Exception {
        super.open(parameters);
        adder.initialise();
    }

    @Override
    public void invoke(final Iterable<? extends Element> elements) throws Exception {
        adder.add(elements);
    }
}
