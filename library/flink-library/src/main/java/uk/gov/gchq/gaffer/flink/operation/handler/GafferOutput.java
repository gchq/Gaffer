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
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.Validatable;
import uk.gov.gchq.gaffer.store.Store;

import java.io.IOException;

/**
 * Implementation of {@link RichOutputFormat} for Gaffer to allow {@link Element}s
 * to be consumed from external sources.
 */
@SuppressFBWarnings(value = "SE_TRANSIENT_FIELD_NOT_RESTORED", justification = "There are null checks that will initialise the fields")
public class GafferOutput extends RichOutputFormat<Iterable<? extends Element>> {
    private static final long serialVersionUID = 1569145256866410621L;
    private final GafferAdder adder;

    public GafferOutput(final Validatable validatable, final Store store) {
        this(new GafferAdder(validatable, store));
    }

    public GafferOutput(final GafferAdder adder) {
        this.adder = adder;
    }

    @Override
    public void configure(final Configuration parameters) {
        // nothing to configure
    }

    @Override
    public void writeRecord(final Iterable<? extends Element> elements) {
        adder.add(elements);
    }

    @Override
    public void open(final int taskNumber, final int numTasks) throws IOException {
        adder.initialise();
    }

    @Override
    public void close() throws IOException {
        // no action required
    }
}
