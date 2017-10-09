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

import com.google.common.collect.Iterables;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.Validatable;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Helper class to add {@link Element}s to a Gaffer store.
 */
@SuppressFBWarnings(value = "SE_TRANSIENT_FIELD_NOT_RESTORED", justification = "There are null checks that will initialise the fields")
public class GafferAdder implements Serializable {
    private static final long serialVersionUID = -3418606107861031989L;

    private final String graphId;
    private final byte[] schema;
    private final Properties properties;
    private final boolean validate;
    private final boolean skipInvalid;

    private transient Store store;
    private transient ConcurrentLinkedQueue<Element> queue;
    private transient boolean restart;

    public GafferAdder(final Validatable validatable, final Store store) {
        this.store = store;
        this.validate = validatable.isValidate();
        this.skipInvalid = validatable.isSkipInvalidElements();
        graphId = store.getGraphId();
        schema = store.getSchema().toCompactJson();
        properties = store.getProperties().getProperties();
    }

    public void initialise() {
        if (null == store) {
            store = Store.createStore(graphId, Schema.fromJson(schema), StoreProperties.loadStoreProperties(properties));
        }
    }

    public void add(final Iterable<? extends Element> elements) {
        if (null == queue) {
            queue = new ConcurrentLinkedQueue<>();
            restart = true;
        }

        if (null != elements) {
            Iterables.addAll(queue, elements);
        }

        if (restart && !queue.isEmpty()) {
            restart = false;
            store.runAsync(() -> {
                try {
                    store.execute(new AddElements.Builder()
                                    .input(new GafferQueue<>(queue))
                                    .validate(validate)
                                    .skipInvalidElements(skipInvalid)
                                    .build(),
                            new Context(new User()));
                    restart = true;
                } catch (final OperationException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }
}
