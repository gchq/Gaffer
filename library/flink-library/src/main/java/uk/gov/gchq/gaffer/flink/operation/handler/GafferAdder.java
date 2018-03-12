/*
 * Copyright 2017-2018 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.iterable.ConsumableBlockingQueue;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.flink.operation.handler.util.FlinkConstants;
import uk.gov.gchq.gaffer.operation.Operation;
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

/**
 * <p>
 * Helper class to add {@link Element}s to a Gaffer store.
 * </p>
 * <p>
 * The Flink sink is given a single element at a time. Adding a single element
 * at a time to Gaffer would be really inefficient so we add these individual
 * elements to a blocking queue that we simultaneously add elements to whilst
 * the Gaffer Store is consuming them.
 * </p>
 * <p>
 * The queue is a {@link java.util.concurrent.BlockingQueue} with a maximum size to prevent the queue
 * from getting too large and running out of memory. If the maximum size is reached
 * Flink will be blocked from adding elements to the queue. The maximum size of
 * the queue can be configured using the operation option:
 * gaffer.flink.operation.handler.max-queue-size.
 * By default the maximum size is 1,000,000.
 * The blocking queue is only blocked on addition, it does not cause the Gaffer
 * Store to block if the queue is empty. In the case where a Kafka queue has
 * just a single Gaffer element the Store can immediately add this rather than
 * blocking and waiting for a batch to fill up. A side affect of this is that
 * AddElements operation may complete if no elements are added to the queue for
 * some time. In this situation we just restart the AddElements operation next
 * time an element is received.
 * </p>
 */
@SuppressFBWarnings(value = "SE_TRANSIENT_FIELD_NOT_RESTORED", justification = "There are null checks that will initialise the fields")
public class GafferAdder implements Serializable {
    private static final long serialVersionUID = -3418606107861031989L;
    public static final int MAX_QUEUE_SIZE_DEFAULT = 1000000;

    private final String graphId;
    private final byte[] schema;
    private final Properties properties;

    private final boolean validate;
    private final boolean skipInvalid;
    private final int maxQueueSize;

    private transient Store store;
    private transient ConsumableBlockingQueue<Element> queue;
    private transient boolean restart;

    public <OP extends Validatable & Operation> GafferAdder(final OP operation, final Store store) {
        this.store = store;
        this.validate = operation.isValidate();
        this.skipInvalid = operation.isSkipInvalidElements();
        final String maxQueueSizeOption = operation.getOption(FlinkConstants.MAX_QUEUE_SIZE);
        this.maxQueueSize = null != maxQueueSizeOption ? Integer.parseInt(maxQueueSizeOption) : MAX_QUEUE_SIZE_DEFAULT;
        graphId = store.getGraphId();
        schema = store.getSchema().toCompactJson();
        properties = store.getProperties().getProperties();
    }

    public void initialise() {
        if (null == store) {
            store = Store.createStore(graphId, Schema.fromJson(schema), StoreProperties.loadStoreProperties(properties));
        }
    }

    public void add(final Element element) {
        if (null == element) {
            return;
        }

        if (null == queue) {
            queue = new ConsumableBlockingQueue<>(maxQueueSize);
            restart = true;
        }

        try {
            queue.put(element);
        } catch (final InterruptedException e) {
            throw new RuntimeException("Interrupted while waiting to add an element to the queue", e);
        }

        if (restart && !queue.isEmpty()) {
            restart = false;
            store.runAsync(() -> {
                try {
                    store.execute(new AddElements.Builder()
                                    .input(queue)
                                    .validate(validate)
                                    .skipInvalidElements(skipInvalid)
                                    .build(),
                            new Context(new User()));
                    restart = true;
                } catch (final OperationException e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            });
        }
    }
}
