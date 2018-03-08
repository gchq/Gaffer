/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.flink.operation.handler.util;

public final class FlinkConstants {
    /**
     * Operation option key for skipping rebalancing between flatMap and sink.
     * This is false by default.
     */
    public static final String SKIP_REBALANCING = "gaffer.flink.operation.handler.skip-rebalancing";

    /**
     * Operation option key for setting the maximum queue size for adding to
     * a Gaffer store. If the queue size is exceeded Flink will be blocked
     * from adding to the queue until the Gaffer Store has consumed the elements.
     */
    public static final String MAX_QUEUE_SIZE = "gaffer.flink.operation.handler.max-queue-size";

    private FlinkConstants() {
        // private constructor to prevent users instantiating this class as it
        // only contains constants.
    }
}
