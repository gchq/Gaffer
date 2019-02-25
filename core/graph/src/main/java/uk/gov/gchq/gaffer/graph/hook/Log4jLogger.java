/*
 * Copyright 2016-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.graph.hook;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.graph.GraphRequest;

/**
 * A {@code Log4jLogger} is a simple {@link GraphHook} that sends logs of the
 * original operation chains executed by users on a graph to a {@link Logger}.
 */
@JsonPropertyOrder(alphabetic = true)
public class Log4jLogger implements GraphHook {
    private static final Logger LOGGER = LoggerFactory.getLogger(Log4jLogger.class);

    /**
     * Logs the operation chain and the user id.
     *
     * @param request GraphRequest containing the Operation and Context
     */
    @Override
    public void preExecute(final GraphRequest request) {
        LOGGER.info("Running {} as {}", request.getContext().getOriginalOperation(),
                request.getContext().getUser().getUserId());
    }

    @Override
    public <T> T onFailure(final T result, final GraphRequest request,
                           final Exception e) {
        LOGGER.warn("Failed to run {} as {}",
                request.getContext().getOriginalOperation(),
                request.getContext().getUser().getUserId());
        return result;
    }
}
