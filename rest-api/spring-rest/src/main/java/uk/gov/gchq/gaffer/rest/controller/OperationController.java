/*
 * Copyright 2016-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import uk.gov.gchq.gaffer.commonutil.CloseableUtil;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphRequest;
import uk.gov.gchq.gaffer.graph.GraphResult;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.rest.factory.UserFactory;
import uk.gov.gchq.gaffer.store.Context;

import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE_HEADER;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.JOB_ID_HEADER;

@RestController
@RequestMapping("/graph/operations")
public class OperationController {

    private GraphFactory graphFactory;
    private UserFactory userFactory;

    @Autowired
    public void setGraphFactory(final GraphFactory graphFactory) {
        this.graphFactory = graphFactory;
    }

    @Autowired
    public void setUserFactory(final UserFactory userFactory) {
        this.userFactory = userFactory;
    }

    @RequestMapping(value = "/execute",
            method = RequestMethod.POST,
            consumes = "application/json",
            produces = { "text/plain", "application/json" }

    )
    public ResponseEntity<Object> execute(@RequestBody final Operation operation) {
        Pair<Object, String> resultAndGraphId = _execute(OperationChain.wrap(operation), userFactory.createContext());
        return ResponseEntity.ok()
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .header(JOB_ID_HEADER, resultAndGraphId.getSecond())
                .body(resultAndGraphId.getFirst());

    }

    public Pair<Object, String> _execute(final OperationChain chain, final Context context) {
        Graph graph = this.graphFactory.createGraph();
        try {
            GraphResult result = graph.execute(new GraphRequest(chain, context));
            return new Pair<>(result.getResult(), result.getContext().getJobId());
        } catch (final OperationException e) {
            CloseableUtil.close(chain);
            final String message = null != e.getMessage() ? "Error executing opChain: " + e.getMessage() : "Error executing opChain";
            throw new GafferRuntimeException(message, e, e.getStatus());
        }
    }
}
