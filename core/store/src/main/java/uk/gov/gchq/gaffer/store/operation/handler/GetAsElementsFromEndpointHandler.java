/*
 * Copyright 2018-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation.handler;

import com.google.gson.JsonParser;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAsElementsFromEndpoint;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class GetAsElementsFromEndpointHandler implements OperationHandler<GetAsElementsFromEndpoint> {

    @Override
    public Iterable<? extends Element> doOperation(final GetAsElementsFromEndpoint operation, final Context context, final Store store) throws OperationException {
        final Function<Iterable<? extends String>, Iterable<? extends Element>> elementGenerator =
                createElementGenerator(operation.getElementGenerator());
        final List<String> jsonElementAsStringList = new ArrayList<>();
        final String jsonFromEndpoint = getJsonFromEndpoint(operation.getEndpoint());

        new JsonParser()
                .parse(jsonFromEndpoint)
                .getAsJsonArray()
                .forEach(jsonObject -> jsonElementAsStringList.add(jsonObject.toString()));

        return elementGenerator.apply(jsonElementAsStringList);
    }

    private String getJsonFromEndpoint(final String endpoint) throws OperationException {
        final StringBuffer response = new StringBuffer();
        try (final BufferedReader in = new BufferedReader(new InputStreamReader(new URL(endpoint).openStream()))) {
            String readLine;
            while ((readLine = in.readLine()) != null) {
                response.append(readLine);
            }
        } catch (final IOException e) {
            throw new OperationException("Exception reading endpoint", e);
        }
        return response.toString();
    }

    private Function<Iterable<? extends String>, Iterable<? extends Element>> createElementGenerator(
            final Class<? extends Function<Iterable<? extends String>, Iterable<? extends Element>>> elementGeneratorClass) throws OperationException {
        try {
            return elementGeneratorClass.newInstance();
        } catch (final InstantiationException | IllegalAccessException e) {
            throw new OperationException(e);
        }
    }
}
