/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operationdeclaration;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Contains a list of Operations defined in a JSON file, referenced in the store.properties.
 * <p>
 * Used to add operation handlers.
 */
public class OperationDeclarations {
    protected static final JSONSerialiser JSON_SERIALISER = new JSONSerialiser();

    private List<OperationDeclaration> operations;

    public List<OperationDeclaration> getOperations() {
        return operations;
    }

    public void setOperations(final List<OperationDeclaration> operations) {
        this.operations = operations;
    }

    public static class Builder {
        private final OperationDeclarations instance;

        public Builder() {
            this.instance = new OperationDeclarations();
            this.instance.setOperations(new ArrayList<>());
        }

        public Builder declaration(final OperationDeclaration declaration) {
            this.instance.getOperations().add(declaration);
            return this;
        }

        public OperationDeclarations build() {
            return this.instance;
        }
    }

    public static OperationDeclarations fromPaths(final String paths) {
        final OperationDeclarations allDefinitions = new OperationDeclarations.Builder().build();

        try {
            for (final String pathStr : paths.split(",")) {
                final OperationDeclarations definitions;
                final Path path = Paths.get(pathStr);
                if (path.toFile().exists()) {
                    definitions = fromJson(Files.readAllBytes(path));
                } else {
                    definitions = fromJson(StreamUtil.openStream(OperationDeclarations.class, pathStr));
                }
                if (null != definitions && null != definitions.getOperations()) {
                    allDefinitions.getOperations().addAll(definitions.getOperations());
                }
            }
        } catch (final IOException e) {
            throw new SchemaException("Failed to load element definitions from paths: " + paths, e);
        }

        return allDefinitions;
    }

    public static OperationDeclarations fromJson(final byte[] json) {
        try {
            return JSON_SERIALISER.deserialise(json, OperationDeclarations.class);
        } catch (SerialisationException e) {
            throw new SchemaException("Failed to load element definitions from bytes", e);
        }
    }

    public static OperationDeclarations fromJson(final InputStream inputStream) {
        try {
            return JSON_SERIALISER.deserialise(inputStream, OperationDeclarations.class);
        } catch (SerialisationException e) {
            throw new SchemaException("Failed to load element definitions from bytes", e);
        }
    }
}
