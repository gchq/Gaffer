/*
 * Copyright 2018 Crown Copyright
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

package uk.gov.gchq.gaffer.store;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.store.operation.OperationChainValidator;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.ViewValidator;
import uk.gov.gchq.gaffer.user.User;

import java.util.Set;

public class SchemaOperationChainValidator extends OperationChainValidator {

    Schema schema;

    public SchemaOperationChainValidator(final ViewValidator viewValidator) {
        super(viewValidator);
    }

    public SchemaOperationChainValidator(final ViewValidator viewValidator, final Schema schema) {
        super(viewValidator);
        this.schema = schema;
    }

    public void setSchema(final Schema schema) {
        this.schema = schema;
    }

    @Override
    protected Schema getSchema(final Operation operation, final User user, final Store store) {
        return schema;
    }

    @Override
    protected Set<StoreTrait> getStoreTraits(final Store store) {
        return StoreTrait.ALL_TRAITS;
    }
}
