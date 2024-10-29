/*
 * Copyright 2021-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.operation;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSetter;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.koryphe.Since;

/**
 * {@link IFederationOperation} interface is for special operations used to
 * configure/manipulate/control federation.
 * It has no intended function outside of federation and should only be handled
 * by the {@link uk.gov.gchq.gaffer.federatedstore.FederatedStore}.
 *
 * @deprecated Concept of a FederatedOperation class will not exist from 2.4.0,
 *             all federation specifics are handled via operation options.
 */
@Deprecated
@Since("2.0.0")
public interface IFederationOperation extends Operation {

    @JsonGetter("userRequestingAdminUsage")
    @JsonInclude(value = JsonInclude.Include.NON_DEFAULT)
    boolean isUserRequestingAdminUsage();

    @JsonSetter("userRequestingAdminUsage")
    Operation setUserRequestingAdminUsage(final boolean adminRequest);

    abstract class BaseBuilder<OP extends IFederationOperation, B extends Operation.BaseBuilder<OP, ?>> extends Operation.BaseBuilder<OP, B> {
        protected BaseBuilder(final OP op) {
            super(op);
        }

        public B setUserRequestingAdminUsage(final boolean adminRequest) {
            this._getOp().setUserRequestingAdminUsage(adminRequest);
            return _self();
        }
    }

}
