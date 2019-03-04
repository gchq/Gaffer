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

import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.util.Hook;

/**
 * <p>
 * A {@code GraphHook} can be registered with a {@link uk.gov.gchq.gaffer.graph.Graph} and will be
 * triggered before and after operation chains are executed on the graph.
 * </p>
 * <p>
 * If an error occurs whilst running the operation chain the onFailure method will be
 * triggered.
 * </p>
 * <p>
 * The {@link OperationChain} parameter that is given to the graph hook is
 * a clone of the original and can be adapted or optimised in any GraphHook
 * implementation. So please note that if you want to access the original
 * operation chain without modifications then you can access if from
 * {@link Context#getOriginalOpChain()}. This original operation chain should not
 * be modified.
 * </p>
 */
public interface GraphHook extends Hook {
}
