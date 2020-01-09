/*
 * Copyright 2019 Crown Copyright
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

import com.google.common.collect.Lists;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.exception.UnauthorisedException;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.GlobalViewElementDefinition;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.Map;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.koryphe.impl.function.DivideBy;
import uk.gov.gchq.koryphe.impl.function.Identity;
import uk.gov.gchq.koryphe.impl.function.ParseDate;
import uk.gov.gchq.koryphe.impl.function.ToInteger;
import uk.gov.gchq.koryphe.impl.function.ToLong;
import uk.gov.gchq.koryphe.impl.function.ToString;
import uk.gov.gchq.koryphe.tuple.function.TupleAdaptedFunction;

import java.util.function.Function;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class FunctionAuthoriserTest {

    @Test
    public void shouldNotAllowOperationWhichContainsBlacklistedFunction() {
        // Given
        OperationChain badOperation = generateOperation(Identity.class);
        FunctionAuthoriser functionAuthoriser = new FunctionAuthoriser();

        // When
        functionAuthoriser.setUnauthorisedFunctions(Lists.newArrayList(Identity.class));

        // Then
        try {
            functionAuthoriser.preExecute(badOperation, new Context());
            fail("Exception expected");
        } catch (final UnauthorisedException e) {
            assertEquals("Operation chain contained an unauthorised function: uk.gov.gchq.koryphe.impl.function.Identity", e.getMessage());
        }
    }

    @Test
    public void shouldNotAllowGetElementsOperationWithBlacklistedFunctionsInTheView() {
        final OperationChain<CloseableIterable<? extends Element>> viewOperation = new OperationChain.Builder().first(new GetElements.Builder()
                .view(new View.Builder()
                        .globalElements(new GlobalViewElementDefinition.Builder()
                                .transformFunctions(Lists.newArrayList(new TupleAdaptedFunction(new String[]{"input"}, new DivideBy(6), new String[]{"output"})))
                                .build())
                        .build())
                .build())
                .build();

        FunctionAuthoriser functionAuthoriser = new FunctionAuthoriser();

        // When
        functionAuthoriser.setUnauthorisedFunctions(Lists.newArrayList(DivideBy.class));


        // Then
        try {
            functionAuthoriser.preExecute(viewOperation, new Context());
            fail("Exception expected");
        } catch (final UnauthorisedException e) {
            assertEquals("Operation chain contained an unauthorised function: uk.gov.gchq.koryphe.impl.function.DivideBy", e.getMessage());
        }

    }

    @Test
    public void shouldAllowOperationChainWhichDoesNotContainAnyBlacklistedElements() {
        // Given
        OperationChain mapOperation = generateOperation(Identity.class, ToString.class);
        FunctionAuthoriser functionAuthoriser = new FunctionAuthoriser();

        // When
        functionAuthoriser.setUnauthorisedFunctions(Lists.newArrayList(DivideBy.class));

        // Then
        functionAuthoriser.preExecute(mapOperation, new Context());
        // No exceptions thrown
    }

    @Test
    public void shouldNotAllowOperationChainsWhichContainFunctionListedInTheBlacklistedPatterns() {
        // Given
        OperationChain chain = generateOperation(Identity.class, ToString.class);
        FunctionAuthoriser functionAuthoriser = new FunctionAuthoriser();

        // When
        functionAuthoriser.setUnauthorisedFunctionPatterns(Lists.newArrayList(Pattern.compile("uk.gov.gchq.koryphe.impl.function.To*")));

        // Then
        try {
            functionAuthoriser.preExecute(chain, new Context());
            fail("Exception expected");
        } catch (final UnauthorisedException e) {
            assertEquals("Operation chain contained an unauthorised function: uk.gov.gchq.koryphe.impl.function.ToString", e.getMessage());
        }
    }

    @Test
    public void shouldNotAllowOperationChainsWhichContainFunctionListedInBothWhitelistAndBlacklistPatterns() {
        // Given
        OperationChain chain = generateOperation(Identity.class, ToString.class);
        FunctionAuthoriser functionAuthoriser = new FunctionAuthoriser();

        // When
        functionAuthoriser.setAuthorisedFunctionPatterns(Lists.newArrayList(Pattern.compile("uk.gov.gchq.koryphe.impl.function")));
        functionAuthoriser.setUnauthorisedFunctionPatterns(Lists.newArrayList(Pattern.compile("uk.gov.gchq.koryphe.impl.function.To*")));

        // Then
        try {
            functionAuthoriser.preExecute(chain, new Context());
            fail("Exception expected");
        } catch (final UnauthorisedException e) {
            assertEquals("Operation chain contained an unauthorised function: uk.gov.gchq.koryphe.impl.function.ToString", e.getMessage());
        }
    }

    @Test
    public void shouldNotAllowAnOperationChainWhichContainsFunctionWhichDoesNotExactlyMatchWhitelist() {
        // Given
        OperationChain mapOperation = generateOperation(Identity.class, ToString.class);
        FunctionAuthoriser functionAuthoriser = new FunctionAuthoriser();

        // When
        functionAuthoriser.setAuthorisedFunctionPatterns(Lists.newArrayList(Pattern.compile(Identity.class.getName())));

        // Then
        try {
            functionAuthoriser.preExecute(mapOperation, new Context());
            fail("Exception expected");
        } catch (final UnauthorisedException e) {
            assertEquals("Operation chain contained an unauthorised function: uk.gov.gchq.koryphe.impl.function.ToString", e.getMessage());
        }
    }

    @Test
    public void shouldNotAllowAnOperationChainWhichDoesNotMatchAPatternInTheWhiteList() {
        // Given
        OperationChain mapOperation = generateOperation(ToString.class, ToInteger.class, ParseDate.class);
        FunctionAuthoriser functionAuthoriser = new FunctionAuthoriser();

        // When
        functionAuthoriser.setAuthorisedFunctionPatterns(Lists.newArrayList(Pattern.compile("uk.gov.gchq.koryphe.impl.function.To*")));

        // Then
        try {
            functionAuthoriser.preExecute(mapOperation, new Context());
            fail("Exception expected");
        } catch (final UnauthorisedException e) {
            assertEquals("Operation chain contained an unauthorised function: uk.gov.gchq.koryphe.impl.function.ParseDate", e.getMessage());
        }
    }

    @Test
    public void shouldNotAllowOperationChainWhichContainsFunctionsWhichMatchWhitelistButAppearInBlacklist() {
        // Given
        OperationChain mapOperation = generateOperation(ToString.class, ToInteger.class, ParseDate.class);
        FunctionAuthoriser functionAuthoriser = new FunctionAuthoriser();

        // When
        functionAuthoriser.setUnauthorisedFunctions(Lists.newArrayList(ToInteger.class));
        functionAuthoriser.setAuthorisedFunctionPatterns(Lists.newArrayList(Pattern.compile("uk.gov.gchq.koryphe.impl.function.*")));

        // Then
        try {
            functionAuthoriser.preExecute(mapOperation, new Context());
            fail("Exception expected");
        } catch (final UnauthorisedException e) {
            assertEquals("Operation chain contained an unauthorised function: uk.gov.gchq.koryphe.impl.function.ToInteger", e.getMessage());
        }
    }

    @Test
    public void shouldNotAllowOperationChainWhichContainsNoFunctionsInBlacklistButDontMatchWhitelist() {
        // Given
        OperationChain mapOperation = generateOperation(ToString.class, ToInteger.class, ParseDate.class);
        FunctionAuthoriser functionAuthoriser = new FunctionAuthoriser();

        // When
        functionAuthoriser.setUnauthorisedFunctions(Lists.newArrayList(ToLong.class));
        functionAuthoriser.setAuthorisedFunctionPatterns(Lists.newArrayList(Pattern.compile("uk.gov.gchq.koryphe.impl.function.To*")));

        // Then
        try {
            functionAuthoriser.preExecute(mapOperation, new Context());
            fail("Exception expected");
        } catch (final UnauthorisedException e) {
            assertEquals("Operation chain contained an unauthorised function: uk.gov.gchq.koryphe.impl.function.ParseDate", e.getMessage());
        }
    }

    @Test
    public void shouldJsonSerialiseAndDeserialiseWithPopulatedFields() throws SerialisationException {
        String json = "" +
                "{" +
                    "\"class\": \"uk.gov.gchq.gaffer.graph.hook.FunctionAuthoriser\"," +
                    "\"unauthorisedFunctions\":[" +
                        "\"uk.gov.gchq.koryphe.impl.function.ToString\"" +
                    "]," +
                    "\"authorisedFunctionPatterns\":[" +
                        "\"uk.gov.gchq.koryphe.*\"" +
                    "]" +
                "}";

        final FunctionAuthoriser authoriser = new FunctionAuthoriser.Builder()
                .authorsisedPatterns(Lists.newArrayList("uk.gov.gchq.koryphe.*"))
                .unauthorisedFunctions(Lists.newArrayList(ToString.class))
                .build();

        JsonAssert.assertEquals(json, new String(JSONSerialiser.serialise(authoriser)));
    }

    @Test
    public void shouldJsonSerialiseAndDeserialiseWithNoFields() throws SerialisationException {
        String json = "" +
                "{" +
                    "\"class\": \"uk.gov.gchq.gaffer.graph.hook.FunctionAuthoriser\"" +
                "}";

        final FunctionAuthoriser authoriser = new FunctionAuthoriser();

        JsonAssert.assertEquals(json, new String(JSONSerialiser.serialise(authoriser)));
    }

    @Test
    public void shouldNotErrorWhenThereAreNoUnauthorisedFunctions() {
        // Given
        final OperationChain chain = generateOperation(Identity.class);

        // When
        final FunctionAuthoriser functionAuthoriser = new FunctionAuthoriser(null, null, Lists.newArrayList(Pattern.compile(Identity.class.getName())) );

        // Then
        functionAuthoriser.preExecute(chain, new Context());
        // no exceptions
    }

    @Test
    public void shouldNotErrorWhenNoAuthorisedPatternsAreProvidedInTheJson() {
        // Given
        final OperationChain chain = generateOperation(ToString.class);

        // When
        final FunctionAuthoriser functionAuthoriser = new FunctionAuthoriser(Lists.newArrayList(Identity.class), null, null);

        // Then
        functionAuthoriser.preExecute(chain, new Context());
        // no exceptions
    }

    private OperationChain generateOperation(final Class<? extends Function>... functionClasses) {
        final Map.Builder builder = new Map.Builder();
        try {
            final Map.OutputBuilder mob = builder.first(functionClasses[0].newInstance());

            boolean first = true;
            for (Class<? extends Function> functionClass : functionClasses) {
                if (first) {
                    first = false;
                    continue;
                }
                mob.then(functionClass.newInstance());
            }

            return new OperationChain(mob.build());

        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
