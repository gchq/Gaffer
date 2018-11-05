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

package uk.gov.gchq.gaffer.operation;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.koryphe.impl.function.ToLong;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;

import static org.junit.Assert.assertEquals;

public class AltOperationChainTest {
    @Test
    public void shouldSeedQuery() throws Exception {
        final OperationChain oldWay = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input("seed1")
                        .view(new View.Builder()
                                .edge("group1", new ViewElementDefinition.Builder()
                                        .groupBy("propX")
                                        .preAggregationFilter(new ElementFilter.Builder()
                                                .select("propX")
                                                .execute(new IsLessThan(11))
                                                .build())
                                        .postAggregationFilter(new ElementFilter.Builder()
                                                .select("propY")
                                                .execute(new IsLessThan(10))
                                                .build())
                                        .transformer(new ElementTransformer.Builder()
                                                .select("propA")
                                                .execute(new ToLong())
                                                .project("propB")
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();

        final OperationChain newWay = new OperationChain().query0()
                .getElements1("seed1")
                .edge2("group1")
                .filter3("propX")
                .predicate4("uk.gov.gchq.koryphe.impl.predicate.IsLessThan", 11)
                .groupBy3("propX")
                .filter3("propY")
                .predicate4("uk.gov.gchq.koryphe.impl.predicate.IsLessThan", 10)
                .transform3("propA", "propB")
                .function4("uk.gov.gchq.koryphe.impl.function.ToLong")
                .build();

        compare(oldWay, newWay);
    }

    private void compare(final OperationChain<CloseableIterable<? extends Element>> oldWay, final OperationChain newWay) throws SerialisationException {
        final String expected = new String(JSONSerialiser.serialise(oldWay, true));
        final String actual = new String(JSONSerialiser.serialise(newWay, true));
        assertEquals(expected, actual);
    }

    @Test
    public void shouldBuildNothingWhenEmpty() throws Exception {
        final OperationChain oldWay = new OperationChain();
        final OperationChain newWay = new OperationChain().query0().build();
        compare(oldWay, newWay);
    }

    @Test
    public void shouldAddGetElementsOperation() throws Exception {
        final OperationChain oldWay = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input("seed1")
                        .build())
                .build();

        final OperationChain newWay = new OperationChain().query0()
                .getElements1("seed1")
                .build();

        compare(oldWay, newWay);
    }

    @Test
    public void shouldGetElementsAndFilterOnEdge() throws Exception {
        final OperationChain oldWay = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input("seed1")
                        .view(new View.Builder()
                                .edge("group1")
                                .build())
                        .build())
                .build();

        final OperationChain newWay = new OperationChain().query0()
                .getElements1("seed1")
                .edge2("group1")
                .build();

        compare(oldWay, newWay);
    }

    @Test
    public void shouldGetElementsAndMultiFilterOnEdge() throws Exception {
        final OperationChain oldWay = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input("seed1")
                        .view(new View.Builder()
                                .edge("group1")
                                .edge("group2")
                                .build())
                        .build())
                .build();

        final OperationChain newWay = new OperationChain().query0()
                .getElements1("seed1")
                .edge2("group1")
                .edge2("group2")
                .build();

        compare(oldWay, newWay);
    }


    @Test
    public void shouldGetElementsAndGroupBy() throws Exception {
        final OperationChain oldWay = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input("seed1")
                        .view(new View.Builder()
                                .edge("group1", new ViewElementDefinition.Builder()
                                        .groupBy("propX")
                                        .build())
                                .build())
                        .build())
                .build();

        final OperationChain newWay = new OperationChain().query0()
                .getElements1("seed1")
                .edge2("group1")
                .groupBy3("propX")
                .build();

        compare(oldWay, newWay);
    }


    @Test
    public void shouldGetElementsAndMultiGroupBy() throws Exception {
        final OperationChain oldWay = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input("seed1")
                        .view(new View.Builder()
                                .edge("group1", new ViewElementDefinition.Builder()
                                        .groupBy("propX")
                                        .groupBy("propY")
                                        .build())
                                .build())
                        .build())
                .build();

        final OperationChain newWay = new OperationChain().query0()
                .getElements1("seed1")
                .edge2("group1")
                .groupBy3("propX")
                .groupBy3("propY")
                .build();

        compare(oldWay, newWay);
    }

    @Test
    public void shouldGetElementsAndPreFilter() throws Exception {
        final OperationChain oldWay = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input("seed1")
                        .view(new View.Builder()
                                .edge("group1", new ViewElementDefinition.Builder()
                                        .groupBy("propX")
                                        .preAggregationFilter(new ElementFilter.Builder()
                                                .select("propX")
                                                .execute(new IsLessThan(11))
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();

        OperationChain newWay = new OperationChain().query0()
                .getElements1("seed1")
                .edge2("group1")
                .filter3("propX")
                .predicate4("uk.gov.gchq.koryphe.impl.predicate.IsLessThan", 11)
                .groupBy3("propX")
                .build();

        compare(oldWay, newWay);
    }

    @Test
    public void shouldGetElementsAndMultiPreFilter() throws Exception {
        final OperationChain oldWay = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input("seed1")
                        .view(new View.Builder()
                                .edge("group1", new ViewElementDefinition.Builder()
                                        .groupBy("propX")
                                        .preAggregationFilter(new ElementFilter.Builder()
                                                .select("propX")
                                                .execute(new IsLessThan(11))
                                                .select("propXX")
                                                .execute(new IsLessThan(111))
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();

        OperationChain newWay = new OperationChain().query0()
                .getElements1("seed1")
                .edge2("group1")
                .filter3("propX")
                .predicate4("uk.gov.gchq.koryphe.impl.predicate.IsLessThan", 11)
                .filter3("propXX")
                .predicate4("uk.gov.gchq.koryphe.impl.predicate.IsLessThan", 111)
                .groupBy3("propX")
                .build();

        compare(oldWay, newWay);

    }

    @Test
    public void shouldGetElementsAndPreAndPostFilter() throws Exception {
        final OperationChain oldWay = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input("seed1")
                        .view(new View.Builder()
                                .edge("group1", new ViewElementDefinition.Builder()
                                        .groupBy("propX")
                                        .preAggregationFilter(new ElementFilter.Builder()
                                                .select("propX")
                                                .execute(new IsLessThan(11))
                                                .build())
                                        .postAggregationFilter(new ElementFilter.Builder()
                                                .select("propY")
                                                .execute(new IsLessThan(10))
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();

        final OperationChain newWay = new OperationChain().query0()
                .getElements1("seed1")
                .edge2("group1")
                .filter3("propX")
                .predicate4("uk.gov.gchq.koryphe.impl.predicate.IsLessThan", 11)
                .groupBy3("propX")
                .filter3("propY")
                .predicate4("uk.gov.gchq.koryphe.impl.predicate.IsLessThan", 10)
                .build();

        compare(oldWay, newWay);
    }

    @Test
    public void shouldGetElementsAndPreAndMultiPostFilter() throws Exception {
        final OperationChain oldWay = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input("seed1")
                        .view(new View.Builder()
                                .edge("group1", new ViewElementDefinition.Builder()
                                        .groupBy("propX")
                                        .preAggregationFilter(new ElementFilter.Builder()
                                                .select("propX")
                                                .execute(new IsLessThan(11))
                                                .build())
                                        .postAggregationFilter(new ElementFilter.Builder()
                                                .select("propY")
                                                .execute(new IsLessThan(10))
                                                .select("propYY")
                                                .execute(new IsLessThan(100))
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();

        final OperationChain newWay = new OperationChain().query0()
                .getElements1("seed1")
                .edge2("group1")
                .filter3("propX")
                .predicate4("uk.gov.gchq.koryphe.impl.predicate.IsLessThan", 11)
                .groupBy3("propX")
                .filter3("propY")
                .predicate4("uk.gov.gchq.koryphe.impl.predicate.IsLessThan", 10)
                .filter3("propYY")
                .predicate4("uk.gov.gchq.koryphe.impl.predicate.IsLessThan", 100)
                .build();

        compare(oldWay, newWay);
    }

    @Test
    public void shouldGetElementsAndPostFilter() throws Exception {

        final OperationChain oldWay = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input("seed1")
                        .view(new View.Builder()
                                .edge("group1", new ViewElementDefinition.Builder()
                                        .groupBy("propX")
                                        .postAggregationFilter(new ElementFilter.Builder()
                                                .select("propY")
                                                .execute(new IsLessThan(10))
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();

        final OperationChain newWay = new OperationChain().query0()
                .getElements1("seed1")
                .edge2("group1")
                .groupBy3("propX")
                .filter3("propY")
                .predicate4("uk.gov.gchq.koryphe.impl.predicate.IsLessThan", 10)
                .build();

        compare(oldWay, newWay);
    }

    @Test
    public void shouldGetElementsAndTransform() throws Exception {
        final OperationChain oldWay = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input("seed1")
                        .view(new View.Builder()
                                .edge("group1", new ViewElementDefinition.Builder()
                                        .transformer(new ElementTransformer.Builder()
                                                .select("propA")
                                                .execute(new ToLong())
                                                .project("propB")
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();

        final OperationChain newWay = new OperationChain().query0()
                .getElements1("seed1")
                .edge2("group1")
                .transform3("propA", "propB")
                .function4("uk.gov.gchq.koryphe.impl.function.ToLong")
                .build();

        compare(oldWay, newWay);
    }

    @Test
    public void shouldGetElementsAndMultiTransform() throws Exception {
        final OperationChain oldWay = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input("seed1")
                        .view(new View.Builder()
                                .edge("group1", new ViewElementDefinition.Builder()
                                        .transformer(new ElementTransformer.Builder()
                                                .select("propA")
                                                .execute(new ToLong())
                                                .project("propB")
                                                .select("propAA")
                                                .execute(new ToLong())
                                                .project("propBB")
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();

        final OperationChain newWay = new OperationChain().query0()
                .getElements1("seed1")
                .edge2("group1")
                .transform3("propA", "propB")
                .function4("uk.gov.gchq.koryphe.impl.function.ToLong")
                .transform3("propAA", "propBB")
                .function4("uk.gov.gchq.koryphe.impl.function.ToLong")
                .build();

        compare(oldWay, newWay);
    }

    @Test
    public void ShouldKoryphePredicate() throws Exception {
        final OperationChain oldWay = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input("seed1")
                        .view(new View.Builder()
                                .edge("group1", new ViewElementDefinition.Builder()
                                        .preAggregationFilter(new ElementFilter.Builder()
                                                .select("propY")
                                                .execute(new IsLessThan(10))
                                                .build())
                                        .transformer(new ElementTransformer.Builder()
                                                .select("propA")
                                                .execute(new ToLong())
                                                .project("propB")
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();

        final OperationChain newWay = new OperationChain().query0()
                .getElements1("seed1")
                .edge2("group1")
                .filter3("propY")
                .predicate4("uk.gov.gchq.koryphe.impl.predicate.IsLessThan", 10)
                .transform3("propA", "propB")
                .function4("uk.gov.gchq.koryphe.impl.function.ToLong") //ok 1st // auto generate Function4 class with all function names (Ahhhh)
                .build();
        compare(oldWay, newWay);
    }
}