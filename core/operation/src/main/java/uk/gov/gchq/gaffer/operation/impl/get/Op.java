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

package uk.gov.gchq.gaffer.operation.impl.get;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.GetWalks;
import uk.gov.gchq.gaffer.operation.impl.If;
import uk.gov.gchq.gaffer.operation.impl.While;
import uk.gov.gchq.gaffer.operation.impl.compare.Sort;
import uk.gov.gchq.gaffer.operation.impl.output.ToCsv;
import uk.gov.gchq.gaffer.operation.impl.output.ToSet;
import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiElementIdInput;
import uk.gov.gchq.gaffer.operation.io.MultiEntityIdInput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.operation.util.OperationUtil;

public class Op {
    public static OperationChain<Void> chain() {
        return new OperationChain<>();
    }

    public static OpBuilder input(final Object... input) {
        return OpBuilder.input(input);
    }

    public static GetAdjacentIds hop(final String... edges) {
        return getAdjacentIds(edges);
    }

    public static GetAdjacentIds getAdjacentIds(final String... edges) {
        return new GetAdjacentIds().edges(edges);
    }

    public static GetWalks getWalks() {
        return new GetWalks();
    }

    public static GetWalks walk() {
        return getWalks();
    }

    public static GetElements get() {
        return getElements();
    }

    public static GetElements getEntities(final String... entities) {
        return get().entities(entities);
    }

    public static GetElements getEdges(final String... edges) {
        return get().edges(edges);
    }

    public static GetElements getElements() {
        return new GetElements();
    }

    public static <I, O> If<I, O> condition() {
        return _if();
    }

    public static <I, O> If<I, O> _if() {
        return new If<>();
    }

    public static <I, O> While<I, O> repeat() {
        return _while();
    }

    public static While<Void, Void> repeat(final Operation op) {
        return _while(op);
    }

    public static <I> While<I, Void> repeat(final Input<I> op) {
        return _while(op);
    }

    public static <O> While<Void, O> repeat(final Output<O> op) {
        return _while(op);
    }

    public static <I, O> While<I, O> repeat(final InputOutput<I, O> op) {
        return _while(op);
    }

    public static <I, O> While<I, O> _while() {
        return new While<>();
    }

    public static While<Void, Void> _while(final Operation op) {
        return new While.Builder<Void, Void>().operation(op).build();
    }

    public static <I> While<I, Void> _while(final Input<I> op) {
        return new While.Builder<I, Void>().operation(op).build();
    }

    public static <O> While<Void, O> _while(final Output<O> op) {
        return new While.Builder<Void, O>().operation(op).build();
    }

    public static <I, O> While<I, O> _while(final InputOutput<I, O> op) {
        return new While.Builder<I, O>().operation(op).build();
    }

    public static <T> ToSet<T> deduplicate() {
        return toSet();
    }

    public static <T> ToSet<T> toSet() {
        return new ToSet<>();
    }

    public static Sort sort() {
        return new Sort();
    }

    public static ToCsv toCsv() {
        return new ToCsv();
    }

    public static class OpBuilder {
        private OperationChain chain = new OperationChain();
        private Object tmpInput;

        private static OpBuilder input(Object... input) {
            final OpBuilder opChain = new OpBuilder();
            if (1 == input.length) {
                opChain.tmpInput = input[0];
            }
            return opChain;
        }

        private static OpBuilder input(Iterable input) {
            final OpBuilder opChain = new OpBuilder();
            opChain.tmpInput = input;
            return opChain;
        }

        public OperationChain<Void> then(final Operation op) {
            setInput(op);
            return new OperationChain<>(op);
        }

        public <T> OperationChain<T> then(final Output<T> op) {
            setInput(op);
            return new OperationChain<>(op);
        }

        public GetElements getEntities(final String... entities) {
            return Op.getEntities(entities);
        }

        public GetElements getEdges(final String... edges) {
            return Op.getEdges(edges);
        }

        public GetAdjacentIds hop(final String... edges) {
            return Op.hop(edges);
        }

        public ToSet dedup() {
            return Op.toSet();
        }

        private void setInput(final Operation op) {
            if (null != tmpInput) {
                if (op instanceof Input) {
                    if (op instanceof MultiInput) {
                        if (op instanceof MultiEntityIdInput) {
                            setInput((MultiEntityIdInput) op);
                        } else if (op instanceof MultiElementIdInput) {
                            setInput((MultiElementIdInput) op);
                        } else {
                            setInput((MultiInput) op);
                        }
                    } else {
                        setInput(((Input) op));
                    }
                }
                tmpInput = null;
            }
        }

        private void setInput(final Input op) {
            if (null != tmpInput) {
                op.setInput(tmpInput);
                tmpInput = null;
            }
        }

        private void setInput(final MultiInput op) {
            if (null != tmpInput) {
                if (tmpInput instanceof Object[]) {
                    op.setInput(((Object[]) tmpInput));
                } else if (tmpInput instanceof Iterable) {
                    op.setInput(tmpInput);
                } else {
                    op.setInput(new Object[]{tmpInput});
                }
                tmpInput = null;
            }
        }

        private void setInput(final MultiElementIdInput op) {
            if (null != tmpInput) {
                if (tmpInput instanceof Object[]) {
                    op.setInputFromVerticesAndIds(((Object[]) tmpInput));
                } else if (tmpInput instanceof Iterable) {
                    op.setInput(OperationUtil.toElementIds(tmpInput));
                } else {
                    op.setInputFromVerticesAndIds(new Object[]{tmpInput});
                }
                tmpInput = null;
            }
        }

        private void setInput(final MultiEntityIdInput op) {
            if (null != tmpInput) {
                if (tmpInput instanceof Object[]) {
                    op.setInputFromVerticesAndIds(((Object[]) tmpInput));
                } else if (tmpInput instanceof Iterable) {
                    op.setInput(OperationUtil.toEntityIds(tmpInput));
                } else {
                    op.setInputFromVerticesAndIds(new Object[]{tmpInput});
                }
                tmpInput = null;
            }
        }
    }
}
