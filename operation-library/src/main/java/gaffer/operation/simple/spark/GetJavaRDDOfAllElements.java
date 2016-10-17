package gaffer.operation.simple.spark;

import org.apache.spark.api.java.JavaSparkContext;

public class GetJavaRDDOfAllElements extends AbstractGetJavaRDD<Void> {

    public GetJavaRDDOfAllElements() {
    }

    public GetJavaRDDOfAllElements(final JavaSparkContext sparkContext) {
        setJavaSparkContext(sparkContext);
    }

    public abstract static class BaseBuilder<CHILD_CLASS extends BaseBuilder<?>>
            extends AbstractGetJavaRDD.BaseBuilder<GetJavaRDDOfAllElements, Void, CHILD_CLASS> {

        public BaseBuilder() {
            this(new GetJavaRDDOfAllElements());
        }

        public BaseBuilder(final GetJavaRDDOfAllElements op) {
            super(op);
        }
    }

    public static final class Builder extends BaseBuilder<Builder> {

        public Builder() {
        }

        public Builder(final GetJavaRDDOfAllElements op) {
            super(op);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
