/**
 * Copyright 2015 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gaffer.graph.transform.impl;

import gaffer.graph.transform.Transform;
import gaffer.graph.wrappers.GraphElementWithStatistics;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A list of {@link Transform}s.
 */
public class ListOfTransforms implements Transform {

    private static final long serialVersionUID = -1899549289571792276L;
    private List<Transform> list = new ArrayList<Transform>();

    public ListOfTransforms(List<Transform> transforms) {
        list.addAll(transforms);
    }

    public ListOfTransforms(Transform... transforms) {
        Collections.addAll(list, transforms);
    }

    public void clear() {
        list.clear();
    }

    @Override
    public GraphElementWithStatistics transform(GraphElementWithStatistics graphElementWithStatistics) {
        for (Transform transform : list) {
            graphElementWithStatistics = transform.transform(graphElementWithStatistics);
        }
        return graphElementWithStatistics;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(list.size());
        for (Transform transform : list) {
            Text.writeString(out, transform.getClass().getName());
            transform.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        list.clear();
        int listSize = in.readInt();
        for (int i = 0; i < listSize; i++) {
            String className = Text.readString(in);
            try {
                Transform transform = (Transform) Class.forName(className).newInstance();
                transform.readFields(in);
                list.add(transform);
            } catch (InstantiationException e) {
                throw new IOException("Exception deserialising writable: " + e);
            } catch (IllegalAccessException e) {
                throw new IOException("Exception deserialising writable: " + e);
            } catch (ClassNotFoundException e) {
                throw new IOException("Exception deserialising writable: " + e);
            } catch (ClassCastException e) {
                throw new IOException("Exception deserialising writable: " + e);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ListOfTransforms that = (ListOfTransforms) o;

        if (list != null ? !list.equals(that.list) : that.list != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return list != null ? list.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "ListOfTransforms{" +
                "list=" + list +
                '}';
    }
}
