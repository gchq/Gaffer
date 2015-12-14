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
package gaffer.accumulo.iterators;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * A class which holds a column qualifier, a column visibility and a {@link Value}.
 */
public class ColumnQualifierColumnVisibilityValueTriple implements Writable {
	
	String columnQualifier;
	String columnVisibility;
	Value value = new Value();
	
	public ColumnQualifierColumnVisibilityValueTriple() {}
	
	public ColumnQualifierColumnVisibilityValueTriple(String columnQualifier, String columnVisibility, Value value) {
		this.columnQualifier = columnQualifier;
		this.columnVisibility = columnVisibility;
		this.value = value;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.columnQualifier = Text.readString(in);
		this.columnVisibility = Text.readString(in);
		value.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, this.columnQualifier);
		Text.writeString(out, this.columnVisibility);
		value.write(out);
	}
	
	public String getColumnQualifier() {
		return columnQualifier;
	}

	public void setColumnQualifier(String columnQualifier) {
		this.columnQualifier = columnQualifier;
	}

	public String getColumnVisibility() {
		return columnVisibility;
	}

	public void setColumnVisibility(String columnVisibility) {
		this.columnVisibility = columnVisibility;
	}

	public Value getValue() {
		return value;
	}

	public void setValue(Value value) {
		this.value = value;
	}

}
