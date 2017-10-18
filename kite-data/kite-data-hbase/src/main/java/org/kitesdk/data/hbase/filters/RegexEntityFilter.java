/**
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data.hbase.filters;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.FieldMapping;
import org.kitesdk.data.FieldMapping.MappingType;
import org.kitesdk.data.hbase.impl.EntitySchema;
import org.kitesdk.data.hbase.impl.EntitySerDe;

import org.apache.hadoop.hbase.filter.Filter;

/**
 * An EntityFilter that will perform a regular expression filter on an entity's
 * field.
 *
 * @deprecated Kite DataSet API is deprecated as of CDH6.0.0 and will be removed from CDH in an upcoming release.
 * Cloudera recommends that you use the equivalent API in Spark instead of the Kite DataSet API.
 */
@Deprecated
public class RegexEntityFilter implements EntityFilter {

  private final Filter filter;

  public RegexEntityFilter(EntitySchema entitySchema,
      EntitySerDe<?> entitySerDe, String fieldName, String regex,
      boolean isEqual) {
    FieldMapping fieldMapping = entitySchema.getColumnMappingDescriptor()
        .getFieldMapping(fieldName);
    if (fieldMapping.getMappingType() != MappingType.COLUMN) {
      throw new DatasetException(
          "SingleColumnValueFilter only compatible with COLUMN mapping types.");
    }

    this.filter = constructFilter(regex, isEqual, fieldMapping);
  }

  private Filter constructFilter(String regex, boolean isEqual, FieldMapping fieldMapping) {
    byte[] family = fieldMapping.getFamily();
    byte[] qualifier = fieldMapping.getQualifier();
    CompareOperator compareOperator = isEqual ? CompareOperator.EQUAL : CompareOperator.NOT_EQUAL;
    RegexStringComparator comparator = new RegexStringComparator(regex);

    return new SingleColumnValueFilter(family, qualifier, compareOperator, comparator);
  }

  public RegexEntityFilter(EntitySchema entitySchema,
      EntitySerDe<?> entitySerDe, String fieldName, String regex) {
    this(entitySchema, entitySerDe, fieldName, regex, true);
  }

  public Filter getFilter() {
    return filter;
  }
}
