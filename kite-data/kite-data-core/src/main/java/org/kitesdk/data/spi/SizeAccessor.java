/*
 * Copyright 2014 Cloudera.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data.spi;

/**
 * This interface is for classes that can report the storage size of the entity that
 * they represent.
 *
 * @deprecated Kite DataSet API is deprecated as of CDH6.0.0 and will be removed from CDH in an upcoming release.
 * Cloudera recommends that you use the equivalent API in Spark instead of the Kite DataSet API.
 */
@Deprecated
public interface SizeAccessor {
  /**
   * @return the number of bytes of persistent storage used for this entity
   */
  public long getSize();
}
