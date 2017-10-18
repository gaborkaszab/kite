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
package org.kitesdk.data;

import java.io.IOException;

/**
 * <p>
 * Exception thrown for dataset IO-related failures.
 * </p>
 *
 * @since 0.9.0
 *
 * @deprecated Kite DataSet API is deprecated as of CDH6.0.0 and will be removed from CDH in an upcoming release.
 * Cloudera recommends that you use the equivalent API in Spark instead of the Kite DataSet API.
 */
@Deprecated
public class DatasetIOException extends DatasetException {

  private final IOException ioException;
  
  public DatasetIOException(String message, IOException root) {
    super(message, root);
    this.ioException = root;
  }
  
  public IOException getIOException() {
    return ioException;
  }
}
