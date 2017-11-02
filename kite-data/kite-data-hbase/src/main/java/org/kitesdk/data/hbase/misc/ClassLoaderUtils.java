/**
 * Copyright 2017 Cloudera Inc.
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

package org.kitesdk.data.hbase.misc;

/**
 * Contains utility methods to modify class loading behavior.
 */
public class ClassLoaderUtils {

  /**
   * A variation on the java.lang.Class.forName() version of
   * this class that attempts to first load the class using
   * the thread context classloader failing which it loads
   * the class using the current classloader.
   * @param className
   * @return
   * @throws ClassNotFoundException
   */
  public static Class<?> forName(String className)
    throws ClassNotFoundException {
    ClassLoader threadContextClassloader =
      Thread.currentThread().getContextClassLoader();
    try {
      if (threadContextClassloader != null) {
        return Class.forName(className, true, threadContextClassloader);
      }
    } catch (ClassNotFoundException cnfe) {
      // nothing on the thread context classloader
    }
    return Class.forName(className);
  }
}
