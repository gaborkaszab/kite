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
package org.kitesdk.data.hbase.avro.misc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

import org.junit.Test;
import org.kitesdk.data.hbase.misc.ClassLoaderUtils;

public class ClassLoaderUtilsTest {

  private void addJar(File file) throws Exception {
    Method method =
      URLClassLoader.class.getDeclaredMethod("addURL", new Class[]{URL.class});
    method.setAccessible(true);
    method.invoke(ClassLoader.getSystemClassLoader(),
                  new Object[]{file.toURI().toURL()});
  }
  @Test
  public void testThreadContextClassloader() throws Exception {
    ClassLoader currentClassLoader = this.getClass().getClassLoader();
    ClassLoader threadContextClassLoader =
      Thread.currentThread().getContextClassLoader();
    System.out.println("Current: " + currentClassLoader +
      ", TestContextClassLoader: " + threadContextClassLoader);
    assertEquals(currentClassLoader, threadContextClassLoader);

    // Adds the helloworld.jar to the system (!) classloader.
    URL helloworld = currentClassLoader.getResource("fixtures/helloworld.jar");
    addJar(new File(helloworld.getFile()));

    // Veify that we can load test.HellowWorld
    currentClassLoader.loadClass("test.HelloWorld");

    // Create a URLClassLoader that includes helloworldtccl.jar
    URL helloworldtccl = currentClassLoader.getResource(
      "fixtures/helloworldtccl.jar");
    URL[] urls = { helloworldtccl };
    ClassLoader urlClassLoader = URLClassLoader.newInstance(urls);
    // Veify that the URLClassLoader can load test.HellowWorldTccl
    urlClassLoader.loadClass("test.HelloWorldTccl");

    // Set the newly created URLClassLoader as the thread context classloader.
    Thread.currentThread().setContextClassLoader(urlClassLoader);

    try {
      // Uses the current class loader
      Class.forName("test.HelloWorldTccl");
      fail("Expected ClassNotFoundException not thrown");
    } catch (ClassNotFoundException expectedException) {
    }

    // Uses thread context class loader and should not fail
    ClassLoaderUtils.forName("test.HelloWorldTccl");
  }
}
