/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.net;

public class TopologyResolver {
  /** Path separator as a string {@value} */
  public final static String PATH_SEPARATOR_STR = "/";

  public static String getRack(Node host, boolean withNodeGroupLayer) {
    return withNodeGroupLayer ? getFirstHalf(host.getNetworkLocation()) 
        : host.getNetworkLocation();
  }

  public static String getNodeGroup(Node host, boolean withNodeGroupLayer) {
    return withNodeGroupLayer ? getLastHalf(host.getNetworkLocation()) : null;
  }

  public static String getRack(String networkLocation, boolean withNodeGroupLayer) {
    return withNodeGroupLayer ? getFirstHalf(networkLocation) 
        : networkLocation;
  }

  public static String getNodeGroup(String networkLocation, boolean withNodeGroupLayer) {
    return withNodeGroupLayer ? getLastHalf(networkLocation) : null;
  }

  private static String getFirstHalf(String wholeString) {
    int index = wholeString.lastIndexOf(PATH_SEPARATOR_STR);
    return wholeString.substring(0, index);
  }

  private static String getLastHalf(String wholeString) {
    int index = wholeString.lastIndexOf(PATH_SEPARATOR_STR);
    return wholeString.substring(index);
  }

}
