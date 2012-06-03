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
package org.apache.hadoop.yarn.util;

import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.net.TopologyResolver;
import org.junit.Assert;
import org.junit.Test;

public class TestTopologyResolver {
  private static boolean withNodeGroup;

  @Test
  public void testResolverWithoutNodeGroup() {
    String host1 = "h1";
    String rack1 = "/rack1";
    withNodeGroup = false;
    Node node1 = new NodeBase(host1, rack1);
    Assert.assertTrue("Get rack failed.", rack1.equals(TopologyResolver.getRack(node1, withNodeGroup)));
    Assert.assertNull("Nodegroup should be null for native. ", TopologyResolver.getNodeGroup(node1, withNodeGroup));
  }

  @Test
  public void testResolverWithNodeGroup() {
    String host1 = "h1";
    String rack1 = "/rack1";
    String nodegroup1 = "/nodegroup1";
    withNodeGroup = true;
    Node node1 = new NodeBase(host1, rack1 + nodegroup1);
    Assert.assertTrue("Get rack failed.", rack1.equals(TopologyResolver.getRack(node1, withNodeGroup)));
    Assert.assertTrue("Get nodegroup failed.", nodegroup1.equals(TopologyResolver.getNodeGroup(node1, withNodeGroup)));
  }

}
