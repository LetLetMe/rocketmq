/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.tools.command.queue;

import java.util.Map;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.CQOffsetRouteInfo;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class CheckCQOffsetRouteCommand implements SubCommand {

    @Override
    public String commandName() {
        return "checkCQOffsetRoute";
    }

    @Override
    public String commandDesc() {
        return "Check CQ offset route information for grayscale topics";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("n", "nameserverAddr", true, "NameServer address");
        opt.setRequired(true);
        options.addOption(opt);

        OptionGroup optionGroup = new OptionGroup();
        opt = new Option("b", "brokerAddr", true, "Broker address");
        optionGroup.addOption(opt);

        opt = new Option("c", "cluster", true, "Cluster name");
        optionGroup.addOption(opt);

        optionGroup.setRequired(false);
        options.addOptionGroup(optionGroup);

        opt = new Option("t", "topic", true, "Topic name (optional, show all topics if not specified)");
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        defaultMQAdminExt.setNamesrvAddr(StringUtils.trim(commandLine.getOptionValue('n')));

        String brokerAddr = commandLine.hasOption('b') ? commandLine.getOptionValue('b').trim() : null;
        String clusterName = commandLine.hasOption('c') ? commandLine.getOptionValue('c').trim() : null;
        String topic = commandLine.hasOption('t') ? commandLine.getOptionValue('t').trim() : null;

        try {
            defaultMQAdminExt.start();

            if (brokerAddr != null) {
                printCQOffsetRouteInfo(defaultMQAdminExt, brokerAddr, topic, false);
            } else if (clusterName != null) {
                ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();
                Map<String, BrokerData> brokerAddrTable = clusterInfo.getBrokerAddrTable();
                Set<String> brokerNames = clusterInfo.getClusterAddrTable().get(clusterName);
                if (brokerNames == null) {
                    System.out.print("Cluster " + clusterName + " not found");
                    return;
                }
                for (String brokerName : brokerNames) {
                    BrokerData brokerData = brokerAddrTable.get(brokerName);
                    if (brokerData != null && !brokerData.getBrokerAddrs().isEmpty()) {
                        String addr = brokerData.getBrokerAddrs().get(0L);
                        try {
                            printCQOffsetRouteInfo(defaultMQAdminExt, addr, topic, true);
                        } catch (Exception e) {
                            System.out.println("Failed to check broker " + brokerName + ": " + e.getMessage());
                        }
                    }
                }
            } else {
                System.out.print("Please specify either broker address (-b) or cluster name (-c)");
            }

        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

    private void printCQOffsetRouteInfo(DefaultMQAdminExt defaultMQAdminExt, String brokerAddr, String topic, boolean printBroker) throws Exception {
        CQOffsetRouteInfo routeInfo = defaultMQAdminExt.checkCQOffsetRoute(brokerAddr, topic);

        if (printBroker) {
            System.out.println("\n========== Broker: " + brokerAddr + " ==========");
        }

        Map<String, CQOffsetRouteInfo.TopicCQOffsetRouteInfo> topicRouteInfoMap = routeInfo.getTopicRouteInfoMap();
        if (topicRouteInfoMap.isEmpty()) {
            System.out.println("No topics found");
            return;
        }

        for (Map.Entry<String, CQOffsetRouteInfo.TopicCQOffsetRouteInfo> entry : topicRouteInfoMap.entrySet()) {
            CQOffsetRouteInfo.TopicCQOffsetRouteInfo topicInfo = entry.getValue();
            System.out.println("\nTopic: " + topicInfo.getTopic());
            System.out.println("  In Grayscale List: " + topicInfo.isInGrayscaleList());
            System.out.println("  File CQ Offset Range: [" + topicInfo.getFileCQMinOffset() + ", " + topicInfo.getFileCQMaxOffset() + ")");
            System.out.println("  KV CQ Offset Range: [" + topicInfo.getKvCQMinOffset() + ", " + topicInfo.getKvCQMaxOffset() + ")");
            System.out.println("  Route Cutoff Offset: " + topicInfo.getRouteCutoffOffset());

            // Print queue-level information if topic is specified
            if (topic != null && !topicInfo.getQueueRouteInfoMap().isEmpty()) {
                System.out.println("  Queue Details:");
                for (Map.Entry<Integer, CQOffsetRouteInfo.QueueCQOffsetRouteInfo> queueEntry : topicInfo.getQueueRouteInfoMap().entrySet()) {
                    CQOffsetRouteInfo.QueueCQOffsetRouteInfo queueInfo = queueEntry.getValue();
                    System.out.println("    Queue " + queueInfo.getQueueId() + ":");
                    System.out.println("      File CQ: [" + queueInfo.getFileCQMinOffset() + ", " + queueInfo.getFileCQMaxOffset() + ")");
                    System.out.println("      KV CQ: [" + queueInfo.getKvCQMinOffset() + ", " + queueInfo.getKvCQMaxOffset() + ")");
                    System.out.println("      Route Cutoff: " + queueInfo.getRouteCutoffOffset());
                }
            }
        }
    }
}
