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
package org.apache.rocketmq.common;

import java.util.HashMap;
import java.util.Map;

/**
 * ConsumeQueue offset route information for grayscale topics.
 */
public class CQOffsetRouteInfo {
    private Map<String, TopicCQOffsetRouteInfo> topicRouteInfoMap = new HashMap<>();

    public Map<String, TopicCQOffsetRouteInfo> getTopicRouteInfoMap() {
        return topicRouteInfoMap;
    }

    public void setTopicRouteInfoMap(Map<String, TopicCQOffsetRouteInfo> topicRouteInfoMap) {
        this.topicRouteInfoMap = topicRouteInfoMap;
    }

    public static class TopicCQOffsetRouteInfo {
        private String topic;
        private boolean inGrayscaleList;
        private long fileCQMinOffset;
        private long fileCQMaxOffset;
        private long kvCQMinOffset;
        private long kvCQMaxOffset;
        private long routeCutoffOffset; // File CQ maxOffset, the cutoff point for routing
        private Map<Integer, QueueCQOffsetRouteInfo> queueRouteInfoMap = new HashMap<>();

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public boolean isInGrayscaleList() {
            return inGrayscaleList;
        }

        public void setInGrayscaleList(boolean inGrayscaleList) {
            this.inGrayscaleList = inGrayscaleList;
        }

        public long getFileCQMinOffset() {
            return fileCQMinOffset;
        }

        public void setFileCQMinOffset(long fileCQMinOffset) {
            this.fileCQMinOffset = fileCQMinOffset;
        }

        public long getFileCQMaxOffset() {
            return fileCQMaxOffset;
        }

        public void setFileCQMaxOffset(long fileCQMaxOffset) {
            this.fileCQMaxOffset = fileCQMaxOffset;
        }

        public long getKvCQMinOffset() {
            return kvCQMinOffset;
        }

        public void setKvCQMinOffset(long kvCQMinOffset) {
            this.kvCQMinOffset = kvCQMinOffset;
        }

        public long getKvCQMaxOffset() {
            return kvCQMaxOffset;
        }

        public void setKvCQMaxOffset(long kvCQMaxOffset) {
            this.kvCQMaxOffset = kvCQMaxOffset;
        }

        public long getRouteCutoffOffset() {
            return routeCutoffOffset;
        }

        public void setRouteCutoffOffset(long routeCutoffOffset) {
            this.routeCutoffOffset = routeCutoffOffset;
        }

        public Map<Integer, QueueCQOffsetRouteInfo> getQueueRouteInfoMap() {
            return queueRouteInfoMap;
        }

        public void setQueueRouteInfoMap(Map<Integer, QueueCQOffsetRouteInfo> queueRouteInfoMap) {
            this.queueRouteInfoMap = queueRouteInfoMap;
        }
    }

    public static class QueueCQOffsetRouteInfo {
        private int queueId;
        private long fileCQMinOffset;
        private long fileCQMaxOffset;
        private long kvCQMinOffset;
        private long kvCQMaxOffset;
        private long routeCutoffOffset;

        public int getQueueId() {
            return queueId;
        }

        public void setQueueId(int queueId) {
            this.queueId = queueId;
        }

        public long getFileCQMinOffset() {
            return fileCQMinOffset;
        }

        public void setFileCQMinOffset(long fileCQMinOffset) {
            this.fileCQMinOffset = fileCQMinOffset;
        }

        public long getFileCQMaxOffset() {
            return fileCQMaxOffset;
        }

        public void setFileCQMaxOffset(long fileCQMaxOffset) {
            this.fileCQMaxOffset = fileCQMaxOffset;
        }

        public long getKvCQMinOffset() {
            return kvCQMinOffset;
        }

        public void setKvCQMinOffset(long kvCQMinOffset) {
            this.kvCQMinOffset = kvCQMinOffset;
        }

        public long getKvCQMaxOffset() {
            return kvCQMaxOffset;
        }

        public void setKvCQMaxOffset(long kvCQMaxOffset) {
            this.kvCQMaxOffset = kvCQMaxOffset;
        }

        public long getRouteCutoffOffset() {
            return routeCutoffOffset;
        }

        public void setRouteCutoffOffset(long routeCutoffOffset) {
            this.routeCutoffOffset = routeCutoffOffset;
        }
    }
}
