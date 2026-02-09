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
package org.apache.rocketmq.store.util;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

/**
 * Topic grayscale matcher that supports wildcard patterns.
 * Supports '*' (matches any sequence of characters) and '?' (matches any single character).
 */
public class TopicGrayscaleMatcher {
    private final List<String> patterns;

    public TopicGrayscaleMatcher(String grayscaleTopics) {
        this.patterns = parsePatterns(grayscaleTopics);
    }

    /**
     * Parse comma-separated topic patterns.
     *
     * @param grayscaleTopics comma-separated topic patterns, e.g., "test*,prod-*"
     * @return list of patterns
     */
    private List<String> parsePatterns(String grayscaleTopics) {
        List<String> patternList = new ArrayList<>();
        if (StringUtils.isBlank(grayscaleTopics)) {
            return patternList;
        }

        String[] parts = grayscaleTopics.split(",");
        for (String part : parts) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                patternList.add(trimmed);
            }
        }
        return patternList;
    }

    /**
     * Check if a topic matches any pattern in the grayscale list.
     *
     * @param topic topic name to check
     * @return true if topic matches any pattern, false otherwise
     */
    public boolean matches(String topic) {
        if (StringUtils.isBlank(topic) || patterns.isEmpty()) {
            return false;
        }

        for (String pattern : patterns) {
            if (matchesPattern(topic, pattern)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check if a topic matches a specific pattern using wildcard matching.
     * Supports '*' (matches any sequence of characters) and '?' (matches any single character).
     *
     * @param topic   topic name
     * @param pattern pattern with wildcards
     * @return true if topic matches pattern, false otherwise
     */
    private boolean matchesPattern(String topic, String pattern) {
        if (topic.equals(pattern)) {
            return true;
        }

        // Simple wildcard matching: * and ?
        return matchesWildcard(topic, pattern, 0, 0);
    }

    /**
     * Recursive wildcard matching algorithm.
     *
     * @param topic   topic string
     * @param pattern pattern string
     * @param tIndex  current index in topic
     * @param pIndex  current index in pattern
     * @return true if matches
     */
    private boolean matchesWildcard(String topic, String pattern, int tIndex, int pIndex) {
        // If we've reached the end of both strings, it's a match
        if (tIndex == topic.length() && pIndex == pattern.length()) {
            return true;
        }

        // If we've reached the end of pattern but not topic, no match
        if (pIndex == pattern.length()) {
            return false;
        }

        char pChar = pattern.charAt(pIndex);

        if (pChar == '*') {
            // '*' matches zero or more characters
            // Try matching zero characters first
            if (matchesWildcard(topic, pattern, tIndex, pIndex + 1)) {
                return true;
            }
            // Then try matching one or more characters
            for (int i = tIndex; i < topic.length(); i++) {
                if (matchesWildcard(topic, pattern, i + 1, pIndex + 1)) {
                    return true;
                }
            }
            return false;
        } else if (pChar == '?') {
            // '?' matches exactly one character
            if (tIndex >= topic.length()) {
                return false;
            }
            return matchesWildcard(topic, pattern, tIndex + 1, pIndex + 1);
        } else {
            // Regular character match
            if (tIndex >= topic.length()) {
                return false;
            }
            if (topic.charAt(tIndex) == pChar) {
                return matchesWildcard(topic, pattern, tIndex + 1, pIndex + 1);
            }
            return false;
        }
    }

    /**
     * Check if grayscale list is empty.
     *
     * @return true if empty, false otherwise
     */
    public boolean isEmpty() {
        return patterns.isEmpty();
    }
}
