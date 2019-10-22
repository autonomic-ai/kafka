/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.mirror;

import com.google.common.collect.Lists;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.stream.Collector;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.concurrent.ExecutionException;
import java.time.Duration;

/** Emits checkpoints for upstream consumer groups. */
public class MirrorCheckpointTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(MirrorCheckpointTask.class);

    private AdminClient sourceAdminClient;
    private KafkaAdminClient targetAdminClient;
    private String sourceClusterAlias;
    private String targetClusterAlias;
    private String checkpointsTopic;
    private Duration interval;
    private Duration pollTimeout;
    private Duration adminTimeout;
    private TopicFilter topicFilter;
    private Set<String> consumerGroups;
    private ReplicationPolicy replicationPolicy;
    private OffsetSyncStore offsetSyncStore;
    private boolean stopping;
    private MirrorMetrics metrics;
    private Scheduler scheduler;

    public MirrorCheckpointTask() {}

    // for testing
    MirrorCheckpointTask(String sourceClusterAlias, String targetClusterAlias,
            ReplicationPolicy replicationPolicy, OffsetSyncStore offsetSyncStore) {
        this.sourceClusterAlias = sourceClusterAlias;
        this.targetClusterAlias = targetClusterAlias;
        this.replicationPolicy = replicationPolicy;
        this.offsetSyncStore = offsetSyncStore;
    }

    @Override
    public void start(Map<String, String> props) {
        MirrorTaskConfig config = new MirrorTaskConfig(props);
        stopping = false;
        sourceClusterAlias = config.sourceClusterAlias();
        targetClusterAlias = config.targetClusterAlias();
        consumerGroups = config.taskConsumerGroups();
        checkpointsTopic = config.checkpointsTopic();
        topicFilter = config.topicFilter();
        replicationPolicy = config.replicationPolicy();
        interval = config.emitCheckpointsInterval();
        pollTimeout = config.consumerPollTimeout();
        adminTimeout = config.adminTimeout();
        offsetSyncStore = new OffsetSyncStore(config);
        sourceAdminClient = AdminClient.create(config.sourceAdminConfig());
        targetAdminClient = (KafkaAdminClient) Admin.create(config.targetAdminConfig());
        metrics = config.metrics();
        scheduler = new Scheduler(MirrorCheckpointTask.class, config.adminTimeout());
        scheduler.scheduleRepeatingDelayed(this::syncGroupOffset, config.syncGroupOffsetsInterval(),
                                          "sync consumer group offset from source to target");
    }

    @Override
    public void commit() throws InterruptedException {
        // nop
    }

    @Override
    public void stop() {
        long start = System.currentTimeMillis();
        stopping = true;
        Utils.closeQuietly(offsetSyncStore, "offset sync store");
        Utils.closeQuietly(sourceAdminClient, "source admin client");
        Utils.closeQuietly(metrics, "metrics");
        Utils.closeQuietly(scheduler, "scheduler");
        log.info("Stopping {} took {} ms.", Thread.currentThread().getName(), System.currentTimeMillis() - start);
    }

    @Override
    public String version() {
        return "1";
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        try { 
            long deadline = System.currentTimeMillis() + interval.toMillis();
            while (!stopping && System.currentTimeMillis() < deadline) {
                offsetSyncStore.update(pollTimeout);
            }
            List<SourceRecord> records = new ArrayList<>();
            for (String group : consumerGroups) {
                records.addAll(sourceRecordsForGroup(group));
            }
            if (records.isEmpty()) {
                // WorkerSourceTask expects non-zero batches or null
                return null;
            } else {
                return records;
            }
        } catch (Throwable e) {
            log.warn("Failure polling consumer state for checkpoints.", e);
            return null;
        }
    }


    private List<SourceRecord> sourceRecordsForGroup(String group) throws InterruptedException {
        try {
            long timestamp = System.currentTimeMillis();
            List<Checkpoint> checkpoints = checkpointsForGroup(group);
            return checkpoints.stream()
                .map(x -> checkpointRecord(x, timestamp))
                .collect(Collectors.toList());
        } catch (ExecutionException e) {
            log.error("Error querying offsets for consumer group {} on cluster {}.",  group, sourceClusterAlias, e);
            return Collections.emptyList();
        }
    }

    private List<Checkpoint> checkpointsForGroup(String group) throws ExecutionException, InterruptedException {
        return listConsumerGroupOffsets(group).entrySet().stream()
            .filter(x -> shouldCheckpointTopic(x.getKey().topic()))
            .map(x -> checkpoint(group, x.getKey(), x.getValue()))
            .filter(x -> x.downstreamOffset() > 0)  // ignore offsets we cannot translate accurately
            .collect(Collectors.toList());
    }

    private Map<TopicPartition, OffsetAndMetadata> listConsumerGroupOffsets(String group)
            throws InterruptedException, ExecutionException {
        if (stopping) {
            // short circuit if stopping
            return Collections.emptyMap();
        }
        return sourceAdminClient.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();
    }

    Checkpoint checkpoint(String group, TopicPartition topicPartition,
            OffsetAndMetadata offsetAndMetadata) {
        long upstreamOffset = offsetAndMetadata.offset();
        long downstreamOffset = offsetSyncStore.translateDownstream(topicPartition, upstreamOffset);
        return new Checkpoint(group, renameTopicPartition(topicPartition),
            upstreamOffset, downstreamOffset, offsetAndMetadata.metadata());
    }

    SourceRecord checkpointRecord(Checkpoint checkpoint, long timestamp) {
        return new SourceRecord(
            checkpoint.connectPartition(), MirrorUtils.wrapOffset(0),
            checkpointsTopic, 0,
            Schema.BYTES_SCHEMA, checkpoint.recordKey(),
            Schema.BYTES_SCHEMA, checkpoint.recordValue(),
            timestamp);
    }

    TopicPartition renameTopicPartition(TopicPartition upstreamTopicPartition) {
        if (targetClusterAlias.equals(replicationPolicy.topicSource(upstreamTopicPartition.topic()))) {
            // this topic came from the target cluster, so we rename like us-west.topic1 -> topic1
            return new TopicPartition(replicationPolicy.originalTopic(upstreamTopicPartition.topic()),
                upstreamTopicPartition.partition());
        } else {
            // rename like topic1 -> us-west.topic1
            return new TopicPartition(replicationPolicy.formatRemoteTopic(sourceClusterAlias,
                upstreamTopicPartition.topic()), upstreamTopicPartition.partition());
        }
    }

    boolean shouldCheckpointTopic(String topic) {
        return topicFilter.shouldReplicateTopic(topic);
    }

    @Override
    public void commitRecord(SourceRecord record) {
        metrics.checkpointLatency(MirrorUtils.unwrapPartition(record.sourcePartition()),
            Checkpoint.unwrapGroup(record.sourcePartition()),
            System.currentTimeMillis() - record.timestamp());
    }

    private void syncGroupOffset() throws InterruptedException {
        Map<String, KafkaFuture<ConsumerGroupDescription>> describedGroups = targetAdminClient
                                                                                 .describeConsumerGroups(consumerGroups)
                                                                                 .describedGroups();
        for (String group : consumerGroups) {
            try {
                ConsumerGroupDescription consumerGroupDescription = describedGroups.get(group).get();
                // consider to sync offset to the target cluster, only if the consumer group is not active
                if (consumerGroupDescription.state().equals(ConsumerGroupState.EMPTY)) {
                    List<Checkpoint> checkpoints = checkpointsForGroup(group);
                    alterGroupOffset(group, checkpoints);
                }
            } catch (ExecutionException e) {
                log.error("Error querying for consumer group {} on cluster {}.", group, sourceClusterAlias, e);
            }
        }
    }


    // Given a consumer group, alter the group consumer offsets in the target cluster,
    private void alterGroupOffset(String group, List<Checkpoint> checkpoints) throws InterruptedException, ExecutionException
    {
        Map<TopicPartition, OffsetAndMetadata> translatedOffsets = new HashMap<>();
        for (Checkpoint checkpoint : checkpoints) {
            translatedOffsets.put(checkpoint.topicPartition(), checkpoint.offsetAndMetadata());
        }

        Map<TopicPartition, OffsetAndMetadata> committedOffsets = new HashMap<>();

        for (Entry<TopicPartition, OffsetAndMetadata> entry :
            targetAdminClient.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
                             .get().entrySet()) {
            long offsetFromTarget = entry.getValue().offset();
            if (!translatedOffsets.containsKey(entry.getKey()))
                continue;
            long translatedOffset = translatedOffsets.get(entry.getKey()).offset();

            // if translated offset from upstream is smaller than the current consumer offset
            // in the target, skip updating the offset for that partition
            if (offsetFromTarget >= translatedOffset)
                continue;

            committedOffsets.put(entry.getKey(), translatedOffsets.get(entry.getKey()));
        }

        if (committedOffsets.size() > 0) {
            targetAdminClient.alterConsumerGroupOffsets(group, committedOffsets);
            log.trace("syncing the offset for consumer group: {} with {} offset entries", group, committedOffsets.size());
        } else {
            log.trace("skip syncing the offset for consumer group: {}", group);
        }
    }
}
