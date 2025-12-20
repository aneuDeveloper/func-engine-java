/**
* Copyright 2022 aneuDeveloper
* 
* Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the * "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
* 
* The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
* 
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
* 
*/
package io.github.aneudeveloper.func.engine;

import java.io.Closeable;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.aneudeveloper.func.engine.function.Func;
import io.github.aneudeveloper.func.engine.function.FuncContextSerDes;
import io.github.aneudeveloper.func.engine.function.FuncEvent;
import io.github.aneudeveloper.func.engine.function.FuncEvent.Type;
import io.github.aneudeveloper.func.engine.function.FuncEventBuilder;
import io.github.aneudeveloper.func.engine.function.FuncMapper;

public class FuncEngine<T> implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(FuncEngine.class);

    private KafkaProducer<String, byte[]> kafkaProducer;
    private String processName;
    private FuncStream<T> funcStream;
    private FuncContextSerDes<T> funcContextSerDes;
    private FuncMapper<T> funcMapper;
    private TopicResolver topicResolver;
    private FuncExecuter<T> processEventExecuter;
    private SendEventExceptionHandler sendEventExceptionHandler;
    private Properties funcStreamProperties;
    private Properties producerProperties;
    private String bootstrapServers;
    private int newTopicNumPartitions = 1;
    private short newTopicReplicationRactor = 1;
    private FuncEventMapper<T> funcEventMapper = new FuncEventMapper<T>();;

    public FuncEngine(String processName, String bootstrapServers) {
        if (processName == null) {
            throw new IllegalStateException("Please provide processName");
        }
        if (bootstrapServers == null) {
            throw new IllegalStateException("Please provide bootstrapServers");
        }
        this.bootstrapServers = bootstrapServers;
        this.processName = processName;
        this.processEventExecuter = new FuncExecuter<T>(this);
    }

    public void setUncaughtExceptionHandler(StreamsUncaughtExceptionHandler uncaughtExceptionHandler) {
        if (funcStream != null) {
            this.funcStream.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        }
    }

    public void start() {
        createProducer();
        startFuncStream();
    }

    public void createProducer() {
        this.kafkaProducer = new KafkaProducer<>(getProducerProperties());
    }

    public void startFuncStream() {
        if (funcStream != null) {
            return;
        }
        LOG.info("Checking if Kafka is ready. For bootstrapserver={}", bootstrapServers);
        producerProperties = getProducerProperties();
        for (int i = 0; i < 150; ++i) {
            if (this.isKafkaReady(producerProperties)) {
                LOG.info("Kafka is ready. Continue ...");
                break;
            }
            LOG.info("Kafka is not ready yet. Waiting 4100 ms. Bootstrapserver={}", bootstrapServers);
            try {
                Thread.sleep(4100L);
                continue;
            } catch (InterruptedException e) {
                LOG.error(e.getMessage(), e);
            }
        }
        LOG.debug("Start workflow={}", this.getProcessName());

        String workflowTopic = getTopicResolver().resolveTopicName(FuncEvent.Type.WORKFLOW.name());
        LOG.info("Observe workflow topic: {}", workflowTopic);

        funcStream = new FuncStream<T>(this, this.processEventExecuter);
        funcStream.start(List.of(workflowTopic));
    }

    public Properties getProducerProperties() {
        if (this.producerProperties == null) {
            this.producerProperties = new Properties();
            producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    StringSerializer.class.getName());
            producerProperties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
            producerProperties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
            producerProperties.setProperty(ProducerConfig.RETRIES_CONFIG, "10");
            producerProperties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
            producerProperties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
            producerProperties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "60000");
            producerProperties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
            producerProperties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32768));
            producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        }
        return producerProperties;
    }

    public void createMissingTopics() {
        Set<String> missingTopics = this.getMissingTopics();
        if (missingTopics != null && !missingTopics.isEmpty()) {
            LOG.info("Following topics will be created: {}", missingTopics);
            this.createTopics(missingTopics);
        } else {
            LOG.info("There are no missing topics");
        }
    }

    private boolean isKafkaReady(Properties aProperties) {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                aProperties.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
        properties.put("connections.max.idle.ms", 4000);
        properties.put("request.timeout.ms", 4000);
        AdminClient client = null;
        try {
            client = AdminClient.create(properties);
            ListTopicsResult topics = client.listTopics();
            topics.names().get();
            return true;
        } catch (Throwable throwable) {
            return false;
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    public void close() {
        LOG.info("Caught shutdown hook!");
        if (this.funcStream != null) {
            this.funcStream.close();
        }
        if (this.kafkaProducer != null) {
            this.kafkaProducer.close();
        }
        LOG.info("Application exited.");
    }

    private Set<String> getMissingTopics() {
        Set<String> topicsToCreate = new HashSet<>();
        ArrayList<String> requiredTopics = new ArrayList<>();
        requiredTopics.add(getTopicResolver().resolveTopicName(FuncEvent.Type.WORKFLOW.name()));
        requiredTopics.add(getTopicResolver().resolveTopicName(FuncEvent.Type.TRANSIENT.name()));
        requiredTopics.add(getTopicResolver().resolveTopicName(FuncEvent.Type.DELAY.name()));
        Properties properties = this.getAdminClientProperties();
        AdminClient adminClient = AdminClient.create(properties);
        if (adminClient == null) {
            return topicsToCreate;
        }
        try {
            Set<String> existingTopics = adminClient.listTopics().names().get();
            topicsToCreate = requiredTopics.stream() //
                    .filter(requiredTopic -> !existingTopics.contains(requiredTopic)) //
                    .collect(Collectors.toSet());
        } catch (Throwable throwable) {
            try {
                throw throwable;
            } catch (InterruptedException | ExecutionException e) {
                LOG.error(e.getMessage(), e);
                return new HashSet<String>();
            }
        } finally {
            if (adminClient != null) {
                adminClient.close();
            }
        }
        return topicsToCreate;
    }

    private void createTopics(Collection<String> aTopicsToCreate) {
        if (aTopicsToCreate == null || aTopicsToCreate.isEmpty()) {
            LOG.info("No topics should be created.");
            return;
        }
        LOG.info("Try to create following topics: {}", aTopicsToCreate.toString());
        List<NewTopic> topicsToCreate = aTopicsToCreate.parallelStream().map(newTopicName -> {
            LOG.info("Creating topic {} with partitions={} and replicationFactor={}",
                    new Object[] { newTopicName, newTopicNumPartitions, newTopicReplicationRactor });
            NewTopic newTopic = new NewTopic(newTopicName, this.newTopicNumPartitions, newTopicReplicationRactor);
            return newTopic;
        }).collect(Collectors.toList());
        Properties properties = this.getAdminClientProperties();
        try (AdminClient adminClient = AdminClient.create(properties);) {
            if (!topicsToCreate.isEmpty()) {
                CreateTopicsResult createTopics = adminClient.createTopics(topicsToCreate);
                createTopics.values().values().stream().forEach(result -> {
                    try {
                        result.get();
                    } catch (InterruptedException | ExecutionException e) {
                        LOG.error(e.getMessage(), e);
                    }
                });
            }
        }
    }

    public void sendEvent(String destinationTopic, String key, FuncEvent<T> functionEvent) {
        if (this.kafkaProducer == null) {
            return;
        }
        try {
            byte[] serialize = getFuncContextSerDes().serialize(functionEvent.getContext());

            Headers headers = getFuncEventMapper().toHeader(new RecordHeaders(), functionEvent);
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(destinationTopic, null, null, key, serialize,
                    headers);
            this.kafkaProducer.send(record);
            this.kafkaProducer.flush();
        } catch (Exception e) {
            if (sendEventExceptionHandler != null) {
                sendEventExceptionHandler.onException(e, destinationTopic, key, functionEvent);
            } else {
                throw e;
            }
        }
    }

    public void sendEventAndWait(String destinationTopic, String key, FuncEvent<T> functionEvent)
            throws InterruptedException, ExecutionException {
        if (this.kafkaProducer == null) {
            return;
        }
        try {
            byte[] serialize = getFuncContextSerDes().serialize(functionEvent.getContext());

            Headers headers = getFuncEventMapper().toHeader(new RecordHeaders(), functionEvent);
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(destinationTopic, null, null, key, serialize,
                    headers);
            this.kafkaProducer.send(record).get();
            this.kafkaProducer.flush();
        } catch (Exception e) {
            if (sendEventExceptionHandler != null) {
                sendEventExceptionHandler.onException(e, destinationTopic, key, functionEvent);
            } else {
                throw e;
            }
        }
    }

    public void sendContent(String destinationTopic, String key, Headers headers, byte[] functionEvent) {
        if (this.kafkaProducer == null) {
            return;
        }
        try {
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(destinationTopic, null, null, key,
                    functionEvent,
                    headers);
            this.kafkaProducer.send(record);
            this.kafkaProducer.flush();
        } catch (Exception e) {
            if (sendEventExceptionHandler != null) {
                sendEventExceptionHandler.onException(e, destinationTopic, key, functionEvent);
            } else {
                throw e;
            }
        }
    }

    public void sendContentWait(String destinationTopic, String key, Headers headers, byte[] functionEvent)
            throws Exception {
        if (this.kafkaProducer == null) {
            return;
        }
        try {
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(destinationTopic, null, null, key,
                    functionEvent,
                    headers);
            this.kafkaProducer.send(record).get();
            this.kafkaProducer.flush();
        } catch (Exception e) {
            if (sendEventExceptionHandler != null) {
                sendEventExceptionHandler.onException(e, destinationTopic, key, functionEvent);
            } else {
                throw e;
            }
        }
    }

    public void sendEvent(FuncEvent<T> functionEvent) {
        if (this.kafkaProducer == null) {
            return;
        }
        try {
            String destinationTopic = getTopicResolver().resolveTopicName(functionEvent.getType().name());
            this.sendEvent(destinationTopic, null, functionEvent);
        } catch (Exception e) {
            if (sendEventExceptionHandler != null) {
                sendEventExceptionHandler.onException(e, functionEvent);
            } else {
                throw e;
            }
        }
    }

    public String getProcessName() {
        return this.processName;
    }

    public String getProcessNameWithCorrelationId(String correlation) {
        return this.getProcessName() + "_" + correlation;
    }

    public Serde<T> getSerde() {
        final FuncContextSerDes<T> contextSerDes = getFuncContextSerDes();
        Serde<T> processEventSerde = Serdes.serdeFrom(new Serializer<T>() {

            @Override
            public byte[] serialize(String topic, T data) {
                if (data == null) {
                    return "".getBytes();
                }
                return contextSerDes.serialize(data);
            }

        },
                new Deserializer<T>() {

                    @Override
                    public T deserialize(String topic, byte[] data) {
                        return contextSerDes.deserialize(data);
                    }

                });
        return processEventSerde;
    }

    public FuncEvent<T> execute(FuncEvent<T> newFunctionEvent) throws Exception {
        if (newFunctionEvent.getFunction() == null) {
            throw new IllegalStateException(
                    "Missing function. Please provide which function should be called at start.");
        }
        if (newFunctionEvent.getType() == null) {
            throw new IllegalStateException("Function type must be specified.");
        }
        if (newFunctionEvent.getId() == null) {
            throw new IllegalStateException("id must be specified");
        }
        if (newFunctionEvent.getProcessInstanceID() == null) {
            newFunctionEvent.setProcessInstanceID(UUID.randomUUID().toString());
        }
        newFunctionEvent.setProcessName(this.getProcessName());
        if (newFunctionEvent.getType() == FuncEvent.Type.TRANSIENT) {
            try {
                Func<T> functionToExecute = funcMapper.map(newFunctionEvent);
                FuncEvent<T> executionResult = this.processEventExecuter.executeTransientFunction(newFunctionEvent,
                        functionToExecute);

                if (executionResult.getType() == Type.TRANSIENT || executionResult.getType() == Type.END) {
                    String destTopic = this.topicResolver.resolveTopicName(FuncEvent.Type.TRANSIENT.name());
                    this.sendEvent(destTopic, null, executionResult);

                    return executionResult;
                } else {
                    newFunctionEvent = executionResult;
                }
            } catch (Exception e) {
                FuncEvent<T> nextFunction = FuncEventBuilder.newEvent();
                nextFunction.setProcessName(newFunctionEvent.getProcessName());
                nextFunction.setComingFromId(newFunctionEvent.getId());
                nextFunction.setProcessInstanceID(newFunctionEvent.getProcessInstanceID());
                nextFunction.setType(FuncEvent.Type.ERROR);

                Headers header = this.getFuncEventMapper().toHeader(null, nextFunction);

                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                e.printStackTrace(pw);

                String destTopic = this.topicResolver.resolveTopicName(FuncEvent.Type.TRANSIENT.name());
                this.sendContent(destTopic, null, header, sw.toString().getBytes());
                throw e;
            }
        }
        String destinationTopic = getTopicResolver().resolveTopicName(newFunctionEvent.getType().name());
        this.sendEventAndWait(destinationTopic, null, newFunctionEvent);
        return newFunctionEvent;
    }

    public FuncContextSerDes<T> getFuncContextSerDes() {
        return this.funcContextSerDes;
    }

    public void setFuncContextSerDes(FuncContextSerDes<T> funcContextSerDes) {
        this.funcContextSerDes = funcContextSerDes;
    }

    public FuncMapper<T> getFuncMapper() {
        return funcMapper;
    }

    public void setFuncMapper(FuncMapper<T> funcSerDes) {
        this.funcMapper = funcSerDes;
    }

    private Properties getAdminClientProperties() {
        Properties properties = new Properties();
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return properties;
    }

    public TopicResolver getTopicResolver() {
        if (this.topicResolver == null) {
            this.topicResolver = new DefaultTopicResolver(this.processName + "-");
        }
        return this.topicResolver;
    }

    public void setTopicResolver(TopicResolver topicResolver) {
        this.topicResolver = topicResolver;
    }

    public SendEventExceptionHandler getSendEventExceptionHandler() {
        return sendEventExceptionHandler;
    }

    public void setSendEventExceptionHandler(SendEventExceptionHandler sendEventExceptionHandler) {
        this.sendEventExceptionHandler = sendEventExceptionHandler;
    }

    public Properties getFuncStreamProperties() {
        if (funcStreamProperties == null) {
            Properties properties = new Properties();
            properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.put("auto.offset.reset", "earliest");
            properties.put("default.key.serde", Serdes.String().getClass());
            properties.put("num.stream.threads", "1");
            properties.put("default.deserialization.exception.handler",
                    "org.apache.kafka.streams.errors.LogAndContinueExceptionHandler");

            properties.put("processing.guarantee", "exactly_once_v2");
            properties.put("transaction.timeout.ms", "900000");
            properties.put("max.poll.records", "5");
            properties.put(StreamsConfig.APPLICATION_ID_CONFIG, processName + "-FuncStream");
            funcStreamProperties = properties;
        }
        return this.funcStreamProperties;
    }

    public void setFuncStreamProperties(Properties funcStreamProperties) {
        this.funcStreamProperties = funcStreamProperties;
    }

    public int getNewTopicNumPartitions() {
        return newTopicNumPartitions;
    }

    public void setNewTopicNumPartitions(int newTopicNumPartitions) {
        this.newTopicNumPartitions = newTopicNumPartitions;
    }

    public int getNewTopicReplicationRactor() {
        return newTopicReplicationRactor;
    }

    public void setNewTopicReplicationRactor(short newTopicReplicationRactor) {
        this.newTopicReplicationRactor = newTopicReplicationRactor;
    }

    public FuncEventMapper<T> getFuncEventMapper() {
        return this.funcEventMapper;
    }

    public void setFuncEventMapper(FuncEventMapper<T> funcEventMapper) {
        this.funcEventMapper = funcEventMapper;
    }
}
