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
package func.engine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import func.engine.correlation.CorrelationMerger;
import func.engine.correlation.CorrelationState;
import func.engine.correlation.CorrelationStream;
import func.engine.correlation.DefaultCorrelationMerger;
import func.engine.correlation.ProcessCorrelation;
import func.engine.function.Func;
import func.engine.function.FuncContextSerDes;
import func.engine.function.FuncEvent;
import func.engine.function.FuncEvent.Type;
import func.engine.function.FuncEventDeserializer;
import func.engine.function.FuncEventSerializer;
import func.engine.function.FuncSerDes;

public class FuncWorkflow<T> {
    private static final Logger LOG = LoggerFactory.getLogger(FuncWorkflow.class);
    public static final String PROCESS_NAME = "process.name";
    public static final String KAFKA_BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String SYNCHRONOUS_WAITHANDLER_STEAM_APPLICATION_NAME = "synchronous.waithandler.stream.application.name";
    public static final String CORRELATION_STREAM_APPLICATION_NAME = "correlation.stream.application.name";
    public static final String WORKFLOW_STREAM_PREFIX = "workflow.stream.prefix";
    public static final String TOPIC_DEFAULT_NUM_PARTITIONS = "topic.default.num.partitions";
    public static final String TOPIC_DEFAULT_REPLICATION_FACTOR = "topic.default.replication.factor";
    public static final String TOPIC_CORRELATION_RETENTION_MS = "topic.correlation.retention.ms";
    public static final String TOPIC_CORRELATION_DETELE_RETENTION_MS = "topic.correlation.delete.retention.ms";
    public static final String STEPS_NUM_STREAM_THREADS_CONFIG = "steps.num.stream.threads.config";
    public static final String CORRELATION_NUM_STREAM_THREADS_CONFIG = "steps.num.stream.threads.config";
    public static final String DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG = "default.deserialization.exception.handler";
    public static final String TOPIC_RESOLVER_OBJ = "topic.resolver.obj";

    private KafkaProducer<String, FuncEvent> kafkaProducer;
    private String processName;
    private CorrelationStream<T> correlationStream;
    private FuncStream<T> funcStream;
    private Properties properties;
    private FuncContextSerDes<T> funcContextSerDes;
    private FuncSerDes funcSerDes;
    private CorrelationMerger<T> correlationMerger;
    private TopicResolver topicResolver;
    private FuncExecuter<T> processEventExecuter;
    private boolean startCorrelation = true;

    public FuncWorkflow(Properties properties) {
        if (properties == null) {
            throw new IllegalStateException("Please provide properties");
        }
        this.properties = properties;
        this.processName = this.getProcessName(properties);
        this.topicResolver = this.createTopicResolver(properties);
        this.processEventExecuter = new FuncExecuter<T>(this);
    }

    private TopicResolver createTopicResolver(Properties properties) {
        Object topicResolver = properties.get(TOPIC_RESOLVER_OBJ);
        if (topicResolver != null && TopicResolver.class.isAssignableFrom(topicResolver.getClass())) {
            return (TopicResolver) topicResolver;
        }
        return new DefaultTopicResolver(this.processName + "-");
    }

    public TopicResolver getTopicResolver() {
        return this.topicResolver;
    }

    private String getProcessName(Properties properties) {
        this.processName = properties.getProperty(PROCESS_NAME);
        if (this.processName == null || this.processName.trim().isBlank()) {
            throw new IllegalStateException("Please define following property process.name in properties");
        }
        return this.processName;
    }

    public void start() {
        if (funcStream != null) {
            return;
        }
        String bootstrapServer = this.getProducerProperties().getProperty(KAFKA_BOOTSTRAP_SERVERS);
        LOG.info("Checking if Kafka is ready. For bootstrapserver={}", bootstrapServer);
        for (int i = 0; i < 150; ++i) {
            if (this.isKafkaReady(this.getProducerProperties())) {
                LOG.info("Kafka is ready. Continue ...");
                break;
            }
            LOG.info("Kafka is not ready yet. Waiting 4100 ms. Bootstrapserver={}", bootstrapServer);
            try {
                Thread.sleep(4100L);
                continue;
            } catch (InterruptedException e) {
                LOG.error(e.getMessage(), e);
            }
        }
        LOG.debug("Start workflow={}", this.getProcessName());
        this.kafkaProducer = new KafkaProducer<String, FuncEvent>(this.getProducerProperties());
        HashSet<String> allProcessTopics = new HashSet<>();
        allProcessTopics.add(this.topicResolver.resolveTopicName(FuncEvent.Type.WORKFLOW));
        LOG.info("Observed workflow topics: {}", allProcessTopics);
        Set<String> missingTopics = this.getMissingTopics(allProcessTopics);
        LOG.info("Missing topics: {}", missingTopics);
        this.createTopics(missingTopics);

        funcStream = new FuncStream<T>(this, this.processEventExecuter);
        funcStream.start(allProcessTopics);

        if (startCorrelation) {
            if (this.correlationMerger == null) {
                this.correlationMerger = new DefaultCorrelationMerger<T>();
            }
            if (this.correlationStream == null) {
                this.correlationStream = new CorrelationStream<T>(this);
            }
            this.correlationStream.startStreaming();
        }
    }

    private boolean isKafkaReady(Properties aProperties) {
        Properties properties = new Properties();
        properties.put(KAFKA_BOOTSTRAP_SERVERS, aProperties.get(KAFKA_BOOTSTRAP_SERVERS));
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
        this.funcStream.close();
        this.kafkaProducer.close();
        if (this.correlationStream != null) {
            this.correlationStream.close();
        }
        LOG.info("Application exited.");
    }

    private Set<String> getMissingTopics(Collection<String> aAllTopics) {
        Set<String> topicsToCreate = new HashSet<>();
        ArrayList<String> requiredTopics = new ArrayList<>(aAllTopics);
        requiredTopics.add(this.topicResolver.resolveTopicName(FuncEvent.Type.CORRELATION));
        requiredTopics.add(this.topicResolver.resolveTopicName(FuncEvent.Type.CALLBACK));
        requiredTopics.add(this.topicResolver.resolveTopicName(FuncEvent.Type.TRANSIENT));
        requiredTopics.add(this.topicResolver.resolveTopicName(FuncEvent.Type.RETRY));
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
            int numPartitions = Integer.valueOf(
                    this.getPropertyOverridedForTopic(newTopicName, TOPIC_DEFAULT_NUM_PARTITIONS, "1"));
            short replicationFactor = Short.valueOf(
                    this.getPropertyOverridedForTopic(newTopicName, TOPIC_DEFAULT_REPLICATION_FACTOR, "1"));
            LOG.info("Creating topic {} with partitions={} and replicationFactor={}",
                    new Object[] { newTopicName, numPartitions, replicationFactor });
            NewTopic newTopic = new NewTopic(newTopicName, numPartitions, replicationFactor);
            if (this.topicResolver.resolveTopicName(FuncEvent.Type.CORRELATION).equals(newTopicName)) {
                Map<String, String> configs = new HashMap<>();
                configs.put("cleanup.policy", "compact");
                long defaultDuration = 3456000000L;
                long retentionMs = Long.valueOf(this.getProperty(TOPIC_CORRELATION_RETENTION_MS, "" + defaultDuration));
                configs.put("retention.ms", "" + retentionMs);
                long deleteRetentionMs = Long
                        .valueOf(this.getProperty(TOPIC_CORRELATION_DETELE_RETENTION_MS, "" + defaultDuration));
                configs.put("delete.retention.ms", "" + deleteRetentionMs);
                newTopic.configs(configs);
            }
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

    private String getPropertyOverridedForTopic(String topicName, String key, String defaultValue) {
        Object topicSpecificProperty = this.properties.get(topicName + "_" + key);
        if (topicSpecificProperty != null) {
            return String.valueOf(topicSpecificProperty);
        }
        return this.getProperty(key, defaultValue);
    }

    public void sendEvent(String destinationTopic, String key, FuncEvent functionEvent) {
        ProducerRecord<String, FuncEvent> record = new ProducerRecord<>(destinationTopic, key, functionEvent);
        this.kafkaProducer.send(record);
        this.kafkaProducer.flush();
    }

    public void sendEventSync(String destinationTopic, String key, FuncEvent functionEvent)
            throws InterruptedException, ExecutionException {
        this.sendEventSync(destinationTopic, key, functionEvent);
    }

    public void sendEventAndWait(String destinationTopic, String key, FuncEvent functionEvent)
            throws InterruptedException, ExecutionException {
        ProducerRecord<String, FuncEvent> record = new ProducerRecord<>(destinationTopic, key, functionEvent);
        this.kafkaProducer.send(record).get();
        this.kafkaProducer.flush();
    }

    public void sendEvent(FuncEvent<T> functionEvent) {
        String destinationTopic = this.topicResolver.resolveTopicName(functionEvent.getType());
        this.sendEvent(destinationTopic, null, functionEvent);
    }

    public String getProcessName() {
        return this.processName;
    }

    public String getProcessNameWithCorrelationId(String correlation) {
        return this.getProcessName() + "_" + correlation;
    }

    public Serde<FuncEvent<T>> getSerde() {
        Serde<FuncEvent<T>> processEventSerde = Serdes.serdeFrom(new FuncEventSerializer(),
                new FuncEventDeserializer());
        return processEventSerde;
    }

    public FuncEvent<T> startProcess(WorkflowStart<T> processInstance)
            throws InterruptedException, ExecutionException {
        if (processInstance.getFunction() == null) {
            throw new IllegalStateException(
                    "Missing function. Please provide which function should be called at start.");
        }
        UUID processInstanceID = UUID.randomUUID();
        FuncEvent<T> newFunctionEvent = FuncEvent.createWithDefaultValues();
        newFunctionEvent.setType(FuncEvent.Type.WORKFLOW);
        newFunctionEvent.setFunction(this.funcSerDes.serialize(processInstance.getFunction()));
        newFunctionEvent.setProcessInstanceID(processInstanceID.toString());
        newFunctionEvent.setContext(processInstance.getContext());
        newFunctionEvent.setProcessName(this.getProcessName());
        if (processInstance.isTransientFunction()) {
            if (!(processInstance.getFunction() instanceof Func)) {
                throw new IllegalStateException("A transient function should be of type StatefulFunction");
            }
            newFunctionEvent.setType(FuncEvent.Type.TRANSIENT);
            FuncEvent<T> executionResult = this.processEventExecuter.executeMessage(newFunctionEvent);
            if (executionResult.getType() == FuncEvent.Type.ERROR) {
                if (executionResult.getContext() instanceof Throwable) {
                    throw new RuntimeException((Throwable) executionResult.getContext());
                }
                throw new RuntimeException(this.funcContextSerDes.serialize(executionResult.getContext()));
            }
            if (executionResult.getType() == Type.TRANSIENT || executionResult.getType() == Type.END) {
                return executionResult;
            } else {
                newFunctionEvent = executionResult;
            }
        }
        String destinationTopic = this.topicResolver.resolveTopicName(newFunctionEvent.getType());
        this.sendEventAndWait(destinationTopic, null, newFunctionEvent);
        return newFunctionEvent;
    }

    public FuncEvent<T> correlate(ProcessCorrelation<T> correlation)
            throws InterruptedException, ExecutionException {
        FuncEvent<T> callbackMessage = FuncEvent.createWithDefaultValues();
        callbackMessage.setCorrelationState(CorrelationState.CALLBACK_RECEIVED);
        callbackMessage.setCorrelationId(correlation.getCorrelationId());
        callbackMessage.setContext(correlation.getData());
        String topicKeyWithProcessNameAndCorrelationId = this
                .getProcessNameWithCorrelationId(correlation.getCorrelationId());
        String callbackTopic = this.topicResolver.resolveTopicName(FuncEvent.Type.CALLBACK);
        this.sendEventAndWait(callbackTopic, topicKeyWithProcessNameAndCorrelationId, callbackMessage);
        return callbackMessage;
    }

    public Properties getProperties() {
        return this.properties;
    }

    public String getProperty(String key) {
        return this.properties.getProperty(key);
    }

    public String getProperty(String key, String defaultValue) {
        String property = String.valueOf(this.properties.getOrDefault(key, defaultValue));
        return property;
    }

    public FuncContextSerDes<T> getFuncContextSerDes() {
        return this.funcContextSerDes;
    }

    public CorrelationMerger<T> getCorrelationMerger() {
        return this.correlationMerger;
    }

    public void setCorrelationMerger(CorrelationMerger<T> correlationMerger) {
        this.correlationMerger = correlationMerger;
    }

    public String getFunction(FuncEvent event) {
        if (event == null) {
            return null;
        }
        if (event.getFunction() != null) {
            return event.getFunction();
        }
        if (event.getFunctionObj() != null) {
            String serializedFunction = this.funcSerDes.serialize(event.getFunctionObj());
            event.setFunction(serializedFunction);
        } else {
            LOG.error("Step has not been defined.");
        }
        return event.getFunction();
    }

    public void setFuncContextSerDes(FuncContextSerDes<T> funcContextSerDes) {
        this.funcContextSerDes = funcContextSerDes;
    }

    public FuncSerDes getFuncSerDes() {
        return funcSerDes;
    }

    public void setFuncSerDes(FuncSerDes funcSerDes) {
        this.funcSerDes = funcSerDes;
    }

    private Properties getProducerProperties() {
        Properties producerProperties = new Properties();
        producerProperties.setProperty("bootstrap.servers", getProperty("bootstrap.servers"));
        producerProperties.setProperty("key.serializer", StringSerializer.class.getName());
        producerProperties.setProperty("enable.idempotence", "true");
        producerProperties.setProperty("acks", "all");
        producerProperties.setProperty("retries", "10");
        producerProperties.setProperty("max.in.flight.requests.per.connection", "5");
        producerProperties.setProperty("compression.type", "snappy");
        producerProperties.setProperty("max.block.ms", "60000");
        producerProperties.setProperty("linger.ms", "20");
        producerProperties.setProperty("batch.size", Integer.toString(32768));
        producerProperties.put("value.serializer", FuncEventSerializer.class.getName());
        return producerProperties;
    }

    private Properties getAdminClientProperties() {
        Properties clientProperties = this.getProperties();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", clientProperties.getProperty("bootstrap.servers"));
        return properties;
    }

    public void setStartCorrelation(boolean startCorrelation) {
        this.startCorrelation = startCorrelation;
    }
}
