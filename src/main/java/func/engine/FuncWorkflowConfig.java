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

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import func.engine.function.FuncEventDeserializer;
import func.engine.function.FuncEventSerializer;

public class FuncWorkflowConfig<T> {
    private FuncWorkflow<T> processDefinition;

    public FuncWorkflowConfig(FuncWorkflow<T> processDefinition) {
        this.processDefinition = processDefinition;
    }

    public Properties getProducerProperties() {
        Properties producerProperties = new Properties();
        producerProperties.setProperty("bootstrap.servers", this.processDefinition.getProperty("bootstrap.servers"));
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

    public Properties getConsumerProperties(String groupId) {
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty("bootstrap.servers", this.processDefinition.getProperty("bootstrap.servers"));
        consumerProperties.setProperty("key.deserializer", StringDeserializer.class.getName());
        consumerProperties.setProperty("value.deserializer", FuncEventDeserializer.class.getName());
        consumerProperties.setProperty("group.id", groupId);
        consumerProperties.setProperty("enable.auto.commit", "false");
        consumerProperties.setProperty("max.poll.records", "1");
        consumerProperties.setProperty("auto.offset.reset", "earliest");
        return consumerProperties;
    }

    public Properties getAdminClientProperties() {
        Properties clientProperties = this.processDefinition.getProperties();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", clientProperties.getProperty("bootstrap.servers"));
        return properties;
    }

    public Properties getCorrelationStreamProperties() {
        Properties properties = new Properties();
        properties.put("application.id", this.processDefinition.getProperty("correlation.stream.application.name", this.processDefinition.getProcessName() + "-CorrelationStream"));
        properties.put("bootstrap.servers", this.processDefinition.getProperty("bootstrap.servers"));
        properties.put("auto.offset.reset", "earliest");
        properties.put("default.key.serde", Serdes.String().getClass());
        properties.put("num.stream.threads", this.processDefinition.getProperty("steps.num.stream.threads.config", "1"));
        properties.put("processing.guarantee", "exactly_once_v2");
        short replicationFactor = Short.valueOf(this.processDefinition.getProperty("topic.default.replication.factor", "1").toString());
        properties.put("replication.factor", "" + replicationFactor);
        return properties;
    }

    public Properties getWorkflowStreamProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", this.processDefinition.getProperty("bootstrap.servers"));
        properties.put("auto.offset.reset", "earliest");
        properties.put("default.key.serde", Serdes.String().getClass());
        properties.put("num.stream.threads", this.processDefinition.getProperty("steps.num.stream.threads.config", "1"));
        properties.put("default.deserialization.exception.handler", this.processDefinition.getProperty("default.deserialization.exception.handler", "org.apache.kafka.streams.errors.LogAndContinueExceptionHandler"));
        properties.put("processing.guarantee", "exactly_once_v2");
        return properties;
    }
}

