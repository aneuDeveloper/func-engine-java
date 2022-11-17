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

import java.util.Collection;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.RecordContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import func.engine.correlation.CorrelationState;
import func.engine.function.FunctionEvent;
import func.engine.function.FunctionEventTransformer;

public class FunctionsStream<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionsStream.class);
    private FunctionsWorkflow<T> processDefinition;
    private KafkaStreams streams;
    private FunctionExecuter<T> processEventExecuter;

    public FunctionsStream(FunctionsWorkflow<T> processDefinition, FunctionExecuter<T> processEventExecuter) {
        this.processDefinition = processDefinition;
        this.processEventExecuter = processEventExecuter;
    }

    public void start(Collection<String> topics) {
        if (topics == null || topics.isEmpty()) {
            throw new IllegalStateException("Cannot start workflow without any topics to observe.");
        }
        LOGGER.info("Starting workflow stream for following topics: {}", topics.toString());
        Serde<FunctionEvent> processEventSerde = this.processDefinition.getSerde();
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, FunctionEvent> stream = streamsBuilder.stream(topics,
                Consumed.with(Serdes.String(), processEventSerde));

        stream.filter((key, value) -> this.shouldExecuteFuntion(key, value)) //
                .mapValues(this.processEventExecuter::executeMessageAndDiscoverNextStep) //
                .transform(() -> this.createTransformer(), new String[0]) //
                .selectKey((key, functionEvent) -> this.selectTopicKey(key, functionEvent))
                .to((key, functionEvent, recordContext) -> this.toTopic(key, functionEvent, recordContext),
                        Produced.with(Serdes.String(), processEventSerde));

        Properties properties = this.prepareProperties(topics.iterator().next());
        this.streams = new KafkaStreams(streamsBuilder.build(), properties);
        this.streams.start();
    }

    private boolean shouldExecuteFuntion(String key, FunctionEvent value) {
        boolean shouldExecute = (value.getType() == null || value.getType() == FunctionEvent.Type.WORKFLOW)
                && value.getFunction() != null && !value.getFunction().isBlank();
        return shouldExecute;
    }

    private Properties prepareProperties(String topicName) {
        Properties properties = this.processDefinition.getServiceConfig().getWorkflowStreamProperties();
        String applicationNamePrefix = this.processDefinition.getProperty(FunctionsWorkflow.WORKFLOW_STREAM_PREFIX,
                this.processDefinition.getProcessName());
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationNamePrefix + "-" + topicName);
        return properties;
    }

    private FunctionEventTransformer createTransformer() {
        return new FunctionEventTransformer() {

            @Override
            public KeyValue<String, FunctionEvent> transform(String key, FunctionEvent functionEvent) {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Message received: {}",  new String(FunctionsStream.this.processDefinition
                            .getSerde().serializer().serialize(null, functionEvent)));
                }
                if (functionEvent.getNextRetryAt() != null) {
                    functionEvent.setSourceTopic(this.context.topic());
                }
                return new KeyValue<String, FunctionEvent>(key, functionEvent);
            }
        };
    }

    private String selectTopicKey(String key, FunctionEvent functionEvent) {
        if (this.shouldCreateCorrelationMessage(functionEvent)) {
            String topicKeyIncludesProcessNameAndCorrelationId = this.processDefinition
                    .getProcessNameWithCorrelationId(functionEvent.getCorrelationId());
            return topicKeyIncludesProcessNameAndCorrelationId;
        }
        if (functionEvent.getNextRetryAt() != null) {
            return functionEvent.getId();
        }
        return key;
    }

    private String toTopic(String key, FunctionEvent functionEvent, RecordContext recordContext) {
        String topic = this.processDefinition.getTopicResolver().resolveTopicName(functionEvent.getType());
        return topic;
    }

    private boolean shouldCreateCorrelationMessage(FunctionEvent functionEvent) {
        if (functionEvent.getCorrelationState() != CorrelationState.INITIALIZED) {
            return false;
        }
        String correlationId = functionEvent.getCorrelationId();
        return correlationId != null && !correlationId.isBlank();
    }

    public void close() {
        LOGGER.info("Shutdown workflow stream");
        this.streams.close();
    }
}
