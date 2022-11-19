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
package func.engine.correlation;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.RecordContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import func.engine.FunctionsWorkflow;
import func.engine.function.FunctionContextSerDes;
import func.engine.function.FunctionEvent;
import func.engine.function.FunctionEventUtil;

public class CorrelationStream<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CorrelationStream.class);
    private FunctionsWorkflow<T> processDefinition;
    private KafkaStreams stream;

    public CorrelationStream(FunctionsWorkflow<T> processDefinition) {
        this.processDefinition = processDefinition;
    }

    public void startStreaming() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        Serde<FunctionEvent> processEventSerde = this.processDefinition.getSerde();
        String callbackTopic = this.processDefinition.getTopicResolver().resolveTopicName(FunctionEvent.Type.CALLBACK);

        KStream<String, FunctionEvent> callbackStream = streamsBuilder.stream(callbackTopic,
                Consumed.with(Serdes.String(), processEventSerde));

        String correlationTopic = this.processDefinition.getTopicResolver()
                .resolveTopicName(FunctionEvent.Type.CORRELATION);

        KTable<String, FunctionEvent> correlationTable = streamsBuilder.table(correlationTopic,
                Consumed.with(Serdes.String(), processEventSerde));

        callbackStream.join(correlationTable, this::mergeValues) //
                .to(this::selectDestinationTopic, Produced.with(Serdes.String(), processEventSerde));

        Topology topology = streamsBuilder.build();
        Properties properties = this.processDefinition.getServiceConfig().getCorrelationStreamProperties();
        this.stream = new KafkaStreams(topology, properties);
        this.stream.start();
    }

    private FunctionEvent mergeValues(final FunctionEvent callbackProcessEvent,
            final FunctionEvent correlationProcessEvent) {
        try {
            FunctionEvent callbackReceivedMessage = FunctionEventUtil.createWithDefaultValues();
            callbackReceivedMessage.setProcessName(correlationProcessEvent.getProcessName());
            callbackReceivedMessage.setFunction(correlationProcessEvent.getFunction());
            callbackReceivedMessage.setProcessInstanceID(correlationProcessEvent.getProcessInstanceID());
            callbackReceivedMessage.setComingFromId(correlationProcessEvent.getId());
            callbackReceivedMessage.setCorrelationState(CorrelationState.CALLBACK_FORWARDED);
            callbackReceivedMessage.setType(FunctionEvent.Type.WORKFLOW);
            FunctionContextSerDes<T> dataSerDes = this.processDefinition.getDataSerDes();
            T callbackData = dataSerDes.deserialize(callbackProcessEvent.getData());
            T originalData = dataSerDes.deserialize(correlationProcessEvent.getData());
            if (this.processDefinition.getCorrelationMerger() == null) {
                throw new IllegalStateException(
                        "Correlation merger has not been defined. Please define the merger through configuration with this property correlation.merger.class");
            }
            T mergedValue = this.processDefinition.getCorrelationMerger().mergeCorrelation(callbackData, originalData,
                    new CorrelationContext() {

                        @Override
                        public FunctionEvent getCorrelationEvent() {
                            return correlationProcessEvent;
                        }

                        @Override
                        public FunctionEvent getCallbackEvent() {
                            return callbackProcessEvent;
                        }
                    });
            String serializedAsString = dataSerDes.serialize(mergedValue);
            callbackReceivedMessage.setData(serializedAsString);
            return callbackReceivedMessage;
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException("Could not merge correlation events. Message: " + e.getMessage());
        }
    }

    private String selectDestinationTopic(String key, FunctionEvent functionEvent, RecordContext recordContext) {
        try {
            String destinationTopic = this.processDefinition.getTopicResolver()
                    .resolveTopicName(functionEvent.getType());
            return destinationTopic;
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException("Could not choose destination topic. Message " + e.getMessage());
        }
    }

    public void close() {
        LOGGER.info("Shutdown stream correlation stream.");
        this.stream.close();
    }
}
