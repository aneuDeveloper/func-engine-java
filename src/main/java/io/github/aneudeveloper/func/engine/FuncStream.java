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

import java.util.Collection;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.RecordContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.aneudeveloper.func.engine.function.FuncEvent;
import io.github.aneudeveloper.func.engine.function.FuncEvent.Type;
import io.github.aneudeveloper.func.engine.function.FuncEventTransformer;

public class FuncStream<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FuncStream.class);
    private FuncEngine<T> processDefinition;
    private KafkaStreams streams;
    private FuncExecuter<T> processEventExecuter;

    public FuncStream(FuncEngine<T> processDefinition, FuncExecuter<T> processEventExecuter) {
        this.processDefinition = processDefinition;
        this.processEventExecuter = processEventExecuter;
    }

    public void start(Collection<String> topics) {
        if (topics == null || topics.isEmpty()) {
            throw new IllegalStateException("Cannot start workflow without any topics to observe.");
        }
        LOGGER.info("Starting workflow stream for following topics: {}", topics.toString());
        Serde<FuncEvent<T>> processEventSerde = this.processDefinition.getSerde();
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, FuncEvent<T>> stream = streamsBuilder.stream(topics,
                Consumed.with(Serdes.String(), processEventSerde));

        stream.filter(this::shouldExecuteFuntion) //
                .mapValues(this.processEventExecuter::executeMessageAndDiscoverNextStep) //
                .transform(() -> this.createTransformer(), new String[0]) //
                .selectKey((key, functionEvent) -> this.selectTopicKey(key, functionEvent))//
                .to((key, functionEvent, recordContext) -> this.toTopic(key, functionEvent, recordContext),
                        Produced.with(Serdes.String(), processEventSerde));

        this.streams = new KafkaStreams(streamsBuilder.build(), processDefinition.getFuncStreamProperties());
        this.streams.start();
    }

    private boolean shouldExecuteFuntion(String key, FuncEvent<T> value) {
        boolean shouldExecute = (value.getType() == null || value.getType() == FuncEvent.Type.WORKFLOW)
                && value.getFunction() != null && !value.getFunction().isBlank();
        return shouldExecute;
    }

    private FuncEventTransformer<T> createTransformer() {
        return new FuncEventTransformer<T>() {

            @Override
            public KeyValue<String, FuncEvent<T>> transform(String key, FuncEvent<T> functionEvent) {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Message received: {}", new String(FuncStream.this.processDefinition
                            .getSerde().serializer().serialize(null, functionEvent)));
                }
                if (functionEvent.getType() != null && functionEvent.getType() == FuncEvent.Type.DELAY) {
                    functionEvent.setSourceTopic(this.context.topic());
                }
                return new KeyValue<String, FuncEvent<T>>(key, functionEvent);
            }
        };
    }

    private String selectTopicKey(String key, FuncEvent<T> functionEvent) {
        if (functionEvent.getType() != null && functionEvent.getType() == Type.DELAY) {
            return functionEvent.getId();
        }
        return key;
    }

    private String toTopic(String key, FuncEvent<T> functionEvent, RecordContext recordContext) {
        String topic = this.processDefinition.getTopicResolver().resolveTopicName(functionEvent.getType());
        return topic;
    }

    public void close() {
        LOGGER.info("Shutdown workflow stream");
        this.streams.close();
    }
}