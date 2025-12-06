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
import java.util.Optional;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.aneudeveloper.func.engine.function.FuncEvent;
import io.github.aneudeveloper.func.engine.function.FuncEvent.Type;

public class FuncStream<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FuncStream.class);
    private FuncEngine<T> processDefinition;
    private KafkaStreams streams;
    private FuncExecuter<T> processEventExecuter;
    private StreamsUncaughtExceptionHandler uncaughtExceptionHandler;

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
                .process(() -> new Processor<String, FuncEvent<T>, String, FuncEvent<T>>() {
                    private ProcessorContext<String, FuncEvent<T>> context;

                    @Override
                    public void init(ProcessorContext<String, FuncEvent<T>> context) {
                        this.context = context;
                    }

                    @Override
                    public void process(Record<String, FuncEvent<T>> record) {
                        FuncEvent<T> funcEvent = record.value();
                        if (LOGGER.isTraceEnabled()) {
                            LOGGER.trace("Message received: {}", new String(FuncStream.this.processDefinition
                                    .getSerde().serializer().serialize(null, funcEvent)));
                        }

                        Optional<RecordMetadata> metadata = context.recordMetadata();
                        if (metadata.isPresent()) {
                            String topic = metadata.get().topic();
                            funcEvent.setSourceTopic(topic);
                        }

                        context.forward(record.withValue(funcEvent));
                    }

                    @Override
                    public void close() {
                    }
                }, Named.as(processDefinition.getProcessName() + "-processor"), new String[0])
                .selectKey(new KeyValueMapper<String, FuncEvent<T>, String>() {
                    @Override
                    public String apply(String key, FuncEvent<T> funcEvent) {
                        if (funcEvent.getType() != null && funcEvent.getType() == Type.DELAY) {
                            if (LOGGER.isTraceEnabled()) {
                                LOGGER.trace("define key for DELAY topic key: {}", funcEvent.getId());
                            }
                            return funcEvent.getId();
                        }
                        return key;
                    }

                })
                .to((key, functionEvent, recordContext) -> this.toTopic(key, functionEvent, recordContext),
                        Produced.with(Serdes.String(), processEventSerde));

        this.streams = new KafkaStreams(streamsBuilder.build(), processDefinition.getFuncStreamProperties());
        if (uncaughtExceptionHandler != null) {
            this.streams.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        }
        this.streams.start();
    }

    private boolean shouldExecuteFuntion(String key, FuncEvent<T> value) {
        boolean shouldExecute = (value.getType() == null || value.getType() == FuncEvent.Type.WORKFLOW)
                && value.getFunction() != null && !value.getFunction().isBlank();
        return shouldExecute;
    }

    private String toTopic(String key, FuncEvent<T> functionEvent, RecordContext recordContext) {
        String topic = this.processDefinition.getTopicResolver().resolveTopicName(functionEvent.getType());
        return topic;
    }

    public void close() {
        LOGGER.info("Shutdown workflow stream");
        this.streams.close();
    }

    public void setUncaughtExceptionHandler(StreamsUncaughtExceptionHandler uncaughtExceptionHandler) {
        if (streams != null) {
            this.streams.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        } else {
            this.uncaughtExceptionHandler = uncaughtExceptionHandler;
        }
    }
}