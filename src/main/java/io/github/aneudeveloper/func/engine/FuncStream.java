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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Optional;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
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
import io.github.aneudeveloper.func.engine.function.FuncEventBuilder;

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
        Serde<T> processEventSerde = this.processDefinition.getSerde();
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, T> stream = streamsBuilder.stream(topics,
                Consumed.with(Serdes.String(), processEventSerde));

        stream
                .process(() -> new Processor<String, T, String, T>() {
                    private ProcessorContext<String, T> context;

                    @Override
                    public void init(ProcessorContext<String, T> context) {
                        this.context = context;
                    }

                    @Override
                    public void process(Record<String, T> record) {
                        T message = record.value();
                        if (LOGGER.isTraceEnabled()) {
                            LOGGER.trace("Message received: {}", new String(FuncStream.this.processDefinition
                                    .getSerde().serializer().serialize(null, message)));
                        }

                        FuncEvent<T> funcEvent = processDefinition.getFuncEventMapper().from(record.headers(),
                                message);
                        try {
                            if (!shouldExecuteFuntion(funcEvent)) {
                                return;
                            }
                            FuncEvent<T> nextEvent = processEventExecuter.executeMessageAndDiscoverNextStep(funcEvent);

                            Headers nextHeaders = processDefinition.getFuncEventMapper().toHeader(null, nextEvent);
                            Optional<RecordMetadata> metadata = context.recordMetadata();
                            if (metadata.isPresent()) {
                                String topic = metadata.get().topic();
                                nextHeaders.add(FuncEvent.DESTINATION_TOPIC, topic.getBytes());
                            }

                            Record<String, T> nextRecord = record.withValue(nextEvent.getContext())
                                    .withHeaders(nextHeaders);

                            if (nextEvent != null && nextEvent.getType() != null && nextEvent.getType() == Type.DELAY) {
                                if (LOGGER.isTraceEnabled()) {
                                    LOGGER.trace("define key for DELAY topic key: {}", funcEvent.getId());
                                }
                                nextRecord = nextRecord.withKey(funcEvent.getId());
                            }

                            context.forward(nextRecord);
                        } catch (Exception e) {
                            LOGGER.error("Error while executing function. ProcessInstanceID={} functionId={}",
                                    funcEvent.getProcessInstanceID(), funcEvent.getId(), e);

                            FuncEvent<T> nextFunction = FuncEventBuilder.newEvent();
                            nextFunction.setProcessName(funcEvent.getProcessName());
                            nextFunction.setComingFromId(funcEvent.getId());
                            nextFunction.setProcessInstanceID(funcEvent.getProcessInstanceID());
                            nextFunction.setType(FuncEvent.Type.ERROR);

                            Headers header = FuncStream.this.processDefinition.getFuncEventMapper().toHeader(null,
                                    nextFunction);

                            StringWriter sw = new StringWriter();
                            PrintWriter pw = new PrintWriter(sw);
                            e.printStackTrace(pw);

                            String destTopic = FuncStream.this.processDefinition.getTopicResolver()
                                    .resolveTopicName(FuncEvent.Type.ERROR.name());
                            try {
                                FuncStream.this.processDefinition.sendContentWait(destTopic, null, header,
                                        sw.toString().getBytes());
                            } catch (Exception errorProducerException) {
                                LOGGER.error("could not send exception to destTopic={} for functionId={}", destTopic,
                                        funcEvent.getId(), errorProducerException);
                                throw new RuntimeException("Could not write process error to topic. Try later.");
                            }
                        }
                    }
                }, Named.as(processDefinition.getProcessName() + "-processor"), new String[0])
                .to((key, funcContext, recordContext) -> this.toTopic(key, funcContext, recordContext),
                        Produced.with(Serdes.String(), processEventSerde));

        this.streams = new KafkaStreams(streamsBuilder.build(), processDefinition.getFuncStreamProperties());
        if (uncaughtExceptionHandler != null) {
            this.streams.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        }
        this.streams.start();
    }

    private boolean shouldExecuteFuntion(FuncEvent<T> value) {
        boolean shouldExecute = value.getType() == FuncEvent.Type.WORKFLOW ||
                value.getType() == FuncEvent.Type.TRANSIENT;
        return shouldExecute;
    }

    private String toTopic(String key, T functionEvent, RecordContext recordContext) {
        try {
            Iterable<Header> headers = recordContext.headers().headers(FuncEvent.TYPE);
            if (headers != null) {
                String topic = this.processDefinition.getTopicResolver()
                        .resolveTopicName(new String(headers.iterator().next().value()));
                return topic;
            }
        } catch (Exception e) {
            LOGGER.info(e.getMessage(), e);
        }
        return this.processDefinition.getTopicResolver().resolveTopicName(null);
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