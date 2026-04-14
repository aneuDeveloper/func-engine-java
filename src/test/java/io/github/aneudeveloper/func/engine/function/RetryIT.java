package io.github.aneudeveloper.func.engine.function;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.github.aneudeveloper.func.engine.FuncEngine;
import io.github.aneudeveloper.func.engine.Retries;
import io.github.aneudeveloper.func.engine.SendEventExceptionHandler;

@Tag("integration")
public class RetryIT {
    public static class TriggerRetry implements Func<String> {

        @Override
        public FuncEvent<String> work(FuncEvent<String> functionEvent) {
            return FuncEventBuilder.retry(functionEvent, Retries.ONE_TIME_IN_5_MINUTES);
        }

    }

    @Test
    public void testRetry() {
        FuncEngine<String> funcEngine = new FuncEngine<String>("MyTest", "127.0.0.1:9092");
        funcEngine.setFuncContextSerDes(new FuncContextSerDes<String>() {
            @Override
            public byte[] serialize(String data) {
                if (data == null) {
                    return "".getBytes();
                }
                return data.getBytes();
            }

            @Override
            public String deserialize(byte[] data) {
                return new String(data);
            }

        });

        funcEngine.setFuncMapper(new FuncMapper<>() {
            @Override
            public Func<String> map(FuncEvent<String> functionEvent) {
                if (TriggerRetry.class.getName().equals(functionEvent.getFunction())) {
                    return new TriggerRetry();
                }
                return null;
            }

            @Override
            public String map(Func<String> function) {
                String name = function.getClass().getName();
                return name;
            }
        });

        funcEngine.setSendEventExceptionHandler(new SendEventExceptionHandler() {
            @Override
            public void onException(Exception originalException, Object... context) {
                System.err.println(originalException);
            }
        });

        funcEngine.setUncaughtExceptionHandler(new StreamsUncaughtExceptionHandler() {
            @Override
            public StreamThreadExceptionResponse handle(Throwable arg0) {
                System.err.println(arg0);
                return StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
            }
        });
        FuncEvent<String> newEvent = FuncEventBuilder.newEventWorkflowPrefilled();
        newEvent.setFunction(TriggerRetry.class.getName());
        String messageValue = "message from " + System.currentTimeMillis();
        newEvent.setContext(messageValue);

        funcEngine.start();

        try {
            funcEngine.execute(newEvent);
            Thread.sleep(15000);
            funcEngine.close();

            KafkaConsumerTestHelper kafkaComsumerTestHelper = new KafkaConsumerTestHelper();
            List<ConsumerRecord<String, String>> messages = kafkaComsumerTestHelper.getMessages("DELAY", "");

            boolean foundMessage = messages.stream().anyMatch(m -> messageValue.equals(m.value()));
            Assertions.assertNotNull(foundMessage);

        } catch (Exception e) {
            Assertions.assertTrue(false);
        }

    }
}
