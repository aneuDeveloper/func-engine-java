package io.github.aneudeveloper.func.engine.function;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.github.aneudeveloper.func.engine.FuncEngine;
import io.github.aneudeveloper.func.engine.SendEventExceptionHandler;

@Tag("integration")
public class DefaultWorkflowIT {
    private static boolean sendEmail = false;
    private static String sendEmailId;

    private static boolean store = false;

    public static class SendEmai1 implements Func<String> {

        @Override
        public FuncEvent<String> work(FuncEvent<String> functionEvent) {
            sendEmailId = functionEvent.getId();
            functionEvent.setContext(functionEvent.getContext() + " sending email ");
            sendEmail = true;
            return FuncEventBuilder.createAsChild(functionEvent).setFunction("store");
        }

    }

    public static class Store implements Func<String> {

        @Override
        public FuncEvent<String> work(FuncEvent<String> functionEvent) {
            functionEvent.setContext(functionEvent.getContext() + " storing now");
            store = true;
            return null;
        }

    }

    @Test
    public void testCommonWorkflow() {
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
                if ("email".equals(functionEvent.getFunction())) {
                    return new SendEmai1();
                } else if ("store".equals(functionEvent.getFunction())) {
                    return new Store();
                }
                return null;
            }

            @Override
            public String map(Func<String> function) {
                if (function instanceof Store) {
                    return "store";
                }
                if (function instanceof SendEmai1) {
                    return "email";
                }
                return null;
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
        newEvent.setFunction("email");
        newEvent.setContext("message from " + System.currentTimeMillis());

        funcEngine.start();

        try {
            funcEngine.execute(newEvent);
            Thread.sleep(20000);
            funcEngine.close();
            Assertions.assertTrue(sendEmail);
            Assertions.assertTrue(store);

            KafkaComsumerTestHelper kafkaComsumerTestHelper = new KafkaComsumerTestHelper();
            List<ConsumerRecord<String, String>> messages = kafkaComsumerTestHelper.getMessages("MyTest-WORKFLOW",
                    "DefaultTest");
            Assertions.assertNotNull(kafkaComsumerTestHelper.getFuncById(messages, sendEmailId));

        } catch (Exception e) {
            Assertions.assertTrue(false);
        }

    }
}
