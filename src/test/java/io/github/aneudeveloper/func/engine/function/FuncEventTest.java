package io.github.aneudeveloper.func.engine.function;

import java.time.temporal.ChronoUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.github.aneudeveloper.func.engine.Retries;

public class FuncEventTest {
    @Test
    public void shouldEndRetryCount() {
        FuncEvent<String> event = FuncEventBuilder.newEvent();
        for (int i = 0; i < 5; i++) {
            event = FuncEventBuilder.retry(event, //
                    Retries.build().retryTimes(1).in(5, ChronoUnit.MINUTES),
                    Retries.build().retryTimes(1).in(10, ChronoUnit.MINUTES),
                    Retries.build().retryTimes(2).in(1, ChronoUnit.HOURS));
        }
        Assertions.assertNull(event);

    }

    @Test
    public void shouldReturnEvent() {
        FuncEvent<String> event = FuncEventBuilder.newEvent();
        for (int i = 0; i < 3; i++) {
            event = FuncEventBuilder.retry(event, //
                    Retries.build().retryTimes(1).in(5, ChronoUnit.MINUTES),
                    Retries.build().retryTimes(1).in(10, ChronoUnit.MINUTES),
                    Retries.build().retryTimes(2).in(1, ChronoUnit.HOURS));
        }
        Assertions.assertNotNull(event);

    }
}
