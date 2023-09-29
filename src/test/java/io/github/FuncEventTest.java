package io.github;

import java.time.temporal.ChronoUnit;

import org.junit.Assert;
import org.junit.Test;

import io.github.aneudeveloper.func.engine.Retries;
import io.github.aneudeveloper.func.engine.function.FuncEvent;

public class FuncEventTest {
    @Test
    public void shouldEndRetryCount() {
        FuncEvent<Object> event = FuncEvent.newEvent();
        for (int i = 0; i < 5; i++) {
            event = event.retry( //
                    Retries.build().retryTimes(1).in(5, ChronoUnit.MINUTES),
                    Retries.build().retryTimes(1).in(10, ChronoUnit.MINUTES),
                    Retries.build().retryTimes(2).in(1, ChronoUnit.HOURS));
        }
        Assert.assertNull(event);

    }

    @Test
    public void shouldReturnEvent() {
        FuncEvent<Object> event = FuncEvent.newEvent();
        for (int i = 0; i < 4; i++) {
            event = event.retry( //
                    Retries.build().retryTimes(1).in(5, ChronoUnit.MINUTES),
                    Retries.build().retryTimes(1).in(10, ChronoUnit.MINUTES),
                    Retries.build().retryTimes(2).in(1, ChronoUnit.HOURS));
        }
        Assert.assertNotNull(event);

    }
}
