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
package io.github.aneudeveloper.func.engine.function;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import io.github.aneudeveloper.func.engine.Retries;
import io.github.aneudeveloper.func.engine.function.FuncEvent.Type;

public class FuncEventBuilder {

    public static final <T> FuncEvent<T> newEvent() {
        FuncEvent<T> event = new FuncEvent<>("1", UUID.randomUUID().toString());
        event.setTimeStamp(ZonedDateTime.now());
        return event;
    }

    public static final <T> FuncEvent<T> newEventWorkflowPrefilled() {
        FuncEvent<T> event = new FuncEvent<T>("1", UUID.randomUUID().toString())//
        .setTimeStamp(ZonedDateTime.now())
        .setProcessInstanceID(UUID.randomUUID().toString())
        .setType(Type.WORKFLOW);
        return event;
    }

    private static final <T> FuncEvent<T> empty() {
        FuncEvent<T> event = new FuncEvent<>("1", UUID.randomUUID().toString());
        event.setTimeStamp(ZonedDateTime.now());
        return event;
    }

    public static <T> FuncEvent<T> createAsChild(FuncEvent<T> parent) {
        FuncEvent<T> nextFunctionEvent = empty();
        nextFunctionEvent.setProcessName(parent.getProcessName());
        nextFunctionEvent.setComingFromId(parent.getId());
        nextFunctionEvent.setProcessInstanceID(parent.getProcessInstanceID());
        nextFunctionEvent.setType(FuncEvent.Type.WORKFLOW);
        nextFunctionEvent.setContext(parent.getContext());
        return nextFunctionEvent;
    }

    public static <T> FuncEvent<T> nextTransient(FuncEvent<T> parent) {
        FuncEvent<T> nextFunctionEvent = empty();
        nextFunctionEvent.setProcessName(parent.getProcessName());
        nextFunctionEvent.setComingFromId(parent.getComingFromId());
        nextFunctionEvent.setProcessInstanceID(parent.getProcessInstanceID());
        nextFunctionEvent.setType(FuncEvent.Type.TRANSIENT);
        nextFunctionEvent.setContext(parent.getContext());
        return nextFunctionEvent;
    }

    public static <T> FuncEvent<T> retry(FuncEvent<T> parent, Retries... retriesArray) {
        Integer retryObject = parent.getRetryCount();
        int executedRetries = 0;
        if (retryObject != null) {
            executedRetries = retryObject;
        }
        ZonedDateTime currentTime = ZonedDateTime.now();

        Retries choosenRetryUnitWithNumber = null;
        if (retriesArray == null) {
            if (3 <= executedRetries) {
                // no retries anymore
                return null;
            }
            choosenRetryUnitWithNumber = Retries.build().retryTimes(3).in(5, ChronoUnit.MINUTES);
        } else {
            int maxPossibleRetries = 0;
            for (Retries retries : retriesArray) {
                maxPossibleRetries += retries.getRetryTimes();
                if (maxPossibleRetries <= executedRetries) {
                    continue;
                }
                choosenRetryUnitWithNumber = retries;
                break;
            }
        }
        if (choosenRetryUnitWithNumber == null) {
            return null;
        }
        long jdf = choosenRetryUnitWithNumber.getTime();
        ZonedDateTime nextRetryAt = currentTime.plus(jdf, choosenRetryUnitWithNumber.getTimeUnit());

        FuncEvent<T> retry = newEvent();
        retry.setProcessName(parent.getProcessName());
        retry.setComingFromId(parent.getId());
        retry.setType(FuncEvent.Type.DELAY);
        retry.setExecuteAt(nextRetryAt);
        retry.setRetryCount(executedRetries + 1);
        retry.setFunction(parent.getFunction());
        retry.setProcessInstanceID(parent.getProcessInstanceID());
        retry.setContext(parent.getContext());
        return retry;
    }
}
