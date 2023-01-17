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
package func.engine.function;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import func.engine.Retries;
import func.engine.correlation.CorrelationState;

public class FuncEvent<T> {
    public static enum Type {
        END, CORRELATION, CALLBACK, DEAD_LETTER, RETRY, WORKFLOW, TRANSIENT, ERROR;
    }

    public static final String VERSION = "v";
    public static final String ID = "id";
    public static final String PROCESS_NAME = "processName";
    public static final String COMING_FROM_ID = "comingFromId";
    public static final String FUNCTION = "function";
    public static final String PROCESS_INSTANCE_ID = "processInstanceID";
    public static final String CORRELATION_STATE = "correlationState";
    public static final String CORRELATION_ID = "correlationId";
    public static final String RETRY_COUNT = "retryCount";
    public static final String NEXT_RETRY_AT = "nextRetryAt";
    public static final String SOURCE_TOPIC = "sourceTopic";
    public static final String TYPE = "type";
    public static final String TIMESTAMP = "timestamp";

    private String version;
    private String id;
    private ZonedDateTime timeStamp;
    private String processName;
    private String comingFromId;
    private String processInstanceID;
    private String function;
    private Type type;
    private ZonedDateTime nextRetryAt;
    private int retryCount;
    private String sourceTopic;
    private CorrelationState correlationState;

    private volatile String correlationId;
    private volatile IFunc functionObj;
    private volatile T context;
    private volatile Throwable error;

    protected FuncEvent(String version) {
        this.version = version;
    }

    protected FuncEvent(String version, String id) {
        this.version = version;
        this.id = id;
    }

    public String getVersion() {
        return this.version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getProcessName() {
        return this.processName;
    }

    public void setProcessName(String processName) {
        this.processName = processName;
    }

    public String getId() {
        return this.id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getComingFromId() {
        return this.comingFromId;
    }

    public void setComingFromId(String comingFromId) {
        this.comingFromId = comingFromId;
    }

    public String getProcessInstanceID() {
        return this.processInstanceID;
    }

    public void setProcessInstanceID(String processInstanceID) {
        this.processInstanceID = processInstanceID;
    }

    public ZonedDateTime getNextRetryAt() {
        return this.nextRetryAt;
    }

    public void setNextRetryAt(ZonedDateTime nextRetryAt) {
        this.nextRetryAt = nextRetryAt;
    }

    public int getRetryCount() {
        return this.retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public CorrelationState getCorrelationState() {
        return this.correlationState;
    }

    public void setCorrelationState(CorrelationState correlationState) {
        this.correlationState = correlationState;
    }

    public String getCorrelationId() {
        return this.correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    public String getFunction() {
        return this.function;
    }

    public void setFunction(String function) {
        this.function = function;
    }

    public IFunc getFunctionObj() {
        return this.functionObj;
    }

    public void setFunctionObj(IFunc functionObj) {
        this.functionObj = functionObj;
    }

    public ZonedDateTime getTimeStamp() {
        return this.timeStamp;
    }

    public void setTimeStamp(ZonedDateTime timeStamp) {
        this.timeStamp = timeStamp;
    }

    public Type getType() {
        return this.type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public String getSourceTopic() {
        return this.sourceTopic;
    }

    public void setSourceTopic(String sourceTopic) {
        this.sourceTopic = sourceTopic;
    }

    public T getContext() {
        return context;
    }

    public void setContext(T context) {
        this.context = context;
    }

    public Throwable getError() {
        return error;
    }

    public void setError(Throwable error) {
        this.error = error;
    }

    public static final <T> FuncEvent<T> createWithDefaultValues() {
        FuncEvent<T> functionEvent = new FuncEvent<>(FuncEventDeserializer.VERSION, UUID.randomUUID().toString());
        functionEvent.setTimeStamp(ZonedDateTime.now());
        return functionEvent;
    }

    public FuncEvent<T> correlation(String correlationId) {
        if (correlationId == null) {
            throw new IllegalStateException("CorrelationId must be specified.");
        }
        FuncEvent<T> correlation = FuncEvent.createWithDefaultValues();
        correlation.setProcessName(getProcessName());
        correlation.setProcessInstanceID(getProcessInstanceID());
        correlation.setType(FuncEvent.Type.CORRELATION);
        correlation.setCorrelationState(CorrelationState.INITIALIZED);
        correlation.setCorrelationId(correlationId);
        correlation.setFunction(getFunction());
        correlation.setComingFromId(getId());
        correlation.setContext(getContext());
        return correlation;
    }

    public FuncEvent<T> next(IFunc nextFunction) {
        FuncEvent<T> nextFunctionEvent = FuncEvent.createWithDefaultValues();
        nextFunctionEvent.setProcessName(getProcessName());
        nextFunctionEvent.setComingFromId(getId());
        nextFunctionEvent.setProcessInstanceID(getProcessInstanceID());
        nextFunctionEvent.setType(FuncEvent.Type.WORKFLOW);
        nextFunctionEvent.setContext(getContext());
        nextFunctionEvent.setFunctionObj(nextFunction);
        return nextFunctionEvent;
    }

    public FuncEvent<T> nextTransient(IFunc function) {
        FuncEvent<T> nextFunction = FuncEvent.createWithDefaultValues();
        nextFunction.setProcessName(getProcessName());
        nextFunction.setComingFromId(getId());
        nextFunction.setProcessInstanceID(getProcessInstanceID());
        nextFunction.setType(FuncEvent.Type.TRANSIENT);
        nextFunction.setFunctionObj(function);
        nextFunction.setContext(getContext());
        return nextFunction;
    }

    public FuncEvent<T> retry(Retries... retriesArray) {
        int executedRetries = getRetryCount();
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
                if ((maxPossibleRetries += retries.getRetryTimes()) <= executedRetries
                        || maxPossibleRetries <= executedRetries)
                    continue;
                choosenRetryUnitWithNumber = retries;
                break;
            }
        }
        if (choosenRetryUnitWithNumber == null) {
            return null;
        }
        long jdf = choosenRetryUnitWithNumber.getTime();
        ZonedDateTime nextRetryAt = currentTime.plus(jdf, choosenRetryUnitWithNumber.getTimeUnit());

        FuncEvent<T> retry = FuncEvent.createWithDefaultValues();
        retry.setProcessName(getProcessName());
        retry.setComingFromId(getId());
        retry.setType(FuncEvent.Type.RETRY);
        retry.setNextRetryAt(nextRetryAt);
        retry.setRetryCount(getRetryCount() + 1);
        retry.setFunction(getFunction());
        retry.setProcessInstanceID(getProcessInstanceID());
        retry.setContext(getContext());
        return retry;
    }
}
