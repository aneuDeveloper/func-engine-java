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

public class FuncEvent<T> {
    public static enum Type {
        END, DEAD_LETTER, WORKFLOW, TRANSIENT, ERROR, DELAY;
    }

    public static final String VERSION = "v";
    public static final String ID = "id";
    public static final String PROCESS_NAME = "process_name";
    public static final String COMING_FROM_ID = "coming_from_id";
    public static final String FUNCTION = "function";
    public static final String PROCESS_INSTANCE_ID = "process_instance_id";
    public static final String RETRY_COUNT = "retry_count";
    public static final String EXECUTE_AT = "execute_at";
    public static final String DESTINATION_TOPIC = "destination_topic";
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
    private ZonedDateTime executeAt;
    private Integer retryCount;
    private String sourceTopic;
    private T context;

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

    public FuncEvent<T> setId(String id) {
        this.id = id;
        return this;
    }

    public String getComingFromId() {
        return this.comingFromId;
    }

    public FuncEvent<T> setComingFromId(String comingFromId) {
        this.comingFromId = comingFromId;
        return this;
    }

    public String getProcessInstanceID() {
        return this.processInstanceID;
    }

    public FuncEvent<T> setProcessInstanceID(String processInstanceID) {
        this.processInstanceID = processInstanceID;
        return this;
    }

    public ZonedDateTime getExecuteAt() {
        return this.executeAt;
    }

    public FuncEvent<T> setExecuteAt(ZonedDateTime nextRetryAt) {
        this.executeAt = nextRetryAt;
        return this;
    }

    public Integer getRetryCount() {
        return this.retryCount;
    }

    public FuncEvent<T> setRetryCount(Integer retryCount) {
        this.retryCount = retryCount;
        return this;
    }

    public String getFunction() {
        return this.function;
    }

    public FuncEvent<T> setFunction(String function) {
        this.function = function;
        return this;
    }

    public ZonedDateTime getTimeStamp() {
        return this.timeStamp;
    }

    public FuncEvent<T> setTimeStamp(ZonedDateTime timeStamp) {
        this.timeStamp = timeStamp;
        return this;
    }

    public Type getType() {
        return this.type;
    }

    public FuncEvent<T> setType(Type type) {
        this.type = type;
        return this;
    }

    public String getDestinationTopic() {
        return this.sourceTopic;
    }

    public FuncEvent<T> setDestinationTopic(String sourceTopic) {
        this.sourceTopic = sourceTopic;
        return this;
    }

    public T getContext() {
        return context;
    }

    public FuncEvent<T> setContext(T context) {
        this.context = context;
        return this;
    }

}
