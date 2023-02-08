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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;

import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import func.engine.function.FuncEvent.Type;

public class FuncEventSerializer<T> implements Serializer<FuncEvent<T>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FuncEventSerializer.class);
    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static final String PAYLOAD_SEPARATOR = "$e%,";

    private FuncContextSerDes<T> serDes;
    private FuncSerDes funcSerDes;

    public FuncEventSerializer(FuncContextSerDes<T> serDes, FuncSerDes funcSerDes) {
        this.serDes = serDes;
        this.funcSerDes = funcSerDes;
    }

    public byte[] serialize(String topic, FuncEvent<T> functionEvent) {
        StringBuilder builder = new StringBuilder();
        builder.append("v=").append(functionEvent.getVersion());
        builder.append(",id=").append(functionEvent.getId());
        if (functionEvent.getTimeStamp() != null) {
            builder.append(",timestamp=").append(functionEvent.getTimeStamp().toInstant().toEpochMilli());
        }
        if (functionEvent.getProcessName() != null) {
            builder.append(",processName=").append(functionEvent.getProcessName());
        }
        if (functionEvent.getType() != null) {
            builder.append(",func_type=").append(functionEvent.getType().name());
        }
        if (functionEvent.getComingFromId() != null) {
            builder.append(",comingFromId=").append(functionEvent.getComingFromId());
        }
        if (functionEvent.getProcessInstanceID() != null) {
            builder.append(",processInstanceID=").append(functionEvent.getProcessInstanceID());
        }
        if (functionEvent.getFunction() != null) {
            builder.append(",func=").append(functionEvent.getFunction());
        } else if (functionEvent.getFunctionObj() != null) {
            builder.append(",func=").append(funcSerDes.serialize(functionEvent.getFunctionObj()));
        }

        if (functionEvent.getNextRetryAt() != null) {
            builder.append(",nextRetryAt=").append(functionEvent.getNextRetryAt().toInstant().toEpochMilli());
        }
        if (functionEvent.getSourceTopic() != null) {
            builder.append(",sourceTopic=").append(functionEvent.getSourceTopic());
        }
        if (functionEvent.getCorrelationState() != null) {
            builder.append(",correlationState=").append(functionEvent.getCorrelationState());
        }

        builder.append(",$e%,");
        if (functionEvent.getType() != null && functionEvent.getType() == Type.ERROR
                && functionEvent.getError() != null) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            functionEvent.getError().printStackTrace(pw);
            builder.append(sw.toString());
        } else if (functionEvent.getContext() != null) {
            builder.append(this.serDes.serialize(functionEvent.getContext()));
        }

        LOGGER.trace("ProcessEvent is serialized to: {}", builder.toString());
        return builder.toString().getBytes();
    }
}
