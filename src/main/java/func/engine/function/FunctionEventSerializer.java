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

import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import func.engine.correlation.CorrelationState;

public class FunctionEventSerializer implements Serializer<FunctionEvent> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionEventSerializer.class);
    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static final String PAYLOAD_SEPARATOR = "$e%,";

    private FunctionContextSerDes<FunctionEvent> serDes;

    public byte[] serialize(String topic, FunctionEvent functionEvent) {
        String retVal = "v=" + functionEvent.getVersion()
                + ",id=" + functionEvent.getId()
                + this.getValueAsMillis(",timestamp=", functionEvent.getTimeStamp())
                + this.getValue(",processName=", functionEvent.getProcessName())
                + this.getValue(functionEvent.getType())
                + this.getValue(",comingFromId=", functionEvent.getComingFromId())
                + this.getValue(",processInstanceID=", functionEvent.getProcessInstanceID())
                + this.getValue(",func=", functionEvent.getFunction())
                + this.getValue(",nextRetryAt=", functionEvent.getNextRetryAt()) + this.getRetryCount(functionEvent)
                + this.getValue(",sourceTopic=", functionEvent.getSourceTopic())
                + this.getValue(",correlationState=", functionEvent.getCorrelationState())
                + ",$e%,"
                + this.getValue("", this.serDes.serialize(functionEvent.getFunctionData()));
        LOGGER.trace("ProcessEvent is serialized to: {}", retVal);
        return retVal.getBytes();
    }

    private String getValueAsMillis(String variableName, ZonedDateTime date) {
        if (date == null) {
            return "";
        }
        return variableName + date.toInstant().toEpochMilli();
    }

    private String getValue(String variableName, ZonedDateTime date) {
        if (date == null) {
            return "";
        }
        String dataAsString = DateTimeFormatter.ISO_INSTANT.format(date);
        return variableName + dataAsString;
    }

    private String getValue(String variableName, CorrelationState state) {
        if (state == null) {
            return "";
        }
        return variableName + state.name();
    }

    private String getValue(String variableName, String value) {
        if (value == null) {
            return "";
        }
        return variableName + value;
    }

    private String getValue(FunctionEvent.Type type) {
        if (type == null) {
            return "";
        }
        return ",func_type=" + type.name();
    }

    private String getRetryCount(FunctionEvent functionEvent) {
        if (functionEvent.getRetryCount() <= 0) {
            return "";
        }
        return ",retryCount=" + functionEvent.getRetryCount();
    }

    public FunctionContextSerDes<FunctionEvent> getSerDes() {
        return serDes;
    }

    public void setSerDes(FunctionContextSerDes<FunctionEvent> serDes) {
        this.serDes = serDes;
    }
}
