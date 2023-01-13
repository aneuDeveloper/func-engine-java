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

import java.text.ParseException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;

import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import func.engine.correlation.CorrelationState;

public class FuncEventDeserializer<T> implements Deserializer<FuncEvent<T>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FuncEventDeserializer.class);
    private FuncContextSerDes<T> serDes;

    public FuncEvent<T> deserialize(String topic, byte[] data) {
        String dataAsString = new String(data);
        FuncEvent<T> functionEvent = this.deserialize(dataAsString);
        return functionEvent;
    }

    public FuncEvent<T> deserialize(String messageAsString) {
        int indexOfEnd = messageAsString.indexOf("$e%,");
        String metadata = this.getMetaData(messageAsString, indexOfEnd);
        String clientData = messageAsString.substring(indexOfEnd + 4, messageAsString.length());
        FuncEvent<T> functionEvent = FuncEvent.createWithDefaultValues();
        functionEvent.setContext(serDes.deserialize(clientData));
        this.populateVariables(functionEvent, metadata);
        return functionEvent;
    }

    private String getMetaData(String data, int indexOfEnd) {
        return data.substring(0, indexOfEnd);
    }

    private void populateVariables(FuncEvent<T> functionEvent, String metadata) {
        for (String variable : metadata.split(",")) {
            String[] keyValuePair = variable.split("=");
            if (keyValuePair.length <= 1) {
                continue;
            }
            switch (keyValuePair[0]) {
                case "v": {
                    functionEvent.setVersion(keyValuePair[1]);
                    break;
                }
                case "id": {
                    functionEvent.setId(keyValuePair[1]);
                    break;
                }
                case "timestamp": {
                    functionEvent.setTimeStamp(this.parseDateFromMillis(keyValuePair[1]));
                    break;
                }
                case "processName": {
                    functionEvent.setProcessName(keyValuePair[1]);
                    break;
                }
                case "processInstanceID": {
                    functionEvent.setProcessInstanceID(keyValuePair[1]);
                    break;
                }
                case "comingFromId": {
                    functionEvent.setComingFromId(keyValuePair[1]);
                    break;
                }
                case "func": {
                    functionEvent.setFunction(keyValuePair[1]);
                    break;
                }
                case "func_type": {
                    try {
                        functionEvent.setType(FuncEvent.Type.valueOf(keyValuePair[1]));
                    } catch (Exception e) {
                        LOGGER.error("Unknown FunctionEvent.Type=" + keyValuePair[1]);
                    }
                    continue;
                }
                case "processStep": {
                    functionEvent.setFunction(keyValuePair[1]);
                    break;
                }
                case "correlationState": {
                    functionEvent.setCorrelationState(CorrelationState.valueOf(keyValuePair[1]));
                    break;
                }
                case "retryCount": {
                    functionEvent.setRetryCount(Integer.parseInt(keyValuePair[1]));
                    break;
                }
                case "nextRetryAt": {
                    try {
                        functionEvent.setNextRetryAt(ZonedDateTime.parse(keyValuePair[1]));
                    } catch (RuntimeException e) {
                        LOGGER.error(
                                "Could not parse NextRetryAt=" + keyValuePair[1] + " trying to parse other dateformet.",
                                e);
                        try {
                            Date parsedNextRetryAt = FuncEventSerializer.DATE_FORMAT.parse(keyValuePair[1]);
                            ZoneId id = ZoneId.systemDefault();
                            ZonedDateTime ofInstant = ZonedDateTime.ofInstant(parsedNextRetryAt.toInstant(), id);
                            functionEvent.setNextRetryAt(ofInstant);
                        } catch (ParseException parseException) {
                            LOGGER.error(parseException.getMessage(), parseException);
                        }
                    }
                    break;
                }
                case "sourceTopic": {
                    functionEvent.setSourceTopic(keyValuePair[1]);
                    break;
                }
            }
        }
    }

    private ZonedDateTime parseDateFromMillis(String millis) {
        try {
            long millisParsed = Long.parseLong(millis);
            ZonedDateTime dateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(millisParsed),
                    ZoneId.systemDefault());
            return dateTime;
        } catch (NumberFormatException e) {
            LOGGER.error(e.getMessage(), e);
            return null;
        }
    }

    public void setSerDes(FuncContextSerDes<T> serDes) {
        this.serDes = serDes;
    }
}
