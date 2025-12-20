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
package io.github.aneudeveloper.func.engine;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.aneudeveloper.func.engine.function.FuncEvent;
import io.github.aneudeveloper.func.engine.function.FuncEventBuilder;

public class FuncEventMapper<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FuncEventMapper.class);
    private static final DateTimeFormatter TIME_STAMP_FORMATTER = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    public FuncEvent<T> from(Headers headers, T t) {
        FuncEvent<T> event = FuncEventBuilder.newEvent();
        event.setContext(t);
        event.setType(getHeaderType(FuncEvent.TYPE, headers));
        event.setId(getHeader(FuncEvent.ID, headers));
        event.setComingFromId(getHeader(FuncEvent.COMING_FROM_ID, headers));
        event.setProcessName(getHeader(FuncEvent.PROCESS_NAME, headers));
        event.setProcessInstanceID(getHeader(FuncEvent.PROCESS_INSTANCE_ID, headers));
        event.setRetryCount(getHeaderAsInt(FuncEvent.RETRY_COUNT, headers));
        event.setExecuteAt(getHeaderAsDate(FuncEvent.NEXT_RETRY_AT, headers));
        event.setTimeStamp(getHeaderAsDate(FuncEvent.TIMESTAMP, headers));
        event.setFunction(getHeader(FuncEvent.FUNCTION, headers));
        return event;
    }

    private String getHeader(String key, Headers headers) {
        Iterable<Header> availableHeaders = headers.headers(key);
        if (availableHeaders != null) {
            Iterator<Header> iterator = availableHeaders.iterator();
            if (iterator != null && iterator.hasNext()) {
                Header header = iterator.next();
                if (header != null) {
                    return new String(header.value());
                }
            }
        }
        return null;
    }

    private Integer getHeaderAsInt(String key, Headers headers) {
        String intAsString = getHeader(key, headers);
        if (intAsString != null && !intAsString.isEmpty()) {
            return Integer.parseInt(intAsString);
        }
        return null;
    }

    private ZonedDateTime getHeaderAsDate(String key, Headers headers) {
        String asString = getHeader(key, headers);
        if (asString != null && !asString.isEmpty()) {
            try {
                return ZonedDateTime.parse(asString, TIME_STAMP_FORMATTER);
            } catch (Exception e) {
                LOGGER.warn("key={} could not be parsed with value={}", key, asString, e);
            }
        }
        return null;
    }

    private FuncEvent.Type getHeaderType(String key, Headers headers) {
        String asString = getHeader(key, headers);
        if (asString != null && !asString.isEmpty()) {
            return FuncEvent.Type.valueOf(asString);
        }
        return null;
    }

    public Headers toHeader(Headers headers, FuncEvent<T> event) {
        if (headers == null) {
            headers = new RecordHeaders();
        }
        if (event.getVersion() != null) {
            headers.add(FuncEvent.VERSION, event.getVersion().getBytes());
        }
        if (event.getId() != null) {
            headers.add(FuncEvent.ID, event.getId().getBytes());
        }
        if (event.getComingFromId() != null) {
            headers.add(FuncEvent.COMING_FROM_ID, event.getComingFromId().getBytes());
        }
        if (event.getType() != null) {
            headers.add(FuncEvent.TYPE, event.getType().name().getBytes());
        }
        if (event.getFunction() != null) {
            headers.add(FuncEvent.FUNCTION, event.getFunction().getBytes());
        }
        if (event.getExecuteAt() != null) {
            headers.add(FuncEvent.NEXT_RETRY_AT,
                    event.getExecuteAt().format(TIME_STAMP_FORMATTER).getBytes());
        }
        if (event.getTimeStamp() != null) {
            headers.add(FuncEvent.TIMESTAMP,
                    event.getTimeStamp().format(TIME_STAMP_FORMATTER).getBytes());
        }
        return headers;
    }

}
