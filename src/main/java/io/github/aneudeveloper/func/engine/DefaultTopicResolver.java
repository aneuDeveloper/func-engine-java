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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.processor.RecordContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.aneudeveloper.func.engine.function.FuncEvent;
import io.github.aneudeveloper.func.engine.function.FuncEvent.Type;

public class DefaultTopicResolver implements TopicResolver {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultTopicResolver.class);

    private String prefix;
    private String delayTopicName;

    public DefaultTopicResolver(String prefix, String delayTopicName) {
        this.prefix = prefix;
        this.delayTopicName = delayTopicName;
    }

    @Override
    public String resolveByType(Type type) {
        if (type == null) {
            return Type.DEAD_LETTER.name();
        }

        switch (type) {
            case DEAD_LETTER:
            case TRANSIENT:
                return type.name();
            default:
                return this.prefix + FuncEvent.Type.WORKFLOW.name();
        }
    }

    @Override
    public <T> String resolve(T message, RecordContext recordContext) {
        try {
            Iterable<Header> typeHeaders = recordContext.headers().headers(FuncEvent.TYPE);
            String type = null;
            if (typeHeaders != null && typeHeaders.iterator().hasNext()) {
                type = new String(typeHeaders.iterator().next().value());
            }

            Iterable<Header> executeAtHeader = recordContext.headers().headers(FuncEvent.EXECUTE_AT);
            String executeAt = null;
            if (executeAtHeader != null && executeAtHeader.iterator().hasNext()) {
                executeAt = new String(executeAtHeader.iterator().next().value());
            }

            if (type != null && !type.isEmpty() && executeAt != null && !executeAt.isEmpty()) {
                return delayTopicName;
            }
            if (type != null && !type.isEmpty()) {
                return resolveByType(Type.valueOf(type));
            }
            return resolveByType(Type.DEAD_LETTER);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            return resolveByType(Type.DEAD_LETTER);
        }
    }

}
