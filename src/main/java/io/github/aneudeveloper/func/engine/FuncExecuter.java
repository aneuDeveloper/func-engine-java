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

import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.aneudeveloper.func.engine.function.Func;
import io.github.aneudeveloper.func.engine.function.FuncEvent;
import io.github.aneudeveloper.func.engine.function.FuncEvent.Type;
import io.github.aneudeveloper.func.engine.function.IFunc;

public class FuncExecuter<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FuncExecuter.class);
    private FuncEngine<T> functionWorkflow;
    private TopicResolver topicResolver;

    public FuncExecuter(FuncEngine<T> functionWorkflow) {
        this.functionWorkflow = functionWorkflow;
        this.topicResolver = this.functionWorkflow.getTopicResolver();
    }

    public FuncEvent<T> executeMessageAndDiscoverNextStep(FuncEvent<T> functionEvent) {
        try {
            FuncEvent<T> nextFunctionEvent = this.executeMessage(functionEvent);
            return nextFunctionEvent;
        } catch (Throwable e) {
            LOGGER.error("Error while executing function. ProcessInstanceID=" + functionEvent.getProcessInstanceID()
                    + " functionId=" + functionEvent.getId(), e);
            FuncEvent<T> nextFunction = FuncEvent.newEvent();
            nextFunction.setProcessName(functionEvent.getProcessName());
            nextFunction.setComingFromId(functionEvent.getId());
            nextFunction.setProcessInstanceID(functionEvent.getProcessInstanceID());
            nextFunction.setType(FuncEvent.Type.ERROR);
            nextFunction.setError(e);
            return nextFunction;
        }
    }

    private FuncEvent<T> createEndEvent(FuncEvent<T> functionEvent) {
        FuncEvent<T> endEvent = FuncEvent.newEvent();
        endEvent.setProcessName(functionEvent.getProcessName());
        endEvent.setType(FuncEvent.Type.END);
        endEvent.setProcessInstanceID(functionEvent.getProcessInstanceID());
        endEvent.setComingFromId(functionEvent.getId());
        endEvent.setContext(functionEvent.getContext());
        return endEvent;
    }

    protected FuncEvent<T> executeMessage(FuncEvent<T> functionEvent)
            throws InterruptedException, ExecutionException {
        IFunc function = this.getFunctionObj(functionEvent);
        if (functionEvent.getType() == FuncEvent.Type.TRANSIENT) {
            return this.executeTransientFunction(functionEvent, (Func<T>) function);
        }
        LOGGER.trace("Execute stateful function id={} functionId={} function={}", new Object[] {
                functionEvent.getProcessInstanceID(), functionEvent.getId(), functionEvent.getFunction() });
        return this.executeStatefulFunction(functionEvent, (Func<T>) function);
    }

    private IFunc getFunctionObj(FuncEvent<T> functionEvent) {
        if (functionEvent.getFunctionObj() != null) {
            return functionEvent.getFunctionObj();
        }
        if (functionEvent.getFunction() != null) {
            IFunc functionObj = this.functionWorkflow.getFuncSerDes().deserialize(functionEvent);
            functionEvent.setFunctionObj(functionObj);
        }
        if (functionEvent.getFunctionObj() == null) {
            throw new IllegalStateException(
                    "Function could not be determined for function event with Id=" + functionEvent.getId()
                            + " and ProcessInstanceID=" + functionEvent.getProcessInstanceID());
        }
        return functionEvent.getFunctionObj();
    }

    private FuncEvent<T> executeStatefulFunction(FuncEvent<T> functionEvent, Func<T> function) {
        FuncEvent<T> result = function.work(functionEvent);
        if (result == null) {
            return this.createEndEvent(functionEvent);
        }
        return result;
    }

    protected FuncEvent<T> executeTransientFunction(FuncEvent<T> functionEvent, Func<T> function) {
        if (functionEvent != null) {
            LOGGER.trace("Execute transient function id={} functionId={} function={}", new Object[] {
                    functionEvent.getProcessInstanceID(), functionEvent.getId(), functionEvent.getFunction() });
        }
        String destTopic = null;
        byte[] originalEvent = null;
        try {
            destTopic = this.topicResolver.resolveTopicName(FuncEvent.Type.TRANSIENT);
            originalEvent = this.functionWorkflow.getFuncEventSerializer().serialize(destTopic, functionEvent);

            FuncEvent<T> result = function.work(functionEvent);

            if (result == null) {
                FuncEvent<T> endEvent = this.createEndEvent(functionEvent);
                this.functionWorkflow.sendEvent(destTopic, null, endEvent);
                return endEvent;
            }
            if (result.getType() == Type.TRANSIENT) {
                if (result.getNextRetryAt() != null) {
                    long millisToWait = (result.getNextRetryAt().toEpochSecond() * 1000) - System.currentTimeMillis();
                    if (millisToWait > 0) {
                        Thread.sleep(millisToWait);
                    }
                }
                IFunc nextFunction = this.getFunctionObj(result);
                result = this.executeTransientFunction(result, (Func<T>) nextFunction);
            }
            return result;
        } catch (Throwable e) {
            FuncEvent<T> nextFunction = FuncEvent.newEvent();
            nextFunction.setProcessName(functionEvent.getProcessName());
            nextFunction.setComingFromId(functionEvent.getId());
            nextFunction.setProcessInstanceID(functionEvent.getProcessInstanceID());
            nextFunction.setType(FuncEvent.Type.ERROR);
            nextFunction.setError(e);
            return nextFunction;
        } finally {
            if (destTopic == null) {
                destTopic = "TRANSIENT";
            }
            if (originalEvent != null) {
                this.functionWorkflow.sendEvent(destTopic, originalEvent);
            }
        }
    }
}
