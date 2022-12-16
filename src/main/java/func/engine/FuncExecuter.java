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
package func.engine;

import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import func.engine.correlation.CorrelationState;
import func.engine.function.IFunc;
import func.engine.function.FuncEvent;
import func.engine.function.FuncEvent.Type;
import func.engine.function.FuncEventUtil;
import func.engine.function.FuncAsync;
import func.engine.function.Func;

public class FuncExecuter<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FuncExecuter.class);
    private FuncWorkflow<T> functionWorkflow;
    private TopicResolver topicResolver;

    public FuncExecuter(FuncWorkflow<T> functionWorkflow) {
        this.functionWorkflow = functionWorkflow;
        this.topicResolver = this.functionWorkflow.getTopicResolver();
    }

    public FuncEvent<T> executeMessageAndDiscoverNextStep(FuncEvent<T> functionEvent) {
        try {
            FuncEvent<T> nextFunctionEvent = this.executeMessage(functionEvent);
            return nextFunctionEvent;
        } catch (Throwable e) {
            FuncEvent<T> nextFunction = FuncEventUtil.createWithDefaultValues();
            nextFunction.setProcessName(functionEvent.getProcessName());
            nextFunction.setComingFromId(functionEvent.getId());
            nextFunction.setProcessInstanceID(functionEvent.getProcessInstanceID());
            nextFunction.setType(FuncEvent.Type.ERROR);
            nextFunction.setError(e);
            return nextFunction;
        }
    }

    private FuncEvent<T> createEndEvent(FuncEvent<T> functionEvent) {
        FuncEvent<T> endEvent = FuncEventUtil.createWithDefaultValues();
        endEvent.setProcessName(functionEvent.getProcessName());
        endEvent.setType(FuncEvent.Type.END);
        endEvent.setProcessInstanceID(functionEvent.getProcessInstanceID());
        endEvent.setComingFromId(functionEvent.getId());
        return endEvent;
    }

    protected FuncEvent<T> executeMessage(FuncEvent<T> functionEvent)
            throws InterruptedException, ExecutionException {
        IFunc function = this.functionWorkflow.getProcessEventUtil().getFunctionObj(functionEvent);
        if (functionEvent.getType() == FuncEvent.Type.TRANSIENT) {
            LOGGER.trace("Execute transient function id={} functionId={} function={}", new Object[] {
                    functionEvent.getProcessInstanceID(), functionEvent.getId(), functionEvent.getFunction() });
            return this.executeTransientFunction(functionEvent, (Func<T>) function);
        }
        if (FuncAsync.class.isAssignableFrom(function.getClass())) {
            LOGGER.trace("Execute async function id={} functionId={} function={}", new Object[] {
                    functionEvent.getProcessInstanceID(), functionEvent.getId(), functionEvent.getFunction() });
            return this.executeStatefulAsyncFunction(functionEvent, (FuncAsync<T>) function);
        }
        LOGGER.trace("Execute stateful function id={} functionId={} function={}", new Object[] {
                functionEvent.getProcessInstanceID(), functionEvent.getId(), functionEvent.getFunction() });
        return this.executeStatefulFunction(functionEvent, (Func<T>) function);
    }

    private FuncEvent<T> executeStatefulFunction(FuncEvent<T> functionEvent, Func<T> function) {
        FuncEvent<T> result = function.work(functionEvent);
        if (result == null) {
            return this.createEndEvent(functionEvent);
        }
        return result;
    }

    protected FuncEvent<T> executeTransientFunction(FuncEvent<T> functionEvent, Func<T> function)
            throws InterruptedException, ExecutionException {
        FuncEvent<T> result = function.work(functionEvent);
        this.functionWorkflow.sendEvent(this.topicResolver.resolveTopicName(FuncEvent.Type.TRANSIENT), null,
                functionEvent);
        if (result == null) {
            FuncEvent<T> endEvent = this.createEndEvent(functionEvent);
            this.functionWorkflow.sendEvent(this.topicResolver.resolveTopicName(FuncEvent.Type.TRANSIENT), null,
                    endEvent);
            return endEvent;
        }
        if (result.getType() == Type.TRANSIENT) {
            result = this.executeMessage(result);
        } else if (result.getType() == Type.RETRY) {
            // wait in case of transient for retry
        }
        return result;
    }

    private FuncEvent<T> executeStatefulAsyncFunction(FuncEvent<T> functionEvent, FuncAsync<T> function)
            throws InterruptedException, ExecutionException {
        if (functionEvent.getCorrelationState() == null) {
            FuncEvent<T> correlationEvent = function.start(functionEvent);
            if (correlationEvent == null) {
                throw new IllegalStateException(
                        String.format("No correlation specified for processInstanceId=%s and functionId=%s",
                                functionEvent.getProcessInstanceID(), functionEvent.getId()));
            }
            return correlationEvent;
        }
        if (functionEvent.getCorrelationState() == CorrelationState.CALLBACK_FORWARDED) {
            FuncEvent<T> result = function.continueFunction(functionEvent);
            if (result == null) {
                return this.createEndEvent(functionEvent);
            }
            return result;
        }
        throw new IllegalStateException("Could not execute async step.");
    }
}
