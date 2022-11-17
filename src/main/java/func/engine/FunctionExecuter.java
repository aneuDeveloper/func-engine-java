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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import func.engine.correlation.CorrelationState;
import func.engine.function.Function;
import func.engine.function.FunctionEvent;
import func.engine.function.FunctionEventUtil;
import func.engine.function.StatefulAsyncFunction;
import func.engine.function.StatefulAsyncFunctionControl;
import func.engine.function.StatefulFunction;
import func.engine.function.StatefulFunctionControl;
import func.engine.function.TransientFunction;
import func.engine.function.TransientFunctionControl;

public class FunctionExecuter<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionExecuter.class);
    private FunctionsWorkflow<T> functionWorkflow;
    private TopicResolver topicResolver;

    public FunctionExecuter(FunctionsWorkflow<T> functionWorkflow) {
        this.functionWorkflow = functionWorkflow;
        this.topicResolver = this.functionWorkflow.getTopicResolver();
    }

    public FunctionEvent executeMessageAndDiscoverNextStep(FunctionEvent functionEvent) {
        try {
            FunctionEvent nextFunctionEvent = this.executeMessage(functionEvent);
            return nextFunctionEvent;
        } catch (Throwable e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            functionEvent.setError(sw.toString());
            functionEvent.setException(e);
            Function function = this.functionWorkflow.getProcessEventUtil().getFunctionObj(functionEvent);
            if (TransientFunction.class.isAssignableFrom(function.getClass())) {
                this.functionWorkflow.sendEvent(this.topicResolver.resolveTopicName(FunctionEvent.Type.TRANSIENT), null,
                        functionEvent);
            }
            return functionEvent;
        }
    }

    private FunctionEvent createEndEvent(FunctionEvent functionEvent, T data) {
        FunctionEvent endEvent = FunctionEventUtil.createWithDefaultValues();
        endEvent.setProcessName(functionEvent.getProcessName());
        endEvent.setType(FunctionEvent.Type.END);
        endEvent.setProcessInstanceID(functionEvent.getProcessInstanceID());
        endEvent.setComingFromId(functionEvent.getId());
        endEvent.setData(this.functionWorkflow.getDataSerDes().serialize(data));
        return endEvent;
    }

    private FunctionEvent executeMessage(FunctionEvent functionEvent) throws InterruptedException, ExecutionException {
        Function function = this.functionWorkflow.getProcessEventUtil().getFunctionObj(functionEvent);
        if (functionEvent.getType() == null || functionEvent.getType() == FunctionEvent.Type.WORKFLOW) {
            if (StatefulFunction.class.isAssignableFrom(function.getClass())) {
                LOGGER.trace("Execute stateful function id={} functionId={} function={}", new Object[] {
                        functionEvent.getProcessInstanceID(), functionEvent.getId(), functionEvent.getFunction() });
                return this.executeStatefulFunction(functionEvent, (StatefulFunction<T>) function);
            }
            if (TransientFunction.class.isAssignableFrom(function.getClass())) {
                LOGGER.trace("Execute transient function id={} functionId={} function={}", new Object[] {
                        functionEvent.getProcessInstanceID(), functionEvent.getId(), functionEvent.getFunction() });
                return this.executeTransientFunction(functionEvent, (TransientFunction<T>) function);
            }
            if (StatefulAsyncFunction.class.isAssignableFrom(function.getClass())) {
                LOGGER.trace("Execute async function id={} functionId={} function={}", new Object[] {
                        functionEvent.getProcessInstanceID(), functionEvent.getId(), functionEvent.getFunction() });
                return this.executeStatefulAsyncFunction(functionEvent, (StatefulAsyncFunction<T>) function);
            }
        }
        throw new IllegalStateException(String.format("Could not execute function id=%s functionId=%s function=%s.",
                functionEvent.getProcessInstanceID(), functionEvent.getId(), functionEvent.getFunction()));
    }

    private FunctionEvent executeStatefulFunction(FunctionEvent functionEvent, StatefulFunction<T> function) {
        StatefulFunctionControl<T> processControl = new StatefulFunctionControl<T>(functionEvent,
                this.functionWorkflow);
        FunctionEvent result = function.work(processControl);
        if (result == null) {
            return this.createEndEvent(functionEvent, processControl.getData());
        }
        return result;
    }

    private FunctionEvent executeTransientFunction(FunctionEvent functionEvent, TransientFunction<T> function)
            throws InterruptedException, ExecutionException {
        TransientFunctionControl<T> processControl = new TransientFunctionControl<T>(functionEvent,
                this.functionWorkflow);
        this.functionWorkflow.sendEvent(this.topicResolver.resolveTopicName(FunctionEvent.Type.TRANSIENT), null,
                functionEvent);
        FunctionEvent result = function.work(processControl);
        if (result == null) {
            FunctionEvent endEvent = this.createEndEvent(functionEvent, processControl.getData());
            this.functionWorkflow.sendEvent(this.topicResolver.resolveTopicName(FunctionEvent.Type.TRANSIENT), null,
                    endEvent);
            return endEvent;
        }
        if (result.getType() == FunctionEvent.Type.WORKFLOW && TransientFunction.class
                .isAssignableFrom(this.functionWorkflow.getProcessEventUtil().getFunctionObj(result).getClass())) {
            result = this.executeMessage(result);
        } else {
            this.functionWorkflow.sendEvent(result);
        }
        return result;
    }

    private FunctionEvent executeStatefulAsyncFunction(FunctionEvent functionEvent, StatefulAsyncFunction<T> function)
            throws InterruptedException, ExecutionException {
        if (functionEvent.getCorrelationState() == null) {
            StatefulAsyncFunctionControl<T> functionControl = new StatefulAsyncFunctionControl<T>(functionEvent,
                    this.functionWorkflow);
            FunctionEvent correlationEvent = function.start(functionControl);
            if (correlationEvent == null) {
                throw new IllegalStateException(
                        String.format("No correlation specified for processInstanceId=%s and functionId=%s",
                                functionEvent.getProcessInstanceID(), functionEvent.getId()));
            }
            return correlationEvent;
        }
        if (functionEvent.getCorrelationState() == CorrelationState.CALLBACK_FORWARDED) {
            StatefulFunctionControl<T> processControl = new StatefulFunctionControl<T>(functionEvent,
                    this.functionWorkflow);
            FunctionEvent result = function.continueFunction(processControl);
            if (result == null) {
                return this.createEndEvent(functionEvent, processControl.getData());
            }
            return result;
        }
        throw new IllegalStateException("Could not execute async step.");
    }
}
