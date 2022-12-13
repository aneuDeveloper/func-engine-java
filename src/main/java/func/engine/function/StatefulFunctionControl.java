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

import func.engine.FunctionsWorkflow;
import func.engine.PeriodUtil;
import func.engine.Retries;

public class StatefulFunctionControl<T> implements StatefulFunction.WorkflowControl<T> {
    protected FunctionEvent sourceFunctionEvent;
    protected FunctionsWorkflow<T> workflow;

    public StatefulFunctionControl(FunctionEvent sourceFunctionEvent, FunctionsWorkflow<T> processDefinition) {
        this.sourceFunctionEvent = sourceFunctionEvent;
        this.workflow = processDefinition;
    }

    @Override
    public T getData() {
        return this.sourceFunctionEvent.getFunctionData();
    }

    @Override
    public FunctionEvent nextFunction(Function function) {
        FunctionEvent nextFunction = FunctionEventUtil.createWithDefaultValues();
        nextFunction.setProcessName(this.sourceFunctionEvent.getProcessName());
        nextFunction.setComingFromId(this.sourceFunctionEvent.getId());
        nextFunction.setProcessInstanceID(this.sourceFunctionEvent.getProcessInstanceID());
        nextFunction.setRetryCount(0);
        nextFunction.setType(FunctionEvent.Type.WORKFLOW);
        this.workflow.getProcessEventUtil().setFunction(nextFunction, function);
        nextFunction.setFunctionData(sourceFunctionEvent.getFunctionData());
        return nextFunction;
    }

    @Override
    public FunctionEvent retry(Retries... retries) {
        int executedRetriesInThePast = this.sourceFunctionEvent.getRetryCount();
        ZonedDateTime nextRetryAt = PeriodUtil.getNextRetryAt(executedRetriesInThePast, ZonedDateTime.now(), retries);
        if (nextRetryAt == null) {
            return null;
        }
        FunctionEvent retry = FunctionEventUtil.createWithDefaultValues();
        retry.setProcessName(this.sourceFunctionEvent.getProcessName());
        retry.setComingFromId(this.sourceFunctionEvent.getId());
        retry.setType(FunctionEvent.Type.RETRY);
        retry.setNextRetryAt(nextRetryAt);
        retry.setRetryCount(this.sourceFunctionEvent.getRetryCount() + 1);
        retry.setFunction(this.sourceFunctionEvent.getFunction());
        retry.setProcessInstanceID(this.sourceFunctionEvent.getProcessInstanceID());
        retry.setFunctionData(this.sourceFunctionEvent.getFunctionData());
        return retry;
    }

    @Override
    public FunctionEvent endWorkflow() {
        return null;
    }

    @Override
    public String getProcessInstanceId() {
        return this.sourceFunctionEvent.getProcessInstanceID();
    }

    @Override
    public FunctionEvent getSource() {
        return this.sourceFunctionEvent;
    }
}
