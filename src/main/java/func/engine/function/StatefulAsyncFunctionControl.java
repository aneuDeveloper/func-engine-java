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

import func.engine.FunctionsWorkflow;
import func.engine.correlation.CorrelationState;

public class StatefulAsyncFunctionControl<T> extends StatefulFunctionControl<T>
        implements StatefulAsyncFunction.WorkflowControlAsync<T> {
    public StatefulAsyncFunctionControl(FunctionEvent functionEvent, FunctionsWorkflow<T> functionsWorkflow) {
        super(functionEvent, functionsWorkflow);
    }

    @Override
    public FunctionEvent createCorrelation(String correlationId, long asyncTimeout) {
        if (correlationId == null) {
            throw new IllegalStateException("CorrelationId must be specified.");
        }
        FunctionEvent correlation = FunctionEventUtil.createWithDefaultValues();
        correlation.setProcessName(this.sourceFunctionEvent.getProcessName());
        correlation.setProcessInstanceID(this.sourceFunctionEvent.getProcessInstanceID());
        correlation.setType(FunctionEvent.Type.CORRELATION);
        correlation.setCorrelationState(CorrelationState.INITIALIZED);
        correlation.setCorrelationId(correlationId);
        correlation.setFunction(this.sourceFunctionEvent.getFunction());
        correlation.setComingFromId(this.sourceFunctionEvent.getId());
        String dataAsString = this.workflow.getDataSerDes().serialize(this.data);
        correlation.setData(dataAsString);
        return correlation;
    }

    @Override
    public FunctionEvent getSource() {
        return this.sourceFunctionEvent;
    }
}
