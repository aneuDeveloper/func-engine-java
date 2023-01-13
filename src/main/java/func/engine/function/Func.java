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

import func.engine.PeriodUtil;
import func.engine.Retries;

public interface Func<T> extends IFunc {
    public FuncEvent<T> work(FuncEvent<T> functionEvent);

    default FuncEvent<T> nextFunction(FuncEvent<T> functionEvent, IFunc nextFunction) {
        FuncEvent<T> nextFunctionEvent = FuncEvent.createWithDefaultValues();
        nextFunctionEvent.setProcessName(functionEvent.getProcessName());
        nextFunctionEvent.setComingFromId(functionEvent.getId());
        nextFunctionEvent.setProcessInstanceID(functionEvent.getProcessInstanceID());
        nextFunctionEvent.setType(FuncEvent.Type.WORKFLOW);
        nextFunctionEvent.setContext(functionEvent.getContext());
        nextFunctionEvent.setFunctionObj(nextFunction);
        return nextFunctionEvent;
    }

    default FuncEvent<T> nextFunctionTransient(FuncEvent<T> functionEvent, IFunc function) {
        FuncEvent<T> nextFunction = FuncEvent.createWithDefaultValues();
        nextFunction.setProcessName(functionEvent.getProcessName());
        nextFunction.setComingFromId(functionEvent.getId());
        nextFunction.setProcessInstanceID(functionEvent.getProcessInstanceID());
        nextFunction.setType(FuncEvent.Type.TRANSIENT);
        nextFunction.setFunctionObj(function);
        nextFunction.setContext(functionEvent.getContext());
        return nextFunction;
    }

    default FuncEvent<T> retry(FuncEvent<T> functionEvent, Retries... retries) {
        int executedRetriesInThePast = functionEvent.getRetryCount();
        ZonedDateTime nextRetryAt = PeriodUtil.getNextRetryAt(executedRetriesInThePast, ZonedDateTime.now(), retries);
        if (nextRetryAt == null) {
            return null;
        }
        FuncEvent<T> retry = FuncEvent.createWithDefaultValues();
        retry.setProcessName(functionEvent.getProcessName());
        retry.setComingFromId(functionEvent.getId());
        retry.setType(FuncEvent.Type.RETRY);
        retry.setNextRetryAt(nextRetryAt);
        retry.setRetryCount(functionEvent.getRetryCount() + 1);
        retry.setFunction(functionEvent.getFunction());
        retry.setProcessInstanceID(functionEvent.getProcessInstanceID());
        retry.setContext(functionEvent.getContext());
        return retry;
    }
}
