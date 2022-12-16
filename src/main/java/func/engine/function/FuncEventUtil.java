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
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FuncEventUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(FuncEventUtil.class);
    private FuncSerDes functionSerDes;

    public FuncEventUtil(FuncSerDes functionSerDes) {
        this.functionSerDes = functionSerDes;
    }

    public static <T> FuncEvent<T> createWithDefaultValues() {
        FuncEvent<T> functionEvent = new FuncEvent<T>("1", UUID.randomUUID().toString());
        functionEvent.setTimeStamp(ZonedDateTime.now());
        return functionEvent;
    }

    public void setFunction(FuncEvent functionEvent, IFunc function) {
        functionEvent.setFunctionObj(function);
        String functionName = this.functionSerDes.serialize(function);
        functionEvent.setFunction(functionName);
    }

    public void configureFunctionByFunctionName(FuncEvent functionEvent) {
        IFunc functionObj = this.functionSerDes.deserialize(functionEvent);
        functionEvent.setFunctionObj(functionObj);
    }

    public IFunc getFunctionObj(FuncEvent functionEvent) {
        if (functionEvent.getFunctionObj() != null) {
            return functionEvent.getFunctionObj();
        }
        if (functionEvent.getFunction() != null) {
            IFunc functionObj = this.functionSerDes.deserialize(functionEvent);
            functionEvent.setFunctionObj(functionObj);
            this.setFunction(functionEvent, functionObj);
            return functionEvent.getFunctionObj();
        }
        if (functionEvent.getFunctionObj() == null) {
            LOGGER.error("Function object has not been defined and could not be deserialized for String represention.");
        }
        return functionEvent.getFunctionObj();
    }
}

