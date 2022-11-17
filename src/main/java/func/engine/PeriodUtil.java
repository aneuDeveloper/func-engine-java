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

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

public class PeriodUtil {
    public static ZonedDateTime getNextRetryAt(int executedRetries, ZonedDateTime currentTime, Retries ... retriesArray) {
        Retries choosenRetryUnitWithNumber = null;
        if (retriesArray == null) {
            if (3 <= executedRetries) {
                return null;
            }
            choosenRetryUnitWithNumber = Retries.build().retryTimes(3).in(5, ChronoUnit.MINUTES);
        } else {
            int maxPossibleRetries = 0;
            for (Retries retries : retriesArray) {
                if ((maxPossibleRetries += retries.getRetryTimes()) <= executedRetries || maxPossibleRetries <= executedRetries) continue;
                choosenRetryUnitWithNumber = retries;
                break;
            }
        }
        if (choosenRetryUnitWithNumber == null) {
            return null;
        }
        long jdf = choosenRetryUnitWithNumber.getTime();
        ZonedDateTime returnVal = currentTime.plus(jdf, choosenRetryUnitWithNumber.getTimeUnit());
        return returnVal;
    }
}

