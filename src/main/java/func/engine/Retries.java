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

import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;

public class Retries {
    private int retryTimes = 1;
    private int time = 5;
    private TemporalUnit timeUnit = ChronoUnit.MINUTES;

    private Retries() {
    }

    public Retries(int retryTimes, int time, TemporalUnit timeUnit) {
        this.retryTimes = retryTimes;
        this.time = time;
        this.timeUnit = timeUnit;
    }

    public static Retries build() {
        return new Retries();
    }

    public int getRetryTimes() {
        return this.retryTimes;
    }

    public Retries retryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
        return this;
    }

    public int getTime() {
        return this.time;
    }

    public Retries in(int time, TemporalUnit timeUnit) {
        this.time = time;
        this.timeUnit = timeUnit;
        return this;
    }

    public TemporalUnit getTimeUnit() {
        return this.timeUnit;
    }
}

