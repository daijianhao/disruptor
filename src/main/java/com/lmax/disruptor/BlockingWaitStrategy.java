/*
 * Copyright 2011 LMAX Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lmax.disruptor;

/**
 * Blocking strategy that uses a lock and condition variable for {@link EventProcessor}s waiting on a barrier.
 *
 *
 * <p>This strategy can be used when throughput and low-latency are not as important as CPU resource.
 */
public final class BlockingWaitStrategy implements WaitStrategy
{
    private final Object mutex = new Object();

    /**
     * WaitStrategy.waitFor()获取的是依赖消费者的消费进度sequence（默认依赖RingBuffer上已申请进度的sequence）。
     * 需要注意的一点是，当消费者获取可消费事件的过程中，存在两种场景需要等待：
     *
     * RingBuffer上没有事件可以消费
     * RingBuffer上有可消费事件，但是依赖的消费者还未消费完该事件
     * 如果是第一种场景，那么消费者会采用WaitStrategy的策略进行等待。
     * 而如果是第二种场景的话，只能如上所示一样进入死循环（此时可能造成cpu升高）
     *
     *
     * @param sequence          消费者想要消费的最小sequence（底线）
     * @param cursorSequence    Sequencer的cursor，也就是当前RingBuffer上已经被申请的最大sequence（在讲生产者逻辑的时候提到了）
     * @param dependentSequence 在我们当前链路为cursorSequence，不存在消费依赖（如果存在依赖的话，则为依赖消费者消费进度）
     * @param barrier           这个主要是用了其中一些中断方法，不用太care
     * @return
     * @throws AlertException
     * @throws InterruptedException
     */
    @Override
    public long waitFor(final long sequence, final Sequence cursorSequence, final Sequence dependentSequence, final SequenceBarrier barrier)
            throws AlertException, InterruptedException
    {
        long availableSequence;
        //当前可读取的位置 小于 需要的位置，则进入等待
        if (cursorSequence.get() < sequence)
        {
            synchronized (mutex)
            {
                while (cursorSequence.get() < sequence)
                {
                    barrier.checkAlert();
                    mutex.wait();
                }
            }
        }
        // 看到了吧，这里已经保证了availableSequence必然大于等于sequence
        // 并且在存在依赖的场景中，被依赖消费者存在慢消费的话，会直接导致下游进入死循环
        while ((availableSequence = dependentSequence.get()) < sequence)
        {
            barrier.checkAlert();
            Thread.onSpinWait();
        }
        //返回可用的位置
        return availableSequence;
    }

    @Override
    public void signalAllWhenBlocking()
    {
        synchronized (mutex)
        {
            mutex.notifyAll();
        }
    }

    @Override
    public String toString()
    {
        return "BlockingWaitStrategy{" +
                "mutex=" + mutex +
                '}';
    }
}
