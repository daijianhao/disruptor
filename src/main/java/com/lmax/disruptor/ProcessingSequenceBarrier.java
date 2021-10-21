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
 * {@link SequenceBarrier} handed out for gating {@link EventProcessor}s on a cursor sequence and optional dependent {@link EventProcessor}(s),
 * using the given WaitStrategy.
 */
final class ProcessingSequenceBarrier implements SequenceBarrier
{
    private final WaitStrategy waitStrategy;
    /**
     * SequenceBarrier主要是设置消费依赖的。
     * 比如某个消费者必须等它依赖的消费者消费完某个消息之后才可以消费该消息。
     * 当然此处是从Disruptor上直接创建消费组，sequencesToTrack都为空数组，
     * 所以只依赖于RingBuffer上的cursorSequence（也就是只要RingBuffer上写(publish)到哪了，
     * 那么我就能消费到哪）
     *
     * dependentSequence 用来记录当前消费者所依赖的其他消费者的Sequence，所以叫dependent
     */
    private final Sequence dependentSequence;
    private volatile boolean alerted = false;
    private final Sequence cursorSequence;
    private final Sequencer sequencer;

    ProcessingSequenceBarrier(
        final Sequencer sequencer,
        final WaitStrategy waitStrategy,
        final Sequence cursorSequence,
        final Sequence[] dependentSequences)
    {
        this.sequencer = sequencer;
        this.waitStrategy = waitStrategy;
        this.cursorSequence = cursorSequence;
        if (0 == dependentSequences.length)
        {
            /*
             0 == dependentSequences.length ，说明当前消费者不依赖其他消费者，
             而是直接从ringBuffer消费，即为 cursorSequence
             */
            dependentSequence = cursorSequence;
        }
        else
        {
            /*
             * 当存在依赖的其他消费者时，就将多个消费者 包装为一个，获取序号时取最小的
             */
            dependentSequence = new FixedSequenceGroup(dependentSequences);
        }
    }

    @Override
    public long waitFor(final long sequence)
        throws AlertException, InterruptedException, TimeoutException
    {
        checkAlert();
        // waitStrategy派上用场了，这是我们在构造Disruptor的时候的入参（也是构造RingBuffer的入参）
        long availableSequence = waitStrategy.waitFor(sequence, cursorSequence, dependentSequence, this);
        // 理论上没有可能为true，因为当前每种waitStrategy内都保证了availableSequence一定大于等于sequence
        if (availableSequence < sequence)
        {
            return availableSequence;
        }
        // 返回最大的已发布的sequence，在单生产者模式下这个函数返回值就等于availableSequence
        return sequencer.getHighestPublishedSequence(sequence, availableSequence);
    }

    @Override
    public long getCursor()
    {
        return dependentSequence.get();
    }

    @Override
    public boolean isAlerted()
    {
        return alerted;
    }

    @Override
    public void alert()
    {
        alerted = true;
        waitStrategy.signalAllWhenBlocking();
    }

    @Override
    public void clearAlert()
    {
        alerted = false;
    }

    @Override
    public void checkAlert() throws AlertException
    {
        if (alerted)
        {
            throw AlertException.INSTANCE;
        }
    }
}