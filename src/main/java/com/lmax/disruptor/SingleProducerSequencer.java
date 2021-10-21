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

import com.lmax.disruptor.util.Util;

import java.util.Arrays;
import java.util.concurrent.locks.LockSupport;

abstract class SingleProducerSequencerPad extends AbstractSequencer
{
    protected byte
            p10, p11, p12, p13, p14, p15, p16, p17,
            p20, p21, p22, p23, p24, p25, p26, p27,
            p30, p31, p32, p33, p34, p35, p36, p37,
            p40, p41, p42, p43, p44, p45, p46, p47,
            p50, p51, p52, p53, p54, p55, p56, p57,
            p60, p61, p62, p63, p64, p65, p66, p67,
            p70, p71, p72, p73, p74, p75, p76, p77;

    SingleProducerSequencerPad(final int bufferSize, final WaitStrategy waitStrategy)
    {
        super(bufferSize, waitStrategy);
    }
}

abstract class SingleProducerSequencerFields extends SingleProducerSequencerPad
{
    SingleProducerSequencerFields(final int bufferSize, final WaitStrategy waitStrategy)
    {
        super(bufferSize, waitStrategy);
    }

    /**
     * Set to -1 as sequence starting point
     */
    long nextValue = Sequence.INITIAL_VALUE;
    long cachedValue = Sequence.INITIAL_VALUE;
}

/**
 * Coordinator for claiming sequences for access to a data structure while tracking dependent {@link Sequence}s.
 * Not safe for use from multiple threads as it does not implement any barriers.
 *
 * <p>* Note on {@link Sequencer#getCursor()}:  With this sequencer the cursor value is updated after the call
 * to {@link Sequencer#publish(long)} is made.
 * <p>
 * 单生产者Sequencer
 */
public final class SingleProducerSequencer extends SingleProducerSequencerFields
{
    protected byte
            p10, p11, p12, p13, p14, p15, p16, p17,
            p20, p21, p22, p23, p24, p25, p26, p27,
            p30, p31, p32, p33, p34, p35, p36, p37,
            p40, p41, p42, p43, p44, p45, p46, p47,
            p50, p51, p52, p53, p54, p55, p56, p57,
            p60, p61, p62, p63, p64, p65, p66, p67,
            p70, p71, p72, p73, p74, p75, p76, p77;

    /**
     * Construct a Sequencer with the selected wait strategy and buffer size.
     *
     * @param bufferSize   the size of the buffer that this will sequence over.
     * @param waitStrategy for those waiting on sequences.
     */
    public SingleProducerSequencer(final int bufferSize, final WaitStrategy waitStrategy)
    {
        super(bufferSize, waitStrategy);
    }

    /**
     * @see Sequencer#hasAvailableCapacity(int)
     */
    @Override
    public boolean hasAvailableCapacity(final int requiredCapacity)
    {
        return hasAvailableCapacity(requiredCapacity, false);
    }

    private boolean hasAvailableCapacity(final int requiredCapacity, final boolean doStore)
    {
        long nextValue = this.nextValue;

        long wrapPoint = (nextValue + requiredCapacity) - bufferSize;
        long cachedGatingSequence = this.cachedValue;

        if (wrapPoint > cachedGatingSequence || cachedGatingSequence > nextValue)
        {
            if (doStore)
            {
                cursor.setVolatile(nextValue);  // StoreLoad fence
            }

            long minSequence = Util.getMinimumSequence(gatingSequences, nextValue);
            this.cachedValue = minSequence;

            if (wrapPoint > minSequence)
            {
                return false;
            }
        }

        return true;
    }

    /**
     * @see Sequencer#next()
     */
    @Override
    public long next()
    {
        return next(1);
    }

    /**
     * 获取下n个可使用的位置
     *
     * @see Sequencer#next(int)
     */
    @Override
    public long next(final int n)
    {
        if (n < 1 || n > bufferSize)
        {
            //参数异常
            throw new IllegalArgumentException("n must be > 0 and < bufferSize");
        }
        // nextValue这个变量名有点诡异，实际上表示已经申请到的那个sequence
        long nextValue = this.nextValue;
        // nextSequence表示本次需要申请的最大sequence
        long nextSequence = nextValue + n;
        // 计算出nextSequence在上一圈的点位
        long wrapPoint = nextSequence - bufferSize;
        // 最慢消费进度的缓存
        long cachedGatingSequence = this.cachedValue;
        // 下面这个条件表达式以及其代码块解释起来可能需要比较大的篇幅，所以在下面[核心代码详解]里说明
        /*
         * wrapPoint > cachedGatingSequence:
         *      结合我们对Disruptor的了解，此处判断的应该是此次要申请的sequence是否已经领先最慢消费进度一圈了（类似1000米跑步的套圈）
         *      对于第一个条件表达式，也有个值得注意的地方：因为里面的【最慢消费进度】取的是缓存值（cached）。
         *      而这个缓存值是什么时候更新的呢？答案是只有在“套圈”了以后才会更新。这个逻辑你品，你细品，那么你会发现，
         *      每申请RingBuffer.size()个sequence之后都会走进上面的“套圈”逻辑来更新cachedGatingSequence。
         *      这样就极大的减少了Util.getMinimumSequence(gatingSequences, nextValue)的运算量
         *
         *
         * cachedGatingSequence > nextValue:
         *      判断的是最慢消费进度超过了我们即将要申请的sequence，乍一看这应该是不可能的吧，都还没申请到该sequence怎么可能消费到呢？
         *      找了些资料，发现确实是存在该场景的：RingBuffer提供了一个叫resetTo的方法，
         *      可以重置当前已申请sequence为一个指定值并publish出去：
         *          https://github.com/LMAX-Exchange/disruptor/issues/280
         *          https://github.com/LMAX-Exchange/disruptor/issues/76
         *
         *      不过该代码已经标注为@Deprecated，按照作者的意思，后续是要删掉的。那么在此处分析的时候，我们就将当它恒为false。
         *
         * */
        if (wrapPoint > cachedGatingSequence || cachedGatingSequence > nextValue)
        {
            //插入一个StoreLoad屏障，防止是因为内存可见性导致的消费者消费不了数据（应该极少存在这样的情况吧）
            cursor.setVolatile(nextValue);  // StoreLoad fence

            long minSequence;
            //实时计算一下最慢消费进度Util.getMinimumSequence(gatingSequences, nextValue)
            //如果真的套圈了，那么就一直死循环直到RingBuffer上有空间可以申请
            while (wrapPoint > (minSequence = Util.getMinimumSequence(gatingSequences, nextValue)))
            {
                // 生产者如果没有空间写数据了，只能无限park
                LockSupport.parkNanos(1L); // TODO: Use waitStrategy to spin?
            }
            //更新【最慢消费进度缓存】
            this.cachedValue = minSequence;
        }

        this.nextValue = nextSequence;

        return nextSequence;
    }

    /**
     * @see Sequencer#tryNext()
     */
    @Override
    public long tryNext() throws InsufficientCapacityException
    {
        return tryNext(1);
    }

    /**
     * @see Sequencer#tryNext(int)
     */
    @Override
    public long tryNext(final int n) throws InsufficientCapacityException
    {
        if (n < 1)
        {
            throw new IllegalArgumentException("n must be > 0");
        }

        if (!hasAvailableCapacity(n, true))
        {
            throw InsufficientCapacityException.INSTANCE;
        }

        long nextSequence = this.nextValue += n;

        return nextSequence;
    }

    /**
     * @see Sequencer#remainingCapacity()
     */
    @Override
    public long remainingCapacity()
    {
        long nextValue = this.nextValue;

        long consumed = Util.getMinimumSequence(gatingSequences, nextValue);
        long produced = nextValue;
        return getBufferSize() - (produced - consumed);
    }

    /**
     * @see Sequencer#claim(long)
     */
    @Override
    public void claim(final long sequence)
    {
        this.nextValue = sequence;
    }

    /**
     * @see Sequencer#publish(long)
     */
    @Override
    public void publish(final long sequence)
    {
        /*
         *这里会把cursor的值设置为当前申请的sequence，代表序号为sequence的事件发布成功。
         * 这里的cursor表示已经publish的最大事件序号（在多生产者模式中并不是），
         * 所以我们在使用过程中需要依次申请，依次发布，不能直接上来就publish(100)，
         * 这样会导致消费者会认为100以前的序号也都就绪了。另外，由于我们现在看的是单生产者模式，也不需要考虑并发场景
         */
        cursor.set(sequence);
        //通知阻塞的消费者进行消费
        waitStrategy.signalAllWhenBlocking();
    }

    /**
     * @see Sequencer#publish(long, long)
     */
    @Override
    public void publish(final long lo, final long hi)
    {
        publish(hi);
    }

    /**
     * @see Sequencer#isAvailable(long)
     */
    @Override
    public boolean isAvailable(final long sequence)
    {
        final long currentSequence = cursor.get();
        return sequence <= currentSequence && sequence > currentSequence - bufferSize;
    }

    @Override
    public long getHighestPublishedSequence(final long lowerBound, final long availableSequence)
    {
        return availableSequence;
    }

    @Override
    public String toString()
    {
        return "SingleProducerSequencer{" +
                "bufferSize=" + bufferSize +
                ", waitStrategy=" + waitStrategy +
                ", cursor=" + cursor +
                ", gatingSequences=" + Arrays.toString(gatingSequences) +
                '}';
    }
}
