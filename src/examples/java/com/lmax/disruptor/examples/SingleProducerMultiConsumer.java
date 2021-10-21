package com.lmax.disruptor.examples;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.locks.LockSupport;

public class SingleProducerMultiConsumer
{

    static class LongEvent
    {
        long val;
    }

    static class MyHandler implements EventHandler<LongEvent>
    {

        private long handlerCount = 0;

        private long num;

        private long numOfConsumer;

        public MyHandler(final int num, final int numOfConsumer)
        {
            this.num = num;
            this.numOfConsumer = numOfConsumer;
        }

        @Override
        public void onEvent(final LongEvent event, final long sequence, final boolean endOfBatch) throws Exception
        {
            if ((sequence % numOfConsumer) == num)
            {
                System.out.println(event.val);
                handlerCount++;
            }
        }
    }

    public static void main(String[] args)
    {
        long disruptor = testDisruptor();
        long testExecutor = testExecutor();
        System.out.println("disruptor time:" + disruptor);
        System.out.println("executor time:" + testExecutor);
    }

    private static long testExecutor()
    {
        ArrayBlockingQueue<LongEvent> queue = new ArrayBlockingQueue<>(1024);

        Thread[] threads = new Thread[4];
        for (int i = 0; i < 4; i++)
        {
            threads[i] = new Thread(() ->
            {

                try
                {
                    while (true)
                    {
                        LongEvent peek = queue.take();
                        System.out.println(peek.val);
                    }
                }
                catch (InterruptedException e)
                {
                }
            });
            threads[i].start();
        }

        long start = System.nanoTime();
        for (int i = 0; i < 2000000; i++)
        {
            LongEvent longEvent = new LongEvent();
            longEvent.val = i;
            try
            {
                queue.put(longEvent);
            }
            catch (InterruptedException e)
            {
            }
        }
        while (!queue.isEmpty())
        {
            LockSupport.parkNanos(1);
        }
        long end = System.nanoTime();
        for (int i = 0; i < 4; i++)
        {
            threads[i].interrupt();
        }
        return (end - start);
    }

    private static long testDisruptor()
    {
        Disruptor<LongEvent> disruptor
                = new Disruptor<>(LongEvent::new, 1024,
                DaemonThreadFactory.INSTANCE, ProducerType.SINGLE, new BlockingWaitStrategy());

        MyHandler handler0 = new MyHandler(0, 4);
        MyHandler handler1 = new MyHandler(1, 4);
        MyHandler handler2 = new MyHandler(2, 4);
        MyHandler handler3 = new MyHandler(3, 4);
        disruptor.handleEventsWith(handler0,
                handler1,
                handler2,
                handler3);

        long start = System.nanoTime();
        disruptor.start();
        for (int i = 0; i < 2000000; i++)
        {
            int j = i;
            disruptor.publishEvent((event, sequence) -> event.val = j);
        }

        disruptor.shutdown();
        long end = System.nanoTime();
        return (end - start);
    }
}
