package xyz.klausturbo.manageable.kafka.consumer;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.OptionalLong;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * 位移管理.
 *
 * @author <a href="mailto:guyue375@outlook.com">Klaus.turbo</a>
 * @program manageable-kafka-consumer
 **/
public class OffsetMonitor {
    
    private final int pageSize;
    
    /**
     * 一个分区最多分配page的个数
     */
    private final int maxOpenPagesPerPartition;
    
    private final Map<Integer/*partition id*/, PartitionMonitor> partitionMonitors;
    
    public OffsetMonitor(int pageSize, int maxOpenPagesPerPartition) {
        this.pageSize = pageSize;
        this.maxOpenPagesPerPartition = maxOpenPagesPerPartition;
        this.partitionMonitors = new HashMap<>();
    }
    
    public void reset() {
        partitionMonitors.clear();
    }
    
    public boolean track(int partition, long offset) {
        return partitionMonitors.computeIfAbsent(partition, key -> new PartitionMonitor(offset)).track(offset);
    }
    
    public OptionalLong ack(int partition, long offset) {
        PartitionMonitor partitionTracker = partitionMonitors.get(partition);
        if (partitionTracker == null) {
            return OptionalLong.empty();
        }
        return partitionTracker.ack(offset);
    }
    
    private class PartitionMonitor {
        
        private final Map<Long/*page index*/, PageMonitor> pageMonitors = new HashMap<>();
        
        /**
         * 已经完成的页面（即：被追踪，并且全部已经确认）利用treeset排序
         */
        private final SortedSet<Long /*page index*/> completedPages = new TreeSet<>();
        
        /**
         * 最后一个连续的页面下标
         */
        private long lastConsecutivePageIndex;
        
        /**
         * 最后一个打开的页面下标 打开的意思是追踪了offset，但是还没有被确认
         */
        private long lastOpenedPageIndex;
        
        /**
         * 最后一个被追踪的下标 offset
         */
        private long lastTrackedOffset;
        
        private volatile int openPagesSize = 0;
        
        private volatile int completedPagesSize = 0;
        
        public PartitionMonitor(long initialOffset /* begin offset of this partition */) {
            openNewPageForOffset(initialOffset);
            lastConsecutivePageIndex = offsetToPage(initialOffset) - 1;
            // 最后一个被追踪的位移下标
            lastTrackedOffset = initialOffset - 1;
        }
        
        private boolean openNewPageForOffset(long offset) {
            if ((pageMonitors.size() + completedPages.size()) >= maxOpenPagesPerPartition) {
                return false;
            }
            // offset 属于哪一个 page
            long pageIndex = offsetToPage(offset);
            // offset 在 page 的哪个位置
            int margin = offsetToPageOffset(offset);
            pageMonitors.put(pageIndex, new PageMonitor(pageSize, margin));
            openPagesSize = pageMonitors.size();
            lastOpenedPageIndex = pageIndex;
            return true;
        }
        
        boolean track(long offset) {
            // 获取这个下标应该在哪一个 分区page之中
            long pageIndex = offsetToPage(offset);
            // 正常情况，位移按顺序提交过来
            // 顺序消息
            if (offset - lastTrackedOffset == 1) {
                // pageindex 超过 最后新增的pageindex 并且 已经新增不了 page 给这个 offset了，则返回false
                if (pageIndex > lastOpenedPageIndex && !openNewPageForOffset(offset)) {
                    return false;
                }
                lastTrackedOffset = offset;
                return true;
            }
            // 重复的消息
            if (offset <= lastTrackedOffset) {
                return true;
            }
            // 我们看到了一个大于预期的位移，并且存在一个缺口。
            // 我们的反应是：我们填补了这个空白，并假设丢失的记录之前已经确认。我们已经处理包括一下所有的可能，间隙仅在最后一页，或者间隙包括新页。
            // 消息缺失
            final PageMonitor lastPage = pageMonitors.get(lastOpenedPageIndex);
            // 走到这了，说明 offset 在最后一个 page 并且，当前 offset - lastTrackedOffset > 1(即中间产生了空缺)
            if (pageIndex == lastOpenedPageIndex) {
                // 直接假设中间缺失的部分都已经处理确认过了
                lastPage.bulkAck(offsetToPageOffset(lastTrackedOffset + 1), offsetToPageOffset(offset));
                return true;
            }
            
            final long lastPageIndexBeforeGap = lastOpenedPageIndex;
            
            final long lastOffsetBeforeGap = lastTrackedOffset;
            
            if (!openNewPageForOffset(offset)) {
                return false;
            }
            if (lastPage != null) {
                boolean completed = lastPage.bulkAck(offsetToPageOffset(lastOffsetBeforeGap + 1), pageSize);
                if (completed) {
                    pageMonitors.remove(lastPageIndexBeforeGap);
                }
            }
            completedPages.clear();
            completedPagesSize = 0;
            lastConsecutivePageIndex = lastOpenedPageIndex - 1;
            lastTrackedOffset = offset;
            return true;
        }
        
        OptionalLong ack(long offset) {
            // Tell the corresponding page tracker that this offset is acked.
            long pageIndex = offsetToPage(offset);
            PageMonitor pageTracker = pageMonitors.get(pageIndex);
            if (pageTracker == null) {
                return OptionalLong.empty();
            }
            int pageOffset = offsetToPageOffset(offset);
            if (!pageTracker.ack(pageOffset)) {
                return OptionalLong.empty();
            }
            
            // If the page is completed (all offsets in the page is acked), add the pages to the
            // list of completed pages.
            pageMonitors.remove(pageIndex);
            openPagesSize = pageMonitors.size();
            if (pageIndex <= lastConsecutivePageIndex) {
                return OptionalLong.empty();
            }
            completedPages.add(pageIndex);
            completedPagesSize = completedPages.size();
            
            // See whether the completed pages, construct a consecutive chain.
            int numConsecutive = 0;
            Iterator<Long> iterator = completedPages.iterator();
            while (iterator.hasNext()) {
                long index = iterator.next();
                if (index != lastConsecutivePageIndex + 1) {
                    break;
                }
                numConsecutive++;
                lastConsecutivePageIndex = index;
            }
            
            // There is no consecutive completed pages. So there is no offset to report as
            // safe to commit.
            if (numConsecutive == 0) {
                return OptionalLong.empty();
            }
            
            // There are consecutive completed pages which are not reported.
            // Remove them and report the next offset for commit.
            iterator = completedPages.iterator();
            for (int i = 0; i < numConsecutive; i++) {
                iterator.next();
                iterator.remove();
            }
            completedPagesSize = completedPages.size();
            return OptionalLong.of(pageToFirstOffset(lastConsecutivePageIndex + 1));
        }
        
        private long pageToFirstOffset(long pageIndex) {
            return pageIndex * pageSize;
        }
        
        /**
         * 将指定的 offset，投放到指定的 page 中去 例如一个 partition 有 3 个 page 。 0        1         2 P0：[0 100]-[101 200]-[201 300] 则
         * offset=128  一定在下标为 1 的page中间，即属于 101-200 的区间内 所以 使用 offset/pagesize = 128/100 = 1 即落入 page 1
         *
         * @param offset
         * @return
         */
        private long offsetToPage(long offset) {
            return offset / pageSize;
        }
        
        /**
         * 一个位移在page中的位置，偏移是多少
         *
         * @param offset
         * @return
         */
        private int offsetToPageOffset(long offset) {
            return (int) (offset % pageSize);
        }
    }
    
    /**
     * .
     **/
    private class PageMonitor {
        
        /**
         * The begin of this page .
         **/
        private final int margin;
        
        /**
         * Offset data is kept here before the offset is been committed.
         **/
        private final BitSet bits;
        
        /**
         * The effective size of this page , and also means the capacity of the bitset .
         **/
        private final int effectiveSize;
        
        /**
         * the first position of offset that has not been acked.
         **/
        private int firstUnackedOffset;
        
        public PageMonitor(int size, int margin) {
            this.margin = margin;
            this.effectiveSize = size - margin;
            this.firstUnackedOffset = 0;
            this.bits = new BitSet(effectiveSize);
        }
        
        int getMargin() {
            return margin;
        }
        
        boolean ack(int offset) {
            if (offset < margin) {
                return false;
            }
            // Set the bit representing this offset.
            int effectiveOffset = offset - margin;
            // 设置指定索引的位为 true
            bits.set(effectiveOffset);
            // Find number of consecutive offsets which are acked starting from margin.
            if (effectiveOffset == firstUnackedOffset) {
                // bits.nextClearBit 找到第一个设置为false的位的索引
                firstUnackedOffset = bits.nextClearBit(firstUnackedOffset);
            }
            // Return true if all expected offsets are acked.
            return (firstUnackedOffset == effectiveSize);
        }
        
        boolean bulkAck(int from, int to) {
            bits.set(from - margin, to - margin);
            firstUnackedOffset = bits.nextClearBit(firstUnackedOffset);
            return firstUnackedOffset == effectiveSize;
        }
    }
}
