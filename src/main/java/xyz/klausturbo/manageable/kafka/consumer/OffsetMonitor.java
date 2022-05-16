package xyz.klausturbo.manageable.kafka.consumer;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.OptionalLong;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Kafka consume offset monitor.
 * <p>
 * Each ${@link OffsetMonitor} contains several  ${@link PartitionMonitor} , and each ${@link PartitionMonitor} contains
 * several ${@link PageMonitor}.
 * </p>
 * <p>
 *
 * </p>
 * @author <a href="mailto:guyue375@outlook.com">Klaus.turbo</a>
 * @program manageable-kafka-consumer
 **/
public class OffsetMonitor {
    
    /**
     * Max size of each page (Offset number it can contains).
     */
    private final int pageSize;
    
    /**
     * Max pages that each partition contains .
     */
    private final int maxOpenPagesPerPartition;
    
    /**
     * Relationship of partition and ${@link PartitionMonitor} .
     * <p>
     * PartitionId = ${@link PartitionMonitor}
     * </p>
     */
    private final Map<Integer, PartitionMonitor> partitionMonitors;
    
    /**
     * Constrauctor.
     * @param pageSize                 Size of each page.
     * @param maxOpenPagesPerPartition Max number of page that each ${@link PartitionMonitor} can open.
     */
    public OffsetMonitor(int pageSize, int maxOpenPagesPerPartition) {
        this.pageSize = pageSize;
        this.maxOpenPagesPerPartition = maxOpenPagesPerPartition;
        this.partitionMonitors = new HashMap<>();
    }
    
    /**
     * reset .
     */
    public void reset() {
        partitionMonitors.clear();
    }
    
    /**
     * track offset for a specific partition.
     * @param partition partition id.
     * @param offset    offset .
     * @return
     */
    public boolean track(int partition, long offset) {
        return partitionMonitors.computeIfAbsent(partition, key -> new PartitionMonitor(offset)).track(offset);
    }
    
    /**
     * Confirm which offset the current partition consumes.
     * @param partition partition id.
     * @param offset    offset the current partition consumes.
     * @return
     */
    public OptionalLong ack(int partition, long offset) {
        PartitionMonitor partitionMonitor = partitionMonitors.get(partition);
        if (partitionMonitor == null) {
            return OptionalLong.empty();
        }
        return partitionMonitor.ack(offset);
    }
    
    /**
     * Partition Monitor.
     * <p>
     * Each partition contains a single ${@link PartitionMonitor}.
     * </p>
     * <p>
     * Each ${@link PartitionMonitor} contains several ${@link PageMonitor}.
     * </p>
     */
    private class PartitionMonitor {
        
        /**
         * pageIndex : ----0-----1-----2---.
         * <p>
         * pageMonitor : [0,1] [2,3] [4,5].
         */
        private final Map<Long/*page index*/, PageMonitor> pageMonitors = new HashMap<>();
        
        /**
         * page index that has been tracked and already completed .
         * <p>
         * Use ${@link TreeSet} to sort.
         */
        private final SortedSet<Long /*page index*/> completedPages = new TreeSet<>();
        
        /**
         * The last consecutive page index.
         */
        private long lastConsecutivePageIndex;
        
        /**
         * last opened page index.
         */
        private long lastOpenedPageIndex;
        
        /**
         * last tracked offset.
         */
        private long lastTrackedOffset;
        
        /**
         * Constructor.
         * @param initialOffset first offset for this monitor.
         */
        public PartitionMonitor(long initialOffset /* begin offset of this partition */) {
            openNewPageForOffset(initialOffset);
            lastConsecutivePageIndex = offsetToPage(initialOffset) - 1;
            // last tracked offset for this monitor.
            lastTrackedOffset = initialOffset - 1;
        }
        
        private boolean openNewPageForOffset(long offset) {
            if ((pageMonitors.size() + completedPages.size()) >= maxOpenPagesPerPartition) {
                return false;
            }
            // figure out the index of the new page .
            long pageIndex = offsetToPage(offset);
            // figure out the position of this offset in page.
            // By this step , figure out the margin
            int margin = offsetToPageOffset(offset);
            pageMonitors.put(pageIndex, new PageMonitor(pageSize, margin));
            lastOpenedPageIndex = pageIndex;
            return true;
        }
        
        boolean track(long offset) {
            // figure out where this offset should be placed .
            long pageIndex = offsetToPage(offset);
            /* Condition ONE */
            // Message come orderly.
            if (offset - lastTrackedOffset == 1) {
                // In this loop , this PartitionMonitor is already full and meet the MaxOpenPageSize.
                if (pageIndex > lastOpenedPageIndex && !openNewPageForOffset(offset)) {
                    return false;
                }
                lastTrackedOffset = offset;
                return true;
            }
            /* Condition TWO */
            // Old message comes.
            if (offset <= lastTrackedOffset) {
                return true;
            }
            /* Condition THREE */
            // Lack of message before this offset.
            final PageMonitor lastPage = pageMonitors.get(lastOpenedPageIndex);
            // When code goes here , means ( offset - lastTrackedOffset > 1 ) and this offset belongs to the last page.
            // Lack of message between current offset and lastTrackedOffset.
            if (pageIndex == lastOpenedPageIndex) {
                // Assume that the lacked message has already been handled.
                lastPage.bulkAck(offsetToPageOffset(lastTrackedOffset + 1), offsetToPageOffset(offset));
                return true;
            }
            
            // Page index that happens offset gap.
            final long lastPageIndexBeforeGap = lastOpenedPageIndex;
            // Last offset before the offset gap.
            final long lastOffsetBeforeGap = lastTrackedOffset;
            // Try to open a new page to place this offset.
            if (!openNewPageForOffset(offset)) {
                return false;
            }
            // When code goes here , means offset is not in last page ,and new page has been created for this offset.
            // so fill the last page and
            if (lastPage != null) {
                boolean completed = lastPage.bulkAck(offsetToPageOffset(lastOffsetBeforeGap + 1), pageSize);
                if (completed) {
                    pageMonitors.remove(lastPageIndexBeforeGap);
                }
            }
            completedPages.clear();
            lastConsecutivePageIndex = lastOpenedPageIndex - 1;
            lastTrackedOffset = offset;
            return true;
        }
    
        /**
         *
         * @param offset
         * @return next offset can be ack,empty if there is no offset in next page.
         */
        OptionalLong ack(long offset) {
            // Tell the corresponding page monitor that this offset is acked.
            long pageIndex = offsetToPage(offset);
            PageMonitor pageMonitor = pageMonitors.get(pageIndex);
            if (pageMonitor == null) {
                return OptionalLong.empty();
            }
            int pageOffset = offsetToPageOffset(offset);
            if (!pageMonitor.ack(pageOffset)) {
                return OptionalLong.empty();
            }
            
            // If the page is completed (all offsets in the page is acked), add the pages to the
            // list of completed pages.
            pageMonitors.remove(pageIndex);
            if (pageIndex <= lastConsecutivePageIndex) {
                return OptionalLong.empty();
            }
            completedPages.add(pageIndex);
            
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
            return OptionalLong.of(pageToFirstOffset(lastConsecutivePageIndex + 1));
        }
        
        private long pageToFirstOffset(long pageIndex) {
            return pageIndex * pageSize;
        }
        
     
        private long offsetToPage(long offset) {
            return offset / pageSize;
        }
    
        /**
         * figure out the offset of this offset in current page.
         * @param offset offset
         * @return
         */
        private int offsetToPageOffset(long offset) {
            return (int) (offset % pageSize);
        }
    }
    
    /**
     * ${@link PageMonitor}  bind to a partition.
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
         * <p>
         * Number of offset this monitor can still accept.
         * <p>
         * effectiveSize = size - margin.
         **/
        private final int effectiveSize;
        
        /**
         * the first position of offset that has not been acked.
         **/
        private int firstUnackedOffset;
        
        /**
         * Constructor.
         * @param size   capacity of this page.
         * @param margin means the first offset in this monitor.
         */
        public PageMonitor(int size, int margin) {
            this.margin = margin;
            this.effectiveSize = size - margin;
            this.firstUnackedOffset = 0;
            // init a assigned BitSet
            this.bits = new BitSet(effectiveSize);
        }
        
        int getMargin() {
            return margin;
        }
        
        /**
         * acknowledge the offset in this page
         * <p>
         * mark the assigned position true.
         * @param offset offset .
         * @return
         */
        boolean ack(int offset) {
            // means old message comes.
            if (offset < margin) {
                return false;
            }
            // figure out the offset's position in this page.
            int effectiveOffset = offset - margin;
            // set true for this offset's position in the page.
            bits.set(effectiveOffset);
            // find number of consecutive offsets which are acked starting from margin.
            if (effectiveOffset == firstUnackedOffset) {
                // bits.nextClearBit ：find the first false position after firstUnackedOffset
                firstUnackedOffset = bits.nextClearBit(firstUnackedOffset);
            }
            // return true if all expected offsets are acked.
            return (firstUnackedOffset == effectiveSize);
        }
        
        boolean bulkAck(int from, int to) {
            bits.set(from - margin, to - margin);
            firstUnackedOffset = bits.nextClearBit(firstUnackedOffset);
            return firstUnackedOffset == effectiveSize;
        }
    }
}
