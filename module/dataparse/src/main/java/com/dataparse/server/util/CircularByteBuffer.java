package com.dataparse.server.util;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A circular array buffer with a copy-and-swap cursor.
 *
 * <p>This class provides an list of T objects who's size is <em>unstable</em>.
 * It's intended for capturing data where the frequency of sampling greatly
 * outweighs the frequency of inspection (for instance, monitoring).</p>
 *
 * <p>This object keeps in memory a fixed size buffer which is used for
 * capturing objects.  It copies the objects to a snapshot array which may be
 * worked with.  The size of the snapshot array will vary based on the
 * stability of the array during the copy operation.</p>
 *
 * <p>Adding buffer to the buffer is <em>O(1)</em>, and lockless.  Taking a
 * stable copy of the sample is <em>O(n)</em>.</p>
 */
public class CircularByteBuffer {
    private final AtomicLong cursor = new AtomicLong();
    private final byte[] buffer;

    /**
     * Create a new concurrent circular buffer.
     *
     * @param bufferSize The size of the buffer.
     *
     * @throws IllegalArgumentException if the bufferSize is a non-positive
     * value.
     */
    public CircularByteBuffer(final int bufferSize)
    {
        if (bufferSize < 1) {
            throw new IllegalArgumentException(
                    "Buffer size must be a positive value"
            );
        }
        this.buffer = new byte [ bufferSize ];
    }

    /**
     * Add a new object to this buffer.
     *
     * <p>Add a new object to the cursor-point of the buffer.</p>
     *
     * @param sample The object to add.
     */
    public void add (byte sample) {
        buffer[(int) (cursor.getAndIncrement() % buffer.length)] = sample;
    }

    /**
     * Return a stable snapshot of the buffer.
     *
     * <p>Capture a stable snapshot of the buffer as an array.  The snapshot
     * may not be the same length as the buffer, any objects which were
     * unstable during the copy will be factored out.</p>
     *
     * @return An array snapshot of the buffer.
     */
    public byte[] snapshot () {
        byte[] snapshots = new byte [ buffer.length ];

        /* Determine the size of the snapshot by the number of affected
         * records.  Trim the size of the snapshot by the number of records
         * which are considered to be unstable during the copy (the amount the
         * cursor may have moved while the copy took place).
         *
         * If the cursor eliminated the sample (if the sample size is so small
         * compared to the rate of mutation that it did a full-wrap during the
         * copy) then just treat the buffer as though the cursor is
         * buffer.length - 1 and it was not changed during copy (this is
         * unlikley, but it should typically provide fairly stable results).
         */
        long before = cursor.get();

        /* If the cursor hasn't yet moved, skip the copying and simply return a
         * zero-length array.
         */
        if (before == 0) {
            return new byte[0];
        }

        System.arraycopy(buffer, 0, snapshots, 0, buffer.length);

        long after          = cursor.get();
        int  size           = buffer.length - (int) (after - before);
        long snapshotCursor = before - 1;

        /* Highly unlikely, but the entire buffer was replaced while we
         * waited...so just return a zero length array, since we can't get a
         * stable snapshot...
         */
        if (size <= 0) {
            return new byte[0];
        }

        long start = snapshotCursor - (size - 1);
        long end   = snapshotCursor;

        if (snapshotCursor < snapshots.length) {
            size   = (int) snapshotCursor + 1;
            start  = 0;
        }

        /* Copy the sample snapshot to a new array the size of our stable
         * snapshot area.
         */
        byte[] result = new byte[size];

        int startOfCopy = (int) (start % snapshots.length);
        int endOfCopy   = (int) (end   % snapshots.length);

        /* If the buffer space wraps the physical end of the array, use two
         * copies to construct the new array.
         */
        if (startOfCopy > endOfCopy) {
            System.arraycopy(snapshots, startOfCopy,
                             result, 0,
                             snapshots.length - startOfCopy);
            System.arraycopy(snapshots, 0,
                             result, (snapshots.length - startOfCopy),
                             endOfCopy + 1);
        }
        else {
            /* Otherwise it's a single continuous segment, copy the whole thing
             * into the result.
             */
            System.arraycopy(snapshots, startOfCopy,
                             result, 0, endOfCopy - startOfCopy + 1);
        }

        return result;
    }
}
