package org.apache.spark.network.protocol;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCountUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * Created by zhaoshufen
 * User:  zhaoshufen
 * Date: 2017/8/19
 * Time: 15:06
 * To change this setting on:Preferences->editor->File and Code Templates->Include->File Header
 */
public class MessageWithHeader  extends AbstractReferenceCounted implements FileRegion {
    private final ByteBuf header;
    private final int headerLength;
    private final Object body;
    private final long bodyLength;
    private long totalBytesTransferred;

    /**
     * When the write buffer size is larger than this limit, I/O will be done in chunks of this size.
     * The size should not be too large as it will waste underlying memory copy. e.g. If network
     * available buffer is smaller than this limit, the data cannot be sent within one single write
     * operation while it still will make memory copy with this size.
     * 当写入缓冲超出这个大小，IO将在这个块大小中执行。
     * 这个限制大小不能太大，那样会浪费底层缓冲拷贝
     * 如果网络云讯缓冲比这个限制小，数据就不能在一个单独的写操作发送，但是仍然使用这个大小拷贝数据
     */
    private static final int NIO_BUFFER_LIMIT = 256 * 1024;
    MessageWithHeader(ByteBuf header, Object body, long bodyLength) {
        Preconditions.checkArgument(body instanceof ByteBuf || body instanceof FileRegion,
                "Body must be a ByteBuf or a FileRegion.");
        this.header = header;
        this.headerLength = header.readableBytes();
        this.body = body;
        this.bodyLength = bodyLength;
    }

    @Override
    public long count() {
        return headerLength + bodyLength;
    }
    @Override
    public long position() {
        return 0;
    }
    @Override
    protected void deallocate() {
        header.release();
        ReferenceCountUtil.release(body);
    }
    @Override
    public long transfered() {
        return totalBytesTransferred;
    }
    /**
     * This code is more complicated than you would think because we might require multiple
     * transferTo invocations in order to transfer a single MessageWithHeader to avoid busy waiting.
     *
     * The contract is that the caller will ensure position is properly set to the total number
     * of bytes transferred so far (i.e. value returned by transfered()).
     */
    @Override
    public long transferTo(final WritableByteChannel target, final long position) throws IOException {
        Preconditions.checkArgument(position == totalBytesTransferred, "Invalid position.");
        // Bytes written for header in this call.
        long writtenHeader = 0;
        if (header.readableBytes() > 0) {
            writtenHeader = copyByteBuf(header, target);
            totalBytesTransferred += writtenHeader;
            if (header.readableBytes() > 0) {
                return writtenHeader;
            }
        }
        // Bytes written for body in this call.
        long writtenBody = 0;
        if (body instanceof FileRegion) {
            writtenBody = ((FileRegion) body).transferTo(target, totalBytesTransferred - headerLength);

        }else if (body instanceof ByteBuf) {
            writtenBody = copyByteBuf((ByteBuf) body, target);
        }
        totalBytesTransferred += writtenBody;
        return writtenHeader + writtenBody;

    }
    private int copyByteBuf(ByteBuf buf, WritableByteChannel target) throws IOException {
        ByteBuffer buffer = buf.nioBuffer();
        int written = (buffer.remaining() <= NIO_BUFFER_LIMIT) ?
                target.write(buffer) : writeNioBuffer(target, buffer);
        buf.skipBytes(written);
        return written;
    }
    private int writeNioBuffer(
            WritableByteChannel writeCh,
            ByteBuffer buf) throws IOException {
        int originalLimit = buf.limit();
        int ret = 0;

        try {
            int ioSize = Math.min(buf.remaining(), NIO_BUFFER_LIMIT);
            buf.limit(buf.position() + ioSize);
            ret = writeCh.write(buf);
        } finally {
            buf.limit(originalLimit);
        }

        return ret;
    }
}
