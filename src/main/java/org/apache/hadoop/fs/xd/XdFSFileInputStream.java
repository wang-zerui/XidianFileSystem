package org.apache.hadoop.fs.xd;

import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Path;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class XdFSFileInputStream extends FSInputStream {
    private FileInputStream fis;
    private long position;

    public XdFSFileInputStream(Path f) throws IOException {
        XdFileSystem tmp = new XdFileSystem();
        fis = new FileInputStream(tmp.pathToFile(f));
    }

    @Override
    public void seek(long pos) throws IOException {
        if (pos < 0) {
            throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK);
        }
        fis.getChannel().position(pos);
        this.position = pos;
    }

    @Override
    public long getPos() throws IOException {
        return 0;
    }

    /**
     * 不支持该操作
     * @param targetPos
     * @return
     * @throws IOException
     */
    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }

    /**
     * 都委托给FileInputStream
     * @return
     * @throws IOException
     */
    @Override
    public int available() throws IOException { return fis.available(); }
    @Override
    public void close() throws IOException { fis.close(); }

    @Override
    public int read() throws IOException {
        int value = fis.read();
        if (value >= 0) {
            this.position++;
//                statistics.incrementBytesRead(1);
        }
        return value;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int value = fis.read(b, off, len);
        if (value > 0) {
            this.position += value;
//                statistics.incrementBytesRead(value);
        }
        return value;
    }

    @Override
    public int read(long position, byte[] b, int off, int len)
            throws IOException {
        ByteBuffer bb = ByteBuffer.wrap(b, off, len);
        int value = fis.getChannel().read(bb, position);
        if (value > 0) {
//                statistics.incrementBytesRead(value);
        }
        return value;
    }

    @Override
    public long skip(long n) throws IOException {
        long value = fis.skip(n);
        if (value > 0) {
            this.position += value;
        }
        return value;
    }

}
