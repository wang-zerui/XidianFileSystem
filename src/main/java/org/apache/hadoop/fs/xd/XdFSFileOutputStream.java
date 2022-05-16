package org.apache.hadoop.fs.xd;

import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

class XdFSFileOutputStream extends OutputStream {
    private FileOutputStream fos;

    XdFSFileOutputStream(Path f, boolean append,
                         FsPermission permission) throws IOException {
        XdFileSystem xdFs = new XdFileSystem();
        File file = xdFs.pathToFile(f);
//        if (permission == null) {
//            this.fos = new FileOutputStream(file, append);
//        } else {
////            this.fos = NativeIO.Windows.createFileOutputStreamWithMode(file,
//        }
        this.fos = new FileOutputStream(file, append);
    }

    /*
     * Just forward to the fos
     */
    @Override
    public void close() throws IOException { fos.close(); }
    @Override
    public void flush() throws IOException { fos.flush(); }
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        fos.write(b, off, len);
    }

    @Override
    public void write(int b) throws IOException {
        fos.write(b);
    }
}

