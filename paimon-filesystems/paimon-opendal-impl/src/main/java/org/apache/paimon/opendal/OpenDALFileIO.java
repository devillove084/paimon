/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.opendal;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;

import org.apache.opendal.Metadata;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OpenDALFileIO implements FileIO {

    private static final long serialVersionUID = 1L;

    private transient volatile OperatorWrapper operator;

    public OpenDALFileIO() {}

    @Override
    public boolean isObjectStore() {
        // TODO(devillove084): prepare for file and azure
        return true;
    }

    @Override
    public void configure(CatalogContext context) {
        if (operator == null) {
            synchronized (this) {
                if (operator == null) {
                    this.operator = OperatorFactory.create(context);
                }
            }
        }
    }

    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
        byte[] content = operator.readAll(path);
        return new OpenDALSeekableInputStream(content);
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException {
        return new OpenDALPositionOutputStream(operator, path, overwrite);
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        Metadata meta = operator.stat(path);
        return new OpenDALFileStatus(path, meta);
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        List<Metadata> metas = operator.list(path);
        List<FileStatus> results = new ArrayList<>();
        for (Metadata m : metas) {
            results.add(new OpenDALFileStatus(path, m));
        }
        return results.toArray(new FileStatus[0]);
    }

    @Override
    public boolean exists(Path path) throws IOException {
        try {
            operator.stat(path);
            return true;
        } catch (IOException e) {
            // TODO(devillove084): not good
            return false;
        }
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        operator.delete(path, recursive);
        return true;
    }

    @Override
    public boolean mkdirs(Path path) throws IOException {
        // TODO(devillove084): normal obj store meaningless
        operator.createDir(path);
        return true;
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        operator.rename(src, dst);
        return true;
    }

    private static class OpenDALFileStatus implements FileStatus {

        private final Path path;
        private final Metadata metadata;

        private OpenDALFileStatus(Path path, Metadata metadata) {
            this.path = path;
            this.metadata = metadata;
        }

        @Override
        public long getLen() {
            return metadata.getContentLength();
        }

        @Override
        public boolean isDir() {
            return metadata.isDir();
        }

        @Override
        public Path getPath() {
            return path;
        }

        @Override
        public long getModificationTime() {
            return metadata.lastModified.toEpochMilli();
        }

        @Override
        public long getAccessTime() {
            // TODO(devillove084): calculate by ourself
            return metadata.getLastModified().toEpochMilli();
        }

        @Override
        public String getOwner() {
            return metadata.getVersion();
        }
    }

    private static class OpenDALSeekableInputStream extends SeekableInputStream {

        private final byte[] data;
        private final ByteArrayInputStream bais;
        private long markPos = 0; // 用于模拟 getPos()

        private OpenDALSeekableInputStream(byte[] data) {
            this.data = data;
            this.bais = new ByteArrayInputStream(data);
        }

        @Override
        public void seek(long seekPos) throws IOException {
            bais.reset();
            long skipped = bais.skip(seekPos);
            markPos = skipped;
        }

        @Override
        public long getPos() throws IOException {
            return markPos;
        }

        @Override
        public int read() throws IOException {
            int res = bais.read();
            if (res != -1) {
                markPos++;
            }
            return res;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int n = bais.read(b, off, len);
            if (n > 0) {
                markPos += n;
            }
            return n;
        }

        @Override
        public void close() throws IOException {
            bais.close();
        }
    }

    // TODO(devillove084): change to streaming or multi part write
    private static class OpenDALPositionOutputStream extends PositionOutputStream {

        private final OperatorWrapper operator;
        private final Path path;
        private final boolean overwrite;
        private final ByteArrayOutputStream baos;

        private OpenDALPositionOutputStream(
                OperatorWrapper operator, Path path, boolean overwrite) {
            this.operator = operator;
            this.path = path;
            this.overwrite = overwrite;
            this.baos = new ByteArrayOutputStream();
        }

        @Override
        public long getPos() throws IOException {
            return baos.size();
        }

        @Override
        public void write(int b) throws IOException {
            baos.write(b);
        }

        @Override
        public void write(byte[] b) throws IOException {
            baos.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            baos.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            // TODO(devillove084):
        }

        @Override
        public void close() throws IOException {
            operator.writeAll(path, baos.toByteArray(), overwrite);
            baos.close();
        }
    }
}
