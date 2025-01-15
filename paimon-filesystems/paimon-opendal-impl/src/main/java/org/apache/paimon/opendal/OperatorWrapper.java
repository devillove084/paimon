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

import org.apache.opendal.Entry;
import org.apache.opendal.Metadata;
import org.apache.opendal.OpenDALException;
import org.apache.opendal.Operator;
import org.apache.paimon.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OperatorWrapper {
    private final Operator op;

    public OperatorWrapper(Operator op) {
        this.op = op;
    }

    public byte[] readAll(Path path) throws IOException {
         try {
             byte[] data = op.read(path.toString());
             return data;
         } catch (OpenDALException e) {
             throw new FileNotFoundException("File read failed: " + path + ", " + e.getMessage());
         }
    }

    public void writeAll(Path path, byte[] data, boolean overwrite) throws IOException {
         try {
//             if (!overwrite) {
//                 // Optional: check existence first
//                 if ((path)) {
//                     throw new IOException("File already exists, not overwriting: " + path);
//                 }
//             }
             op.write(path.toString(), data);
         } catch (OpenDALException e) {
             throw new IOException("Failed to write to path: " + path + ", " + e.getMessage(), e);
         }
    }

    public Metadata stat(Path path) throws IOException {
         try {
             Metadata s3Meta = op.stat(path.toString());
             return s3Meta;
         } catch (OpenDALException e) {
             throw new IOException("File not found: " + path + ", " + e.getMessage());
         }
    }

    public List<Metadata> list(Path path) throws IOException {
         try {
             List<Entry> entries = op.list(path.toString());
             List<Metadata> result = new ArrayList<>();
             for (Entry entry : entries) {
                 result.add(entry.metadata);
             }
             return result;
         } catch (OpenDALException e) {
             throw new IOException("Failed to list path: " + path + ", " + e.getMessage(), e);
         }
    }

    public void delete(Path path, boolean recursive) throws IOException {
         try {
             if (!recursive) {
                 op.delete(path.toString());
             } else {
                 // 1. list(path)
                 // 2. for each child, call delete(childPath, true)
                 // 3. delete the path itself
                 List<Metadata> children = list(path);
                 for (Metadata child : children) {
                     Path childFullPath = new Path(path, child.toString());
                     if (child.isDir()) {
                         delete(childFullPath, true);
                     } else {
                         op.delete(childFullPath.toString());
                     }
                 }
                 op.delete(path.toString());
             }
         } catch (OpenDALException e) {
             // ? If file not found, you might ignore or rethrow
         }
    }

    public void createDir(Path path) throws IOException {
         try {
             // For S3-like object stores, you might just create a zero-length object with a trailing slash
             // or no operation at all if the store doesn't require a directory concept.
             String s3DirectoryKey = path.toString();
             if (!s3DirectoryKey.endsWith("/")) {
                 s3DirectoryKey += "/";
             }
             op.createDir(s3DirectoryKey);
         } catch (OpenDALException e) {
             throw new IOException("Failed to create directory: " + path + ", " + e.getMessage(), e);
         }
    }

    public void rename(Path src, Path dst) throws IOException {
         try {
             op.rename(src.toString(), dst.toString());
         } catch (OpenDALException e) {
             throw new IOException("Failed to rename " + src + " to " + dst + ", " + e.getMessage(), e);
         }
    }
}
