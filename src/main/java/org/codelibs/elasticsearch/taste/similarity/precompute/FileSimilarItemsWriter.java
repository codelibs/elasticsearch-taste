/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.codelibs.elasticsearch.taste.similarity.precompute;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import com.google.common.base.Charsets;
import com.google.common.io.Closeables;

/**
 * Persist the precomputed item similarities to a file that can later be used
 * by a {@link org.codelibs.elasticsearch.taste.impl.similarity.file.FileItemSimilarity}
 */
public class FileSimilarItemsWriter implements SimilarItemsWriter {

    private final File file;

    private BufferedWriter writer;

    public FileSimilarItemsWriter(final File file) {
        this.file = file;
    }

    @Override
    public void open() throws IOException {
        writer = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(file), Charsets.UTF_8));
    }

    @Override
    public void add(final SimilarItems similarItems) throws IOException {
        final String itemID = String.valueOf(similarItems.getItemID());
        for (final SimilarItem similarItem : similarItems.getSimilarItems()) {
            writer.write(itemID);
            writer.write(',');
            writer.write(String.valueOf(similarItem.getItemID()));
            writer.write(',');
            writer.write(String.valueOf(similarItem.getSimilarity()));
            writer.newLine();
        }
    }

    @Override
    public void close() throws IOException {
        Closeables.close(writer, false);
    }
}
