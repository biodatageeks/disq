/*
 * Disq
 *
 * MIT License
 *
 * Copyright (c) 2018-2019 Disq contributors
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package org.disq_bio.disq.impl.formats.cram;

import htsjdk.samtools.CRAMCRAIIndexer;
import htsjdk.samtools.CRAMFileReader;
import htsjdk.samtools.Chunk;
import htsjdk.samtools.QueryInterval;
import htsjdk.samtools.SAMFileSpan;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReader.PrimitiveSamReaderToSamReaderAdapter;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.cram.CRAIEntry;
import htsjdk.samtools.cram.CRAIIndex;
import htsjdk.samtools.cram.build.CramContainerHeaderIterator;
import htsjdk.samtools.cram.structure.Container;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.BlockCompressedFilePointerUtil;
import htsjdk.samtools.util.CloseableIterator;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.disq_bio.disq.impl.file.FileSystemWrapper;
import org.disq_bio.disq.impl.file.PathChunk;
import org.disq_bio.disq.impl.file.PathSplit;
import org.disq_bio.disq.impl.file.PathSplitSource;
import org.disq_bio.disq.impl.formats.SerializableHadoopConfiguration;
import org.disq_bio.disq.impl.formats.sam.AbstractBinarySamSource;
import org.disq_bio.disq.impl.formats.sam.SamFormat;

public class CramSource extends AbstractBinarySamSource implements Serializable {

  private final PathSplitSource pathSplitSource;

  private void logMsg(String msg) {
    System.out.println("*");
    System.out.println("*");
    System.out.println("*");
    System.out.println("*");
    System.out.println("*> " + msg);
    System.out.println("*");
    System.out.println("*");
    System.out.println("*");
    System.out.println("*");
  }

  public CramSource(FileSystemWrapper fileSystemWrapper) {
    super(fileSystemWrapper);
    this.pathSplitSource = new PathSplitSource(fileSystemWrapper);
  }

  @Override
  public SamFormat getSamFormat() {
    return SamFormat.CRAM;
  }

  @Override
  protected JavaRDD<PathChunk> getPathChunks(
      JavaSparkContext jsc,
      String path,
      int splitSize,
      ValidationStringency stringency,
      String referenceSourcePath)
      throws IOException {

    final Configuration conf = jsc.hadoopConfiguration();

    // store paths (not full URIs) to avoid differences in scheme - this could be improved
    Map<String, NavigableSet<Long>> pathToContainerOffsets = new LinkedHashMap<>();
    long containerOffsetsLoadingTimeDirectory;
    long containerOffsetsLoadingTime;

    logMsg("Loading container offsets");
    if (fileSystemWrapper.isDirectory(conf, path)) {
      logMsg("Inside if directory");
      List<FileSystemWrapper.FileStatus> statuses =
          fileSystemWrapper.listDirectoryStatus(conf, path).stream()
              .filter(fs -> SamFormat.CRAM.fileMatches(fs.getPath()))
              .collect(Collectors.toList());
      logMsg("Container offsets for each status");
      long before = System.currentTimeMillis();
      for (FileSystemWrapper.FileStatus status : statuses) {
        String p = status.getPath();
        long cramFileLength = status.getLength();
        NavigableSet<Long> containerOffsets = getContainerOffsetsFromIndex(conf, p, cramFileLength);
        String normPath = URI.create(fileSystemWrapper.normalize(conf, p)).getPath();
        pathToContainerOffsets.put(normPath, containerOffsets);
      }
      long after = System.currentTimeMillis();
      containerOffsetsLoadingTimeDirectory = after - before;
      logMsg(
          "Container offsets loading for directory took "
              + containerOffsetsLoadingTimeDirectory
              + " milliseconds");
    } else {
      long cramFileLength = fileSystemWrapper.getFileLength(conf, path);
      long before = System.currentTimeMillis();
      NavigableSet<Long> containerOffsets =
          getContainerOffsetsFromIndex(conf, path, cramFileLength);
      long after = System.currentTimeMillis();
      containerOffsetsLoadingTime = after - before;
      logMsg(
          "Container offsets loading for no directory took "
              + containerOffsetsLoadingTime
              + " milliseconds");
      String normPath = URI.create(fileSystemWrapper.normalize(conf, path)).getPath();
      pathToContainerOffsets.put(normPath, containerOffsets);
    }

    long broadcastTime;
    long before = System.currentTimeMillis();
    Broadcast<Map<String, NavigableSet<Long>>> containerOffsetsBroadcast =
        jsc.broadcast(pathToContainerOffsets);
    long after = System.currentTimeMillis();
    broadcastTime = after - before;
    logMsg("Broadcast took " + broadcastTime + " milliseconds");

    SerializableHadoopConfiguration confSer =
        new SerializableHadoopConfiguration(jsc.hadoopConfiguration());

    before = System.currentTimeMillis();
    JavaRDD<PathChunk> chunk =
        pathSplitSource
            .getPathSplits(jsc, path, splitSize)
            .flatMap(
                (FlatMapFunction<PathSplit, PathChunk>)
                    pathSplit -> {
                      Configuration c = confSer.getConf();
                      String p = pathSplit.getPath();
                      Map<String, NavigableSet<Long>> pathToOffsets =
                          containerOffsetsBroadcast.getValue();
                      String normPath = URI.create(fileSystemWrapper.normalize(c, p)).getPath();
                      NavigableSet<Long> offsets = pathToOffsets.get(normPath);
                      long newStart = offsets.ceiling(pathSplit.getStart());
                      long newEnd = offsets.ceiling(pathSplit.getEnd());
                      if (newStart == newEnd) {
                        return Collections.emptyIterator();
                      }
                      // Subtract one from end since CRAMIterator's boundaries are inclusive
                      PathChunk pathChunk =
                          new PathChunk(
                              p,
                              new Chunk(
                                  BlockCompressedFilePointerUtil.makeFilePointer(newStart),
                                  BlockCompressedFilePointerUtil.makeFilePointer(newEnd - 1)));
                      return Collections.singleton(pathChunk).iterator();
                    });
    after = System.currentTimeMillis();
    long pathChunkTime = after - before;
    logMsg("Read path chunk into RDD took " + pathChunkTime + " milliseconds");
    return chunk;
  }

  private NavigableSet<Long> getContainerOffsetsFromIndex(
      Configuration conf, String path, long cramFileLength) throws IOException {
    try (SeekableStream in = findIndex(conf, path)) {
      if (in == null) {
        return getContainerOffsetsFromFile(conf, path, cramFileLength);
      }
      long before = System.currentTimeMillis();
      NavigableSet<Long> containerOffsets = new TreeSet<>();
      CRAIIndex index = CRAMCRAIIndexer.readIndex(in);
      long after = System.currentTimeMillis();
      long elapsed = after - before;
      logMsg("CRAMCRAIIndexer readIndex took " + elapsed + " milliseconds");
      for (CRAIEntry entry : index.getCRAIEntries()) {
        containerOffsets.add(entry.getContainerStartByteOffset());
      }
      containerOffsets.add(cramFileLength);
      return containerOffsets;
    }
  }

  private NavigableSet<Long> getContainerOffsetsFromFile(
      Configuration conf, String path, long cramFileLength) throws IOException {
    try (SeekableStream seekableStream = fileSystemWrapper.open(conf, path)) {
      CramContainerHeaderIterator it = new CramContainerHeaderIterator(seekableStream);
      NavigableSet<Long> containerOffsets = new TreeSet<>();
      while (it.hasNext()) {
        Container container = it.next();
        containerOffsets.add(container.getContainerByteOffset());
      }
      containerOffsets.add(cramFileLength);
      return containerOffsets;
    }
  }

  private CRAMFileReader getUnderlyingCramFileReader(SamReader samReader) {
    return (CRAMFileReader) ((PrimitiveSamReaderToSamReaderAdapter) samReader).underlyingReader();
  }

  @Override
  protected CloseableIterator<SAMRecord> getIterator(SamReader samReader, SAMFileSpan chunks) {
    return getUnderlyingCramFileReader(samReader).getIterator(chunks);
  }

  @Override
  protected CloseableIterator<SAMRecord> createIndexIterator(
      SamReader samReader, QueryInterval[] intervals, boolean contained, long[] filePointers) {
    return getUnderlyingCramFileReader(samReader)
        .createIndexIterator(intervals, contained, filePointers);
  }

  @Override
  protected int getMinUnplacedUnmappedReadsCoordinateCount() {
    return 0; // noCoordinateCount always seems to be 0 for CRAM files
  }
}
