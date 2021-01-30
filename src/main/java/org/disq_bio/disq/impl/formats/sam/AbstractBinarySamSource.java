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
package org.disq_bio.disq.impl.formats.sam;

import com.google.common.collect.Iterators;
import htsjdk.samtools.AbstractBAMFileIndex;
import htsjdk.samtools.BAMFileReader;
import htsjdk.samtools.BAMFileSpan;
import htsjdk.samtools.BAMIndex;
import htsjdk.samtools.QueryInterval;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileSpan;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.util.CloseableIterator;
import htsjdk.samtools.util.Locatable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.disq_bio.disq.HtsjdkReadsTraversalParameters;
import org.disq_bio.disq.impl.file.FileSystemWrapper;
import org.disq_bio.disq.impl.file.PathChunk;
import org.disq_bio.disq.impl.formats.AutocloseIteratorWrapper;
import org.disq_bio.disq.impl.formats.BoundedTraversalUtil;
import org.disq_bio.disq.impl.formats.SerializableHadoopConfiguration;

public abstract class AbstractBinarySamSource extends AbstractSamSource {

  protected AbstractBinarySamSource(FileSystemWrapper fileSystemWrapper) {
    super(fileSystemWrapper);
  }

  protected void logMsg(String msg) {
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

  /**
   * @return an RDD of reads for a bounded traversal (intervals and whether to return unplaced,
   *     unmapped reads).
   */
  @Override
  public <T extends Locatable> JavaRDD<SAMRecord> getReads(
      JavaSparkContext jsc,
      String path,
      int splitSize,
      HtsjdkReadsTraversalParameters<T> traversalParameters,
      ValidationStringency validationStringency,
      String referenceSourcePath)
      throws IOException {
    if (traversalParameters != null
        && traversalParameters.getIntervalsForTraversal() == null
        && !traversalParameters.getTraverseUnplacedUnmapped()) {
      throw new IllegalArgumentException("Traversing mapped reads only is not supported.");
    }

    long beforeBroadcast = System.currentTimeMillis();
    Broadcast<HtsjdkReadsTraversalParameters<T>> traversalParametersBroadcast =
        traversalParameters == null ? null : jsc.broadcast(traversalParameters);
    SerializableHadoopConfiguration confSer =
        new SerializableHadoopConfiguration(jsc.hadoopConfiguration());
    long afterBroadcast = System.currentTimeMillis();
    logMsg(
        "Broadcasting traversal parameters in getReads took "
            + (afterBroadcast - beforeBroadcast)
            + " milliseconds");

    long before = System.currentTimeMillis();
    AtomicInteger partitions = new AtomicInteger();
    JavaRDD<SAMRecord> record =
        getPathChunks(jsc, path, splitSize, validationStringency, referenceSourcePath)
            .mapPartitions(
                (FlatMapFunction<Iterator<PathChunk>, SAMRecord>)
                    pathChunks -> {
                      Configuration c = confSer.getConf();
                      if (!pathChunks.hasNext()) {
                        return Collections.emptyIterator();
                      }
                      PathChunk pathChunk = pathChunks.next();
                      if (pathChunks.hasNext()) {
                        throw new IllegalArgumentException(
                            "Should not have more than one path chunk per partition");
                      }
                      String p = pathChunk.getPath();
                      long beforeCreateReader = System.currentTimeMillis();
                      SamReader samReader =
                          createSamReader(c, p, validationStringency, referenceSourcePath);
                      long afterCreateReader = System.currentTimeMillis();
                      logMsg(
                          "Create SAM reader took "
                              + (afterCreateReader - beforeCreateReader)
                              + " milliseconds for thread "
                              + Thread.currentThread().getName());
                      long beforeSpan = System.currentTimeMillis();
                      BAMFileSpan splitSpan = new BAMFileSpan(pathChunk.getSpan());
                      long afterSpan = System.currentTimeMillis();
                      logMsg(
                          "create BAMFileSpan took "
                              + (afterSpan - beforeSpan)
                              + " ms for thread "
                              + Thread.currentThread().getName());
                      HtsjdkReadsTraversalParameters<T> traversal =
                          traversalParametersBroadcast == null
                              ? null
                              : traversalParametersBroadcast.getValue();
                      if (traversal == null) {
                        long beforeAutocloseIter = System.currentTimeMillis();
                        // no intervals or unplaced, unmapped reads
                        AutocloseIteratorWrapper<SAMRecord> iter =
                            new AutocloseIteratorWrapper<>(
                                getIterator(samReader, splitSpan), samReader);
                        long afterAutocloseIter = System.currentTimeMillis();
                        logMsg(
                            "Creating autoclose iterator for record took "
                                + (afterAutocloseIter - beforeAutocloseIter)
                                + " ms for thread "
                                + Thread.currentThread().getName());
                        return iter;
                        // end here for cram with supplied reference
                      } else {
                        if (!samReader.hasIndex()) {
                          samReader.close();
                          throw new IllegalArgumentException(
                              "Intervals set but no index file found for " + p);
                        }
                        BAMIndex idx = samReader.indexing().getIndex();
                        long startOfLastLinearBin = idx.getStartOfLastLinearBin();
                        long noCoordinateCount =
                            ((AbstractBAMFileIndex) idx).getNoCoordinateCount();
                        Iterator<SAMRecord> intervalReadsIterator;
                        if (traversal.getIntervalsForTraversal() == null
                            || traversal.getIntervalsForTraversal().isEmpty()) {
                          intervalReadsIterator = Collections.emptyIterator();
                          samReader.close(); // not used from this point on
                        } else {
                          long beforeSAMheader = System.currentTimeMillis();
                          SAMFileHeader header = samReader.getFileHeader();
                          long afterSAMheader = System.currentTimeMillis();
                          logMsg(
                              "Get SAM file header took "
                                  + (afterSAMheader - beforeSAMheader)
                                  + " milliseconds");
                          long beforeQueryIntervals = System.currentTimeMillis();
                          QueryInterval[] queryIntervals =
                              BoundedTraversalUtil.prepareQueryIntervals(
                                  traversal.getIntervalsForTraversal(),
                                  header.getSequenceDictionary());
                          long afterQueryIntervals = System.currentTimeMillis();
                          logMsg(
                              "Query intervals for traversal took "
                                  + (afterQueryIntervals - beforeQueryIntervals)
                                  + " ms");
                          BAMFileSpan span = BAMFileReader.getFileSpan(queryIntervals, idx);
                          span = (BAMFileSpan) span.removeContentsBefore(splitSpan);
                          span = (BAMFileSpan) span.removeContentsAfter(splitSpan);
                          long beforeIndexIter = System.currentTimeMillis();
                          intervalReadsIterator =
                              new AutocloseIteratorWrapper<>(
                                  createIndexIterator(
                                      samReader, queryIntervals, false, span.toCoordinateArray()),
                                  samReader);
                          long afterIndexIter = System.currentTimeMillis();
                          logMsg(
                              "Building index iterator (without unmapped) took "
                                  + (afterIndexIter - beforeIndexIter)
                                  + " ms");
                        }

                        // add on unplaced unmapped reads if there are any in this range
                        long beforeUnmappedRds = System.currentTimeMillis();
                        if (traversal.getTraverseUnplacedUnmapped()) {
                          logMsg("  Unplaced unmapped reads present in this range");
                          if (startOfLastLinearBin != -1
                              && noCoordinateCount
                                  >= getMinUnplacedUnmappedReadsCoordinateCount()) {
                            long unplacedUnmappedStart = startOfLastLinearBin;
                            if (pathChunk.getSpan().getChunkStart() <= unplacedUnmappedStart
                                && unplacedUnmappedStart < pathChunk.getSpan().getChunkEnd()) {
                              SamReader unplacedUnmappedReadsSamReader =
                                  createSamReader(c, p, validationStringency, referenceSourcePath);
                              Iterator<SAMRecord> unplacedUnmappedReadsIterator =
                                  new AutocloseIteratorWrapper<>(
                                      unplacedUnmappedReadsSamReader.queryUnmapped(),
                                      unplacedUnmappedReadsSamReader);
                              return Iterators.concat(
                                  intervalReadsIterator, unplacedUnmappedReadsIterator);
                            }
                          }
                        }
                        long afterUnmappedRds = System.currentTimeMillis();
                        logMsg(
                            "Building index iterator (with unmapped reads) took "
                                + (afterUnmappedRds - beforeUnmappedRds)
                                + " ms");
                        partitions.getAndIncrement();
                        return intervalReadsIterator;
                      }
                    });
    long after = System.currentTimeMillis();
    long total = after - before;
    logMsg("Read record took " + total + " milliseconds.");
    logMsg("There were " + partitions + " partitions.");
    return record;
  }

  protected abstract JavaRDD<PathChunk> getPathChunks(
      JavaSparkContext jsc,
      String path,
      int splitSize,
      ValidationStringency validationStringency,
      String referenceSourcePath)
      throws IOException;

  protected abstract CloseableIterator<SAMRecord> getIterator(
      SamReader samReader, SAMFileSpan chunks);

  protected abstract CloseableIterator<SAMRecord> createIndexIterator(
      SamReader samReader, QueryInterval[] intervals, boolean contained, long[] filePointers);

  protected int getMinUnplacedUnmappedReadsCoordinateCount() {
    return 1;
  }
}
