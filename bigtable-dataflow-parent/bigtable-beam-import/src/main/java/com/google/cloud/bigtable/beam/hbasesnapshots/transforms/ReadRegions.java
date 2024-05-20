package com.google.cloud.bigtable.beam.hbasesnapshots.transforms;

// import com.google.cloud.bigtable.beam.hbasesnapshots.ImportSnapshots;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.beam.CloudBigtableIO.CloudBigtableMultiTableWriteFn;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.RegionConfig;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.SnapshotConfig;
import com.google.common.base.Preconditions;
import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.google.common.hash.Hashing;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.hbase.wal.WAL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PTransform} for reading the records from each region and creates Hbase {@link Mutation}
 * instances. Each region will be split into configured size (512 MB) and pipeline option {@link
 * ImportSnapshots.ImportSnapshotsOptions#getUseDynamicSplitting() useDynamicSplitting} can be used
 * to control whether each split needs to be subdivided further or not.
 */
@InternalApi("For internal usage only")
public class ReadRegions
    extends PTransform<PCollection<RegionConfig>, PCollection<Void>> {

  private static final long BYTES_PER_SPLIT = 512 * 1024 * 1024; // 512 MB

  private static final long BYTES_PER_GB = 1024 * 1024 * 1024;

  private final boolean useDynamicSplitting;

  private final int maxMutationsPerRequestThreshold;

  private final boolean filterLargeRows;
  private final long filterLargeRowThresholdBytes;

  private final boolean filterLargeCells;
  private final int filterLargeCellThresholdBytes;

  private final boolean filterLargeRowKeys;
  private final int filterLargeRowKeysThresholdBytes;
  private final @Nullable String checkpointPath;
  private final CloudBigtableTableConfiguration cloudBigtableTableConfiguration;


  public ReadRegions(
      boolean useDynamicSplitting,
      int maxMutationsPerRequestThreshold,
      boolean filterLargeRows,
      long filterLargeRowThresholdBytes,
      boolean filterLargeCells,
      int filterLargeCellThresholdBytes,
      boolean filterLargeRowKeys,
      int filterLargeRowKeysThresholdBytes,
      @Nullable String checkpointPath,
      CloudBigtableTableConfiguration bigtableTableConfiguration) {

    this.useDynamicSplitting = useDynamicSplitting;
    this.maxMutationsPerRequestThreshold = maxMutationsPerRequestThreshold;

    this.filterLargeRows = filterLargeRows;
    this.filterLargeRowThresholdBytes = filterLargeRowThresholdBytes;

    this.filterLargeCells = filterLargeCells;
    this.filterLargeCellThresholdBytes = filterLargeCellThresholdBytes;

    this.filterLargeRowKeys = filterLargeRowKeys;
    this.filterLargeRowKeysThresholdBytes = filterLargeRowKeysThresholdBytes;

    this.checkpointPath = checkpointPath;
    this.cloudBigtableTableConfiguration = bigtableTableConfiguration;
  }

  @Override
  public PCollection<Void> expand(PCollection<RegionConfig> regionConfig) {

    CreateMutationsFn createMutationsFn = new CreateMutationsFn(
        this.maxMutationsPerRequestThreshold,
        this.filterLargeRows,
        this.filterLargeRowThresholdBytes,
        this.filterLargeCells,
        this.filterLargeCellThresholdBytes,
        this.filterLargeRowKeys,
        this.filterLargeRowKeysThresholdBytes);

    CloudBigtableMultiTableWriteFn writeFn = new CloudBigtableMultiTableWriteFn(this.cloudBigtableTableConfiguration);

    return regionConfig
        .apply("Read snapshot region and Write", ParDo.of(
            new ReadRegionFn(this.useDynamicSplitting, this.checkpointPath, createMutationsFn, writeFn)));
  }

  static class ReadRegionFn extends DoFn<RegionConfig, Void> {
    static class RegionProgress {
      public byte[] firstKey;
      public byte[] lastKey;
      public String regionName;
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Logger LOG = LoggerFactory.getLogger(ReadRegionFn.class);

    private final boolean useDynamicSplitting;
    private final @Nullable String checkpointPath;
    private final CreateMutationsFn createMutationsFn;
    private final CloudBigtableMultiTableWriteFn writeFn;
    private final Map<RegionInfo, RangeSet<ByteKey>> regionProgress;

    public ReadRegionFn(boolean useDynamicSplitting,
        @Nullable String checkpointPath,
        CreateMutationsFn createMutationsFn,
        CloudBigtableMultiTableWriteFn writeFn) {
      this.useDynamicSplitting = useDynamicSplitting;
      this.checkpointPath = checkpointPath;
      this.createMutationsFn = createMutationsFn;
      this.writeFn = writeFn;
      this.regionProgress = Maps.newHashMap();
    }

    @StartBundle
    public void startBundle() throws Exception {
      writeFn.startBundle();
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext context) throws Exception {
      // ensure all buffered writes complete
      writeFn.finishBundle(null);

      if (checkpointPath != null) {
        try {
          writeSuccessFiles();
        } catch (IOException ex) {
          LOG.error("Error writing checkpoint file(s)", ex);
        }
      }
      regionProgress.clear();
    }

    private void writeSuccessFiles() throws IOException {
      for (Map.Entry<RegionInfo, RangeSet<ByteKey>> rp : regionProgress.entrySet()) {
        for (Range<ByteKey> range : rp.getValue().asRanges()) {
          writeSuccessFile(rp.getKey(), range);
        }
      }
    }

    private void writeSuccessFile(RegionInfo region, Range<ByteKey> range) throws IOException {
      ByteKeyRange bkRange = toRange(range);
      ResourceId checkpointResource =
          FileSystems.matchNewResource(checkpointPath, true);
      String keySha = Hashing.sha256()
          .newHasher()
          .putBytes(bkRange.getStartKey().getBytes())
          .putBytes(bkRange.getEndKey().getBytes())
          .hash()
          .toString();

      // keys can be arbitrarily long and GCS limits the file name length to 1024 characters, so we
      // use the "encoded name" (an MD5 of the name) instead, and then validate the full name on
      // read in case of collisions.
      String uniqueRegionCheckpoint =
          region.getEncodedName() + "-" + keySha + ".SUCCESS";
      ResourceId fullCheckpoint = checkpointResource.resolve(uniqueRegionCheckpoint,
          StandardResolveOptions.RESOLVE_FILE);

      try (WritableByteChannel out = FileSystems.create(fullCheckpoint, "text/plain")) {
        try (OutputStream outStream = Channels.newOutputStream(out)) {
          RegionProgress rp = new RegionProgress();
          rp.firstKey = bkRange.getStartKey().getBytes();
          rp.lastKey = bkRange.getEndKey().getBytes();
          rp.regionName = region.getRegionNameAsString();
          MAPPER.writeValue(outStream, rp);
        }
      }
    }

    private @Nonnull RegionProgress readCheckpointFile(ResourceId checkpoint) throws IOException {
      try (ReadableByteChannel rbc = FileSystems.open(checkpoint)) {
        try (InputStream is = Channels.newInputStream(rbc)) {
          return MAPPER.readValue(is, RegionProgress.class);
        }
      }
    }

    /**
     * Attempt to read the completed ranges for a given region from previously written checkpoint
     * files.
     */
    private RangeSet<ByteKey> readCompletedRanges(RegionInfo regionInfo) {
      RangeSet<ByteKey> result = TreeRangeSet.create();
      if (checkpointPath == null) {
        return result;
      }

      ResourceId checkpointResource =
          FileSystems.matchNewResource(checkpointPath, true);
      String prefix = regionInfo.getEncodedName() + "-*";
      String matchGlob = checkpointResource.resolve(prefix, StandardResolveOptions.RESOLVE_FILE).toString();
      List<RegionProgress> checkpoints = Lists.newArrayList();
      try {
        MatchResult matched = FileSystems.match(matchGlob, EmptyMatchTreatment.ALLOW);
        for (Metadata md : matched.metadata()) {
          checkpoints.add(readCheckpointFile(md.resourceId()));
        }
      } catch (IOException ex) {
        LOG.error("Error globbing checkpoint files", ex);
        return result;
      }

      if (checkpoints.isEmpty()) {
        return result;
      }

      String expectedRegionName = regionInfo.getRegionNameAsString();
      for (RegionProgress rp : checkpoints) {
        if (!rp.regionName.equals(expectedRegionName)) {
          continue;
        }
        result.add(toRange(rp.firstKey, rp.lastKey));
      }
      return result;
    }

    private static Range<ByteKey> toRange(byte[] startInclusive, byte[] endExclusive) {
      return toRange(ByteKeyRange.of(
          ByteKey.copyFrom(startInclusive),
          ByteKey.copyFrom(endExclusive)));
    }

    private static Range<ByteKey> toRange(ByteKeyRange range) {
      if (range.getStartKey().isEmpty() && range.getEndKey().isEmpty()) {
        return Range.all();
      } else if (range.getStartKey().isEmpty()) {
        return Range.lessThan(range.getEndKey());
      } else if (range.getEndKey().isEmpty()) {
        return Range.atLeast(range.getStartKey());
      } else {
        return Range.closedOpen(range.getStartKey(), range.getEndKey());
      }
    }

    private static ByteKeyRange toRange(Range<ByteKey> range) {
      if (range.hasLowerBound() && range.hasUpperBound()) {
        Preconditions.checkArgument(range.lowerBoundType() == BoundType.CLOSED,
            "lower bound must be closed");
        Preconditions.checkArgument(range.upperBoundType() == BoundType.OPEN,
            "upper bound must be open");
        return ByteKeyRange.of(range.lowerEndpoint(), range.upperEndpoint());
      } else if (range.hasLowerBound()) {
        Preconditions.checkArgument(range.lowerBoundType() == BoundType.CLOSED,
            "lower bound must be closed");
        return ByteKeyRange.of(range.lowerEndpoint(), ByteKey.EMPTY);
      } else if (range.hasUpperBound()) {
        Preconditions.checkArgument(range.upperBoundType() == BoundType.OPEN,
            "upper bound must be open");
        return ByteKeyRange.of(ByteKey.EMPTY, range.upperEndpoint());
      } else {
        return ByteKeyRange.ALL_KEYS;
      }
    }

    @ProcessElement
    public void processElement(
        @Element RegionConfig regionConfig,
        OutputReceiver<Void> outputReceiver,
        RestrictionTracker<ByteKeyRange, ByteKey> tracker)
        throws Exception {

      RangeSet<ByteKey> rangesToRead = TreeRangeSet.create();
      rangesToRead.add(toRange(tracker.currentRestriction()));

      RangeSet<ByteKey> completedRanges = readCompletedRanges(regionConfig.getRegionInfo());
      rangesToRead.removeAll(completedRanges);

      for (Range<ByteKey> range : rangesToRead.asRanges()) {
        if (processRange(regionConfig, outputReceiver, tracker, range)) {
          break;
        }
      }
      tracker.tryClaim(ByteKey.EMPTY);
    }

    private boolean processRange(RegionConfig regionConfig, OutputReceiver<Void> outputReceiver,
        RestrictionTracker<ByteKeyRange, ByteKey> tracker, Range<ByteKey> range) throws Exception {
      byte[] lastRow = null;
      boolean entireRangeProcessed = true;
      ByteKeyRange bkRange = toRange(range);
      try (HBaseRegionScanner scanner = newScanner(regionConfig,
          bkRange.getStartKey().getBytes(),
          bkRange.getEndKey().getBytes())) {
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
          if (tracker.tryClaim(ByteKey.copyFrom(result.getRow()))) {
            lastRow = result.getRow();
            KV<String, Iterable<Mutation>> mutation =
                createMutationsFn.processElement(KV.of(regionConfig.getSnapshotConfig(), result));
            if (mutation != null) {
              writeFn.processElement(mutation, null);
            }
            outputReceiver.output(null);
          } else {
            entireRangeProcessed = false;
            break;
          }
        }
      }

      if (entireRangeProcessed) {
        // we exited "organically" and claimed the whole range. We may have not actually seen a row
        // at the end key though, so bump the end to what we had in the initial scan range.
        lastRow = bkRange.getEndKey().getBytes();
      } else {
        // otherwise we couldn't claim the next row.
        // lastRow is inclusive (we read it), so change the end to be exclusive by adding one

        // lastRow may be null if we never claimed anything
        if (lastRow == null) {
          return true;
        }
        // add a 0 to the end of the rowkey to make it exclusive
        lastRow = Arrays.copyOf(lastRow, lastRow.length + 1);
      }

      RangeSet<ByteKey> ranges = regionProgress.computeIfAbsent(
          regionConfig.getRegionInfo(), noop -> TreeRangeSet.create());
      ranges.add(toRange(ByteKeyRange.of(bkRange.getStartKey(), ByteKey.copyFrom(lastRow))));

      return entireRangeProcessed;
    }

    /**
     * Scans each region for given key range and constructs a ClientSideRegionScanner instance
     *
     * @param regionConfig - HBase Region Configuration
     * @param byteKeyRange - Key range covering start and end row key
     * @return
     * @throws Exception
     */
    private HBaseRegionScanner newScanner(RegionConfig regionConfig, byte[] startKey, byte[] endKey)
        throws Exception {
      Scan scan =
          new Scan()
              // Limit scan to split range
              .withStartRow(startKey)
              .withStopRow(endKey)
              .setIsolationLevel(IsolationLevel.READ_UNCOMMITTED)
              .setCacheBlocks(false);

      SnapshotConfig snapshotConfig = regionConfig.getSnapshotConfig();

      Path sourcePath = snapshotConfig.getSourcePath();
      Path restorePath = snapshotConfig.getRestorePath();
      Configuration configuration = snapshotConfig.getConfiguration();
      FileSystem fileSystem = sourcePath.getFileSystem(configuration);

      return new HBaseRegionScanner(
          configuration,
          fileSystem,
          restorePath,
          regionConfig.getTableDescriptor(),
          regionConfig.getRegionInfo(),
          scan);
    }

    @GetInitialRestriction
    public ByteKeyRange getInitialRange(@Element RegionConfig regionConfig) {
      return ByteKeyRange.of(
          ByteKey.copyFrom(regionConfig.getRegionInfo().getStartKey()),
          ByteKey.copyFrom(regionConfig.getRegionInfo().getEndKey()));
    }

    @GetSize
    public double getSize(@Element RegionConfig regionConfig) {
      return BYTES_PER_SPLIT;
    }

    @NewTracker
    public HbaseRegionSplitTracker newTracker(
        @Element RegionConfig regionConfig, @Restriction ByteKeyRange range) {
      return new HbaseRegionSplitTracker(
          regionConfig.getSnapshotConfig().getSnapshotName(),
          regionConfig.getRegionInfo().getEncodedName(),
          range,
          useDynamicSplitting);
    }

    @SplitRestriction
    public void splitRestriction(
        @Element RegionConfig regionConfig,
        @Restriction ByteKeyRange range,
        OutputReceiver<ByteKeyRange> outputReceiver) {
      try {
        int numSplits = getSplits(regionConfig.getRegionSize());
        LOG.info(
            "Splitting Initial Restriction for SnapshotName: {} - regionname:{} - regionsize(GB):{} - Splits: {}",
            regionConfig.getSnapshotConfig().getSnapshotName(),
            regionConfig.getRegionInfo().getEncodedName(),
            (double) regionConfig.getRegionSize() / BYTES_PER_GB,
            numSplits);
        if (numSplits > 1) {
          RegionSplitter.UniformSplit uniformSplit = new RegionSplitter.UniformSplit();
          byte[][] splits =
              uniformSplit.split(
                  range.getStartKey().getBytes(),
                  range.getEndKey().getBytes(),
                  getSplits(regionConfig.getRegionSize()),
                  true);
          for (int i = 0; i < splits.length - 1; i++)
            outputReceiver.output(
                ByteKeyRange.of(ByteKey.copyFrom(splits[i]), ByteKey.copyFrom(splits[i + 1])));
        } else {
          outputReceiver.output(range);
        }
      } catch (Exception ex) {
        LOG.warn(
            "Unable to split range for region:{} in Snapshot:{}",
            regionConfig.getRegionInfo().getEncodedName(),
            regionConfig.getSnapshotConfig().getSnapshotName());
        outputReceiver.output(range);
      }
    }

    private int getSplits(long sizeInBytes) {
      return (int) Math.ceil((double) sizeInBytes / BYTES_PER_SPLIT);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.add(
          DisplayData.item("DynamicSplitting", useDynamicSplitting ? "Enabled" : "Disabled")
              .withLabel("Dynamic Splitting"));
    }
  }

  /**
   * A {@link DoFn} class for converting Hbase {@link org.apache.hadoop.hbase.client.Result} to list
   * of Hbase {@link org.apache.hadoop.hbase.client.Mutation}s
   */
  static class CreateMutationsFn implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(CreateMutationsFn.class);
    private final int maxMutationsPerRequestThreshold;

    private final boolean filterLargeRows;

    private final long filterLargeRowThresholdBytes;

    private final boolean filterLargeCells;

    private final int filterLargeCellThresholdBytes;

    private final boolean filterLargeRowKeys;

    private final int filterLargeRowKeysThresholdBytes;

    public CreateMutationsFn(
        int maxMutationsPerRequestThreshold,
        boolean filterLargeRows,
        long filterLargeRowThresholdBytes,
        boolean filterLargeCells,
        int filterLargeCellThresholdBytes,
        boolean filterLargeRowKeys,
        int filterLargeRowKeysThresholdBytes) {

      this.maxMutationsPerRequestThreshold = maxMutationsPerRequestThreshold;

      this.filterLargeRows = filterLargeRows;
      this.filterLargeRowThresholdBytes = filterLargeRowThresholdBytes;

      this.filterLargeCells = filterLargeCells;
      this.filterLargeCellThresholdBytes = filterLargeCellThresholdBytes;

      this.filterLargeRowKeys = filterLargeRowKeys;
      this.filterLargeRowKeysThresholdBytes = filterLargeRowKeysThresholdBytes;
    }

    public KV<String, Iterable<Mutation>> processElement(KV<SnapshotConfig, Result> element)
        throws IOException {
      if (element.getValue().listCells().isEmpty()) return null;
      String targetTable = element.getKey().getTableName();

      // Limit the number of mutations per Put (server will reject >= 100k mutations per Put)
      byte[] rowKey = element.getValue().getRow();
      List<Mutation> mutations = new ArrayList<>();

      boolean logAndSkipIncompatibleRowMutations =
          verifyRowMutationThresholds(rowKey, element.getValue().listCells(), mutations);

      if (!logAndSkipIncompatibleRowMutations && mutations.size() > 0) {
         return KV.of(targetTable, mutations);
      } else {
        return null;
      }
    }

    private boolean verifyRowMutationThresholds(
        byte[] rowKey, List<Cell> cells, List<Mutation> mutations) throws IOException {
      boolean logAndSkipIncompatibleRows = false;

      Put put = null;
      int cellCount = 0;
      long totalByteSize = 0L;

      // create mutations
      for (Cell cell : cells) {
        totalByteSize += cell.heapSize();

        // handle large cells
        if (filterLargeCells && cell.getValueLength() > filterLargeCellThresholdBytes) {
          // TODO add config name in log
          LOG.warn(
              "Dropping mutation, cell value length, "
                  + cell.getValueLength()
                  + ", exceeds filter length, "
                  + filterLargeCellThresholdBytes
                  + ", cell: "
                  + cell
                  + ", row key: "
                  + Bytes.toStringBinary(rowKey));
          continue;
        }

        // Split the row into multiple mutations if mutations exceeds threshold limit
        if (cellCount % maxMutationsPerRequestThreshold == 0) {
          cellCount = 0;
          put = new Put(rowKey);
          mutations.add(put);
        }
        put.add(cell);
        cellCount++;
      }

      // TODO add config name in log
      if (filterLargeRows && totalByteSize > filterLargeRowThresholdBytes) {
        logAndSkipIncompatibleRows = true;
        LOG.warn(
            "Dropping row, row length, "
                + totalByteSize
                + ", exceeds filter length threshold, "
                + filterLargeRowThresholdBytes
                + ", row key: "
                + Bytes.toStringBinary(rowKey));
      }

      // TODO add config name in log
      if (filterLargeRowKeys && rowKey.length > filterLargeRowKeysThresholdBytes) {
        logAndSkipIncompatibleRows = true;
        LOG.warn(
            "Dropping row, row key length, "
                + rowKey.length
                + ", exceeds filter length threshold, "
                + filterLargeRowKeysThresholdBytes
                + ", row key: "
                + Bytes.toStringBinary(rowKey));
      }

      return logAndSkipIncompatibleRows;
    }
  }


  /**
   * A workalike for {@link org.apache.hadoop.hbase.client.ClientSideRegionScanner}.
   *
   * <p>It serves the same purpose, but skips block and mobFile cache initialization.
   * Those caches dont appear to useful for the import job and leak threads on shutdown
   */
  static class HBaseRegionScanner implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseRegionScanner.class);

    private HRegion region;
    private RegionScanner scanner;
    private final List<Cell> values;
    boolean hasMore = true;

    public HBaseRegionScanner(
        Configuration conf,
        FileSystem fs,
        Path rootDir,
        TableDescriptor htd,
        RegionInfo hri,
        Scan scan)
        throws IOException {
      scan.setIsolationLevel(IsolationLevel.READ_UNCOMMITTED);
      htd = TableDescriptorBuilder.newBuilder(htd).setReadOnly(true).build();
      this.region =
          HRegion.newHRegion(
              CommonFSUtils.getTableDir(rootDir, htd.getTableName()),
              (WAL) null,
              fs,
              conf,
              hri,
              htd,
              (RegionServerServices) null);
      this.region.setRestoredRegion(true);
      conf.set("hfile.block.cache.policy", "IndexOnlyLRU");
      conf.setIfUnset("hfile.onheap.block.cache.fixed.size", String.valueOf(33554432L));
      conf.unset("hbase.bucketcache.ioengine");

      this.region.initialize();
      this.scanner = this.region.getScanner(scan);
      this.values = new ArrayList();

      this.region.startRegionOperation();
    }

    public void close() {
      if (this.scanner != null) {
        try {
          this.scanner.close();
          this.scanner = null;
        } catch (IOException var3) {
          LOG.warn("Exception while closing scanner", var3);
        }
      }

      if (this.region != null) {
        try {
          this.region.closeRegionOperation();
          this.region.close(true);
          this.region = null;
        } catch (IOException var2) {
          LOG.warn("Exception while closing region", var2);
        }
      }
    }

    public Result next() throws IOException {
      do {
        if (!this.hasMore) {
          return null;
        }

        this.values.clear();
        this.hasMore = this.scanner.nextRaw(this.values);
      } while (this.values.isEmpty());

      Result result = Result.create(this.values);

      return result;
    }
  }
}
