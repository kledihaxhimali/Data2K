package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.singletons.SystemConfigurationSingleton;
import de.ddm.structures.InclusionDependency;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import java.io.File;
import java.util.*;

public class DependencyMiner extends AbstractBehavior<DependencyMiner.Message> {

    ////////////////////
    // Actor Messages //
    ////////////////////

    public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage { }

    @NoArgsConstructor
    public static class StartMessage implements Message { private static final long serialVersionUID = -1963913294517850454L; }

    @Getter @NoArgsConstructor @AllArgsConstructor
    public static class HeaderMessage implements Message {
        private static final long serialVersionUID = -5322425954432915838L;
        int id; String[] header;
    }

    @Getter @NoArgsConstructor @AllArgsConstructor
    public static class BatchMessage implements Message {
        private static final long serialVersionUID = 4591192372652568030L;
        int id; List<String[]> batch;
    }

    @Getter @NoArgsConstructor @AllArgsConstructor
    public static class RegistrationMessage implements Message {
        private static final long serialVersionUID = -4025238529984914107L;
        ActorRef<DependencyWorker.Message> dependencyWorker;
        ActorRef<LargeMessageProxy.Message> workerLargeMessageProxy;
    }

    @Getter @NoArgsConstructor @AllArgsConstructor
    public static class CompletionMessage implements Message {
        private static final long serialVersionUID = -7642425159675583598L;
        ActorRef<DependencyWorker.Message> dependencyWorker; int result;
    }

    @Getter @NoArgsConstructor @AllArgsConstructor
    public static class PartitionChunk implements Message {
        private static final long serialVersionUID = 1L;
        long taskId; int fileId; int columnIndex; int seqNo; String[] values;
    }

    @Getter @NoArgsConstructor @AllArgsConstructor
    public static class RequestColumnValues implements Message, DependencyWorker.Message {
        private static final long serialVersionUID = 2L;
        int fileId; int columnIndex; ActorRef<LargeMessageProxy.Message> minerProxy;
    }

    @Getter @NoArgsConstructor @AllArgsConstructor
    public static class ColumnValuesMessage implements Message, DependencyWorker.Message {
        private static final long serialVersionUID = 3L;
        int fileId; int columnIndex; String[] distinctValues;
    }

    @Getter @NoArgsConstructor @AllArgsConstructor
    public static class CheckIndTask implements Message, DependencyWorker.Message {
        private static final long serialVersionUID = 4L;
        int leftFile, leftCol; int rightFile, rightCol; String[] rightValues; ActorRef<LargeMessageProxy.Message> minerProxy;
    }

    @Getter @NoArgsConstructor @AllArgsConstructor
    public static class IndCheckResult implements Message, DependencyWorker.Message {
        private static final long serialVersionUID = 5L;
        int leftFile, leftCol, rightFile, rightCol; boolean holds;
    }

    private static final class ColKey {
        final int f, c; ColKey(int f,int c){ this.f=f; this.c=c; }
        @Override public int hashCode(){ return (f*31)^c; }
        @Override public boolean equals(Object o){ if (!(o instanceof ColKey k)) return false; return k.f==f && k.c==c; }
        @Override public String toString(){ return f+":"+c; }
    }
    private final Map<ColKey, ActorRef<DependencyWorker.Message>> owner = new HashMap<>();

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "dependencyMiner";
    public static final ServiceKey<DependencyMiner.Message> dependencyMinerService =
            ServiceKey.create(DependencyMiner.Message.class, DEFAULT_NAME + "Service");

    public static Behavior<Message> create() { return Behaviors.setup(DependencyMiner::new); }

    private DependencyMiner(ActorContext<Message> context) {
        super(context);
        this.discoverNaryDependencies = SystemConfigurationSingleton.get().isHardMode();
        this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
        this.headerLines = new String[this.inputFiles.length][];
        this.inputReaders = new ArrayList<>(inputFiles.length);
        for (int id = 0; id < this.inputFiles.length; id++)
            this.inputReaders.add(context.spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));
        this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
        this.largeMessageProxy = this.getContext().spawn(
                LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast(), false),
                LargeMessageProxy.DEFAULT_NAME
        );
        this.dependencyWorkers = new ArrayList<>();
        this.chunkQueue = new ArrayDeque<>();
        this.fileDone = new boolean[this.inputFiles.length];
        this.cursorsByFile = new ArrayList<>(this.inputFiles.length);
        for (int f = 0; f < this.inputFiles.length; f++) this.cursorsByFile.add(new ArrayList<>());
        this.nextChunkId = 1L;
        context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));
    }

    /////////////////
    // Actor State //
    /////////////////

    private long startTime;
    private final boolean discoverNaryDependencies;
    private final File[] inputFiles;
    private final String[][] headerLines;
    private final List<ActorRef<InputReader.Message>> inputReaders;
    private final ActorRef<ResultCollector.Message> resultCollector;
    private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;
    private final List<ActorRef<DependencyWorker.Message>> dependencyWorkers;
    private static final int CHUNK_SIZE = 8_192;
    private final Map<ActorRef<DependencyWorker.Message>, ActorRef<LargeMessageProxy.Message>> workerProxies = new HashMap<>();

    private static class ColumnCursor {
        final int fileId; final int columnIndex; int seqNo = 0; String[] buffer= new String[CHUNK_SIZE]; int fill = 0;
        ColumnCursor(int fileId,int columnIndex){this.fileId=fileId;this.columnIndex=columnIndex;}
    }

    @Getter @NoArgsConstructor @AllArgsConstructor
    public static class InstallColumnValues implements Message, DependencyWorker.Message {
        private static final long serialVersionUID = 6L;
        int fileId; int columnIndex; String[] values;
    }

    private final List<List<ColumnCursor>> cursorsByFile;
    private final Deque<PartitionChunk> chunkQueue;
    private final boolean[] fileDone;
    private long nextChunkId;
    private boolean startedReading = false;
    private final Deque<ActorRef<DependencyWorker.Message>> idleWorkers = new ArrayDeque<>();
    private long totalProduced = 0, totalDispatched = 0;
    private final Map<ColKey, java.util.Set<ActorRef<DependencyWorker.Message>>> owners = new java.util.HashMap<>();
    private boolean finishingTriggered = false;
    private final Map<ColKey, java.util.Set<String>> columnValueSets = new java.util.HashMap<>();
    private final Map<ColKey,String[]> columnValues = new java.util.HashMap<>();
    private long pendingValueExports = 0;
    private long pendingChecks = 0;
    private final Map<ColKey, ActorRef<DependencyWorker.Message>> canonicalOwners = new java.util.HashMap<>();
    private final Map<ColKey, String[]> columnSamples = new java.util.HashMap<>();
    private static final int SAMPLE_SIZE = 64;
    private int nextCanonicalIndex = 0;



    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(StartMessage.class, this::handle)
                .onMessage(BatchMessage.class, this::handle)
                .onMessage(HeaderMessage.class, this::handle)
                .onMessage(RegistrationMessage.class, this::handle)
                .onMessage(CompletionMessage.class, this::handle)
                .onMessage(ColumnValuesMessage.class, this::handle)
                .onMessage(IndCheckResult.class, this::handle)
                .onSignal(Terminated.class, this::handle)
                .build();
    }

    private Behavior<Message> handle(StartMessage m) {
        if (this.dependencyWorkers.isEmpty()) {
            this.getContext().getLog().info("Master started; waiting for worker registration before reading...");
        } else startReading();
        return this;
    }

    private Behavior<Message> handle(HeaderMessage message) {
        this.headerLines[message.getId()] = message.getHeader();
        final int fileId = message.getId();
        this.cursorsByFile.get(fileId).clear();
        if (message.getHeader()!=null)
            for (int c = 0; c < message.getHeader().length; c++) this.cursorsByFile.get(fileId).add(new ColumnCursor(fileId,c));
        return this;
    }

    private Behavior<Message> handle(BatchMessage message) {
        final int fileId = message.getId();
        final List<String[]> rows = message.getBatch();
        if (rows.isEmpty()) {
            this.fileDone[fileId] = true;
            flushAllCursorsOfFile(fileId);
            tryFinalizeOrStartInd();
            return this;
        }
        final List<ColumnCursor> cursors = this.cursorsByFile.get(fileId);
        if (cursors.isEmpty() && this.headerLines[fileId] != null)
            for (int c = 0; c < this.headerLines[fileId].length; c++) cursors.add(new ColumnCursor(fileId,c));
        for (String[] row : rows) {
            if (row == null) continue;
            for (int c = 0; c < Math.min(row.length, cursors.size()); c++) {
                ColumnCursor cur = cursors.get(c);
                String v = row[c];
                cur.buffer[cur.fill++] = v;
                if (cur.fill == CHUNK_SIZE) enqueueChunk(cur);
            }
        }
        this.inputReaders.get(fileId).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf(), 10_000));
        return this;
    }

    private Behavior<Message> handle(RegistrationMessage message) {
        ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
        if (!this.dependencyWorkers.contains(dependencyWorker)) {
            this.dependencyWorkers.add(dependencyWorker);
            this.workerProxies.put(dependencyWorker, message.getWorkerLargeMessageProxy());
            this.getContext().watch(dependencyWorker);
            this.getContext().getLog().info("Registered worker {} (proxy={})", dependencyWorker, message.getWorkerLargeMessageProxy());
            if (!startedReading) startReading();
            if (!sendNextChunkTo(dependencyWorker)) this.idleWorkers.addLast(dependencyWorker);
        }
        return this;
    }

    private Behavior<Message> handle(CompletionMessage message) {
        ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
        sendNextChunkTo(dependencyWorker);
        tryFinalizeOrStartInd();
        return this;
    }

    private Behavior<Message> handle(ColumnValuesMessage m) {
        ColKey key = new ColKey(m.getFileId(), m.getColumnIndex());
        java.util.Set<String> set =
                columnValueSets.computeIfAbsent(key, k -> new java.util.HashSet<>());
        String[] vals = m.getDistinctValues();
        if (vals != null) {
            for (String v : vals) {
                if (v == null) continue;
                v = v.trim();
                if (!v.isEmpty())
                    set.add(v);
            }
        }
        pendingValueExports--;
        if (pendingValueExports == 0) {
            redistributeColumnsToCanonicalOwnersAndDispatchIndChecks();
        }
        return this;
    }

    private Behavior<Message> handle(IndCheckResult m) {
        if (m.isHolds()) {
            java.util.List<InclusionDependency> one =
                    java.util.Collections.singletonList(
                            toIND(m.getLeftFile(), m.getLeftCol(), m.getRightFile(), m.getRightCol())
                    );
            this.resultCollector.tell(new ResultCollector.ResultMessage(one));
        }
        pendingChecks--;
        if (pendingChecks == 0) finalizeRun();
        return this;
    }

    private void finalizeRun() {
        if (finishingTriggered) return;
        finishingTriggered = true;
        this.resultCollector.tell(new ResultCollector.FinalizeMessage());
        long discoveryTime = System.currentTimeMillis() - this.startTime;
        this.getContext().getLog().info("Finished mining within {} ms!", discoveryTime);
    }

    private Behavior<Message> handle(Terminated signal) {
        ActorRef<DependencyWorker.Message> dead = signal.getRef().unsafeUpcast();
        this.dependencyWorkers.remove(dead);
        owners.values().forEach(set -> set.remove(dead));
        owners.entrySet().removeIf(e -> e.getValue().isEmpty());
        if (!this.idleWorkers.isEmpty() && !this.chunkQueue.isEmpty())
            sendNextChunkTo(this.idleWorkers.pollFirst());
        return this;
    }

    private void enqueueChunk(ColumnCursor cur) {
        final String[] payload = java.util.Arrays.copyOf(cur.buffer, cur.fill);
        final long id = nextChunkId++;
        final PartitionChunk chunk = new PartitionChunk(id, cur.fileId, cur.columnIndex, cur.seqNo++, payload);
        this.chunkQueue.addLast(chunk); this.totalProduced++;
        if (!this.idleWorkers.isEmpty()) sendNextChunkTo(this.idleWorkers.pollFirst());
        cur.fill = 0;
    }

    private void flushAllCursorsOfFile(int fileId) {
        final List<ColumnCursor> cursors = this.cursorsByFile.get(fileId);
        for (ColumnCursor cur : cursors) if (cur.fill > 0) enqueueChunk(cur);
    }

    private boolean allFilesDone() { for (boolean d : this.fileDone) if (!d) return false; return true; }
    private boolean allChunksConsumed() { return this.chunkQueue.isEmpty(); }
    private void tryFinalizeOrStartInd() {
        if (allFilesDone() && allChunksConsumed()) {
            startCollectingColumnValues();
        }
    }

    private boolean sendNextChunkTo(ActorRef<DependencyWorker.Message> worker) {
        PartitionChunk picked = null;
        boolean claimedNow = false;
        boolean steal = false;

        Iterator<PartitionChunk> it = this.chunkQueue.iterator();
        while (it.hasNext()) {
            PartitionChunk ch = it.next();
            ColKey key = new ColKey(ch.getFileId(), ch.getColumnIndex());
            java.util.Set<ActorRef<DependencyWorker.Message>> ws = owners.get(key);
            if (ws != null) {
                ws.removeIf(w -> !this.dependencyWorkers.contains(w));
                if (ws.isEmpty()) {
                    owners.remove(key);
                    ws = null;
                }
            }
            if (ws == null || ws.contains(worker)) {
                if (ws == null) {
                    ws = new java.util.HashSet<>();
                    owners.put(key, ws);
                }
                if (ws.add(worker)) {
                    claimedNow = true;
                }
                picked = ch;
                it.remove();
                break;
            }
        }

        if (picked == null && allFilesDone() && !this.chunkQueue.isEmpty()) {
            it = this.chunkQueue.iterator();
            if (it.hasNext()) {
                PartitionChunk ch = it.next();
                it.remove();
                ColKey key = new ColKey(ch.getFileId(), ch.getColumnIndex());
                java.util.Set<ActorRef<DependencyWorker.Message>> ws =
                        owners.computeIfAbsent(key, k -> new java.util.HashSet<>());
                steal = ws.add(worker);
                picked = ch;
            }
        }
        if (picked == null) {
            this.idleWorkers.addLast(worker);
            return false;
        }
        ActorRef<LargeMessageProxy.Message> workerProxy = this.workerProxies.get(worker);
        DependencyWorker.TaskMessage payload = new DependencyWorker.TaskMessage(this.largeMessageProxy, picked);
        this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(payload, workerProxy));
        this.totalDispatched++;
        return true;
    }

    private void startReading() {
        if (startedReading) return;
        this.getContext().getLog().info("Starting to read input (workers registered).");
        for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
            inputReader.tell(new InputReader.ReadHeaderMessage(this.getContext().getSelf()));
        for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
            inputReader.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf(), 10_000));
        this.startTime = System.currentTimeMillis();
        startedReading = true;
    }

    private void startCollectingColumnValues() {
        if (pendingValueExports > 0 || !columnValueSets.isEmpty())
            return;
        java.util.List<ColKey> columns = new java.util.ArrayList<>();
        for (int f = 0; f < headerLines.length; f++) {
            String[] hdr = headerLines[f];
            if (hdr == null) continue;
            for (int c = 0; c < hdr.length; c++)
                columns.add(new ColKey(f, c));
        }
        for (ColKey k : columns) {
            java.util.Set<ActorRef<DependencyWorker.Message>> ws = owners.get(k);
            if (ws == null || ws.isEmpty()) {
                columnValueSets.putIfAbsent(k, new java.util.HashSet<>());
                continue;
            }
            for (ActorRef<DependencyWorker.Message> w : ws) {
                pendingValueExports++;
                RequestColumnValues req = new RequestColumnValues(k.f, k.c, this.largeMessageProxy);
                this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(req, this.workerProxies.get(w)));
            }
        }
        if (pendingValueExports == 0) {
            redistributeColumnsToCanonicalOwnersAndDispatchIndChecks();
        }
    }

    private void dispatchIndChecks() {
        java.util.List<ColKey> cols = new java.util.ArrayList<>(columnValues.keySet());
        long checks = 0L;

        for (int i = 0; i < cols.size(); i++) {
            ColKey a = cols.get(i);
            String[] leftArr = columnValues.get(a);
            if (leftArr == null || leftArr.length == 0) continue;

            ActorRef<DependencyWorker.Message> w = canonicalOwners.get(a);
            if (w == null) continue;
            ActorRef<LargeMessageProxy.Message> workerProxy = this.workerProxies.get(w);
            java.util.Set<String> leftSet = columnValueSets.get(a);
            int leftSize = (leftSet != null) ? leftSet.size() : leftArr.length;

            for (int j = 0; j < cols.size(); j++) {
                if (i == j) continue;
                ColKey b = cols.get(j);

                String[] rightArr = columnValues.get(b);
                if (rightArr == null || rightArr.length == 0) continue;

                java.util.Set<String> rightSet = columnValueSets.get(b);
                int rightSize = (rightSet != null) ? rightSet.size() : rightArr.length;

                if (leftSize > rightSize) continue;

                String[] leftSample = columnSamples.get(a);
                if (leftSample != null && rightSet != null) {
                    boolean impossible = false;
                    int limit = Math.min(leftSample.length, SAMPLE_SIZE);
                    for (int s = 0; s < limit; s++) {
                        String v = leftSample[s];
                        if (!rightSet.contains(v)) {
                            impossible = true;
                            break;
                        }
                    }
                    if (impossible) {
                        continue;
                    }
                }
                CheckIndTask task = new CheckIndTask(a.f, a.c, b.f, b.c, rightArr, this.largeMessageProxy);
                this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(task, workerProxy));
                checks++;
            }
        }
        pendingChecks = checks;
        this.getContext().getLog().info("Dispatched {} IND checks to workers.", pendingChecks);
        if (pendingChecks == 0) finalizeRun();
    }

    private InclusionDependency toIND(int leftFile, int leftCol, int rightFile, int rightCol) {
        java.io.File depFile = this.inputFiles[leftFile];
        java.io.File refFile = this.inputFiles[rightFile];
        String depAttr = (this.headerLines[leftFile] != null && leftCol >= 0 && leftCol < this.headerLines[leftFile].length)
                ? this.headerLines[leftFile][leftCol] : "col" + leftCol;
        String refAttr = (this.headerLines[rightFile] != null && rightCol >= 0 && rightCol < this.headerLines[rightFile].length)
                ? this.headerLines[rightFile][rightCol] : "col" + rightCol;
        return new InclusionDependency(depFile, new String[]{depAttr}, refFile, new String[]{refAttr});
    }

    private void redistributeColumnsToCanonicalOwnersAndDispatchIndChecks() {

        if (this.dependencyWorkers.isEmpty()) {
            this.getContext().getLog().warn(
                    "No workers registered for IND phase – skipping IND checks."
            );
            finalizeRun();
            return;
        }

        for (var e : columnValueSets.entrySet()) {
            ColKey k = e.getKey();
            java.util.Set<String> set = e.getValue();

            ActorRef<DependencyWorker.Message> canonical =
                    this.dependencyWorkers.get(this.nextCanonicalIndex);
            this.nextCanonicalIndex =
                    (this.nextCanonicalIndex + 1) % this.dependencyWorkers.size();

            canonicalOwners.put(k, canonical);

            String[] full = set.toArray(new String[0]);
            columnValues.put(k, full);
            
            int sampleLen = Math.min(SAMPLE_SIZE, full.length);
            if (sampleLen > 0) {
                String[] sample = new String[sampleLen];
                System.arraycopy(full, 0, sample, 0, sampleLen);
                columnSamples.put(k, sample);
            }

            ActorRef<LargeMessageProxy.Message> proxy = this.workerProxies.get(canonical);
            InstallColumnValues msg = new InstallColumnValues(k.f, k.c, full);
            this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(msg, proxy));
        }

        this.getContext().getLog().info(
                "Installed full column values on canonical workers for {} columns. Starting IND checks...",
                canonicalOwners.size()
        );
        dispatchIndChecks();
    }

}
