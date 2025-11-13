//package de.ddm.actors.profiling;
//
//import akka.actor.typed.ActorRef;
//import akka.actor.typed.Behavior;
//import akka.actor.typed.Terminated;
//import akka.actor.typed.javadsl.AbstractBehavior;
//import akka.actor.typed.javadsl.ActorContext;
//import akka.actor.typed.javadsl.Behaviors;
//import akka.actor.typed.javadsl.Receive;
//import akka.actor.typed.receptionist.Receptionist;
//import akka.actor.typed.receptionist.ServiceKey;
//import de.ddm.actors.patterns.LargeMessageProxy;
//import de.ddm.serialization.AkkaSerializable;
//import de.ddm.singletons.InputConfigurationSingleton;
//import de.ddm.singletons.SystemConfigurationSingleton;
//import de.ddm.structures.InclusionDependency;
//import lombok.AllArgsConstructor;
//import lombok.Getter;
//import lombok.NoArgsConstructor;
//import java.io.File;
//import java.util.*;
//
//public class DependencyMiner extends AbstractBehavior<DependencyMiner.Message> {
//
//	////////////////////
//	// Actor Messages //
//	////////////////////
//
//	public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
//	}
//
//	@NoArgsConstructor
//	public static class StartMessage implements Message {
//		private static final long serialVersionUID = -1963913294517850454L;
//	}
//
//	@Getter
//	@NoArgsConstructor
//	@AllArgsConstructor
//	public static class HeaderMessage implements Message {
//		private static final long serialVersionUID = -5322425954432915838L;
//		int id;
//		String[] header;
//	}
//
//	@Getter
//	@NoArgsConstructor
//	@AllArgsConstructor
//	public static class BatchMessage implements Message {
//		private static final long serialVersionUID = 4591192372652568030L;
//		int id;
//		List<String[]> batch;
//	}
//
//	@Getter
//	@NoArgsConstructor
//	@AllArgsConstructor
//	public static class RegistrationMessage implements Message {
//		private static final long serialVersionUID = -4025238529984914107L;
//		ActorRef<DependencyWorker.Message> dependencyWorker;
//        ActorRef<LargeMessageProxy.Message> workerLargeMessageProxy;
//	}
//
//	@Getter
//	@NoArgsConstructor
//	@AllArgsConstructor
//	public static class CompletionMessage implements Message {
//		private static final long serialVersionUID = -7642425159675583598L;
//		ActorRef<DependencyWorker.Message> dependencyWorker;
//		int result;
//	}
//
//    @Getter
//    @NoArgsConstructor
//    @AllArgsConstructor
//    public static class PartitionChunk implements Message {
//        private static final long serialVersionUID = 1L;
//        long taskId;
//        int fileId;
//        int columnIndex;
//        int seqNo;
//        String[] values;
//    }
//
//    private static final class ColKey {
//        final int f, c;
//        ColKey(int f,int c){ this.f=f; this.c=c; }
//        @Override public int hashCode(){ return (f*31)^c; }
//        @Override public boolean equals(Object o){
//            if (!(o instanceof ColKey k)) return false; return k.f==f && k.c==c;
//        }
//    }
//    private final Map<ColKey, ActorRef<DependencyWorker.Message>> owner = new HashMap<>();
//
//    ////////////////////////
//	// Actor Construction //
//	////////////////////////
//
//	public static final String DEFAULT_NAME = "dependencyMiner";
//	public static final ServiceKey<DependencyMiner.Message> dependencyMinerService = ServiceKey.create(DependencyMiner.Message.class, DEFAULT_NAME + "Service");
//	public static Behavior<Message> create() {
//		return Behaviors.setup(DependencyMiner::new);
//	}
//
//	private DependencyMiner(ActorContext<Message> context) {
//		super(context);
//		this.discoverNaryDependencies = SystemConfigurationSingleton.get().isHardMode();
//		this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
//		this.headerLines = new String[this.inputFiles.length][];
//		this.inputReaders = new ArrayList<>(inputFiles.length);
//		for (int id = 0; id < this.inputFiles.length; id++)
//			this.inputReaders.add(context.spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));
//		this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
//		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast(), false), LargeMessageProxy.DEFAULT_NAME);
//		this.dependencyWorkers = new ArrayList<>();
//        this.chunkQueue = new ArrayDeque<>();
//        this.fileDone = new boolean[this.inputFiles.length];
//        this.cursorsByFile = new ArrayList<>(this.inputFiles.length);
//        for (int f = 0; f < this.inputFiles.length; f++) this.cursorsByFile.add(new ArrayList<>());
//        this.nextChunkId = 1L;
//		context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));
//	}
//
//	/////////////////
//	// Actor State //
//	/////////////////
//
//	private long startTime;
//	private final boolean discoverNaryDependencies;
//	private final File[] inputFiles;
//	private final String[][] headerLines;
//	private final List<ActorRef<InputReader.Message>> inputReaders;
//	private final ActorRef<ResultCollector.Message> resultCollector;
//	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;
//	private final List<ActorRef<DependencyWorker.Message>> dependencyWorkers;
//    private static final int CHUNK_SIZE = 8_192;
//
//    private static class ColumnCursor {
//        final int fileId;
//        final int columnIndex;
//        int seqNo = 0;
//        String[] buffer= new String[CHUNK_SIZE];
//        int fill = 0;
//
//        ColumnCursor(int fileId, int columnIndex) {
//            this.fileId = fileId;
//            this.columnIndex = columnIndex;
//        }
//    }
//
//    private final List<List<ColumnCursor>> cursorsByFile;
//    private final Deque<PartitionChunk> chunkQueue;
//    private final boolean[] fileDone;
//    private long nextChunkId;
//    private boolean startedReading = false;
//    private final java.util.Deque<ActorRef<DependencyWorker.Message>> idleWorkers = new java.util.ArrayDeque<>();
//    private long totalProduced = 0;
//    private long totalDispatched = 0;
//    private final Map<ActorRef<DependencyWorker.Message>, ActorRef<LargeMessageProxy.Message>> workerProxies = new HashMap<>();
//
//    ////////////////////
//	// Actor Behavior //
//	////////////////////
//
//	@Override
//	public Receive<Message> createReceive() {
//		return newReceiveBuilder()
//				.onMessage(StartMessage.class, this::handle)
//				.onMessage(BatchMessage.class, this::handle)
//				.onMessage(HeaderMessage.class, this::handle)
//				.onMessage(RegistrationMessage.class, this::handle)
//				.onMessage(CompletionMessage.class, this::handle)
//				.onSignal(Terminated.class, this::handle)
//				.build();
//	}
//
//	private Behavior<Message> handle(StartMessage message) {
//        if (this.dependencyWorkers.isEmpty()) {
//            this.getContext().getLog().info("Master started; waiting for worker registration before reading...");
//        } else {
//            startReading();
//        }
//        return this;
//	}
//
//	private Behavior<Message> handle(HeaderMessage message) {
//        this.getContext().getLog().info("Header received for file {} with {} columns.",
//                message.getId(),
//                message.getHeader() != null ? message.getHeader().length : 0);
//        this.headerLines[message.getId()] = message.getHeader();
//        final int fileId = message.getId();
//        this.cursorsByFile.get(fileId).clear();
//        if (message.getHeader() != null) {
//            for (int c = 0; c < message.getHeader().length; c++) {
//                this.cursorsByFile.get(fileId).add(new ColumnCursor(fileId, c));
//            }
//        }
//        return this;
//	}
//
//	private Behavior<Message> handle(BatchMessage message) {
//        final int fileId = message.getId();
//        final List<String[]> rows = message.getBatch();
//        if (rows.isEmpty()) {
//            this.getContext().getLog().info("File {} finished reading batches. Flushing remaining data...", fileId);
//            this.fileDone[fileId] = true;
//            flushAllCursorsOfFile(fileId);
//            logPartitionProgressIfDone();
//            return this;
//        }
//        if (nextChunkId % 50 == 0)
//            this.getContext().getLog().info("Miner working... processed {} chunks so far.", nextChunkId);
//        final List<ColumnCursor> cursors = this.cursorsByFile.get(fileId);
//        if (cursors.isEmpty() && this.headerLines[fileId] != null) {
//            for (int c = 0; c < this.headerLines[fileId].length; c++) cursors.add(new ColumnCursor(fileId, c));
//        }
//        for (String[] row : rows) {
//            if (row == null) continue;
//            for (int c = 0; c < Math.min(row.length, cursors.size()); c++) {
//                ColumnCursor cur = cursors.get(c);
//                String v = row[c];
//                cur.buffer[cur.fill++] = v;
//                if (cur.fill == CHUNK_SIZE) {
//                    enqueueChunk(cur);
//                }
//            }
//        }
//        this.inputReaders.get(fileId).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf(), 10_000));
//        return this;
//	}
//
//	private Behavior<Message> handle(RegistrationMessage message) {
//        ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
//        if (!this.dependencyWorkers.contains(dependencyWorker)) {
//            this.dependencyWorkers.add(dependencyWorker);
//            this.workerProxies.put(dependencyWorker, message.getWorkerLargeMessageProxy()); // <-- store proxy
//            this.getContext().watch(dependencyWorker);
//            this.getContext().getLog().info("Registered worker {} (proxy={})", dependencyWorker, message.getWorkerLargeMessageProxy());
//            if (!startedReading) startReading();
//            if (!sendNextChunkTo(dependencyWorker)) {
//                this.idleWorkers.addLast(dependencyWorker);
//            }
//        }
//        return this;
//	}
//
//	private Behavior<Message> handle(CompletionMessage message) {
//        ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
//        this.getContext().getLog().info("Worker acked with result={}. Sending next chunk...", message.getResult());
//        sendNextChunkTo(dependencyWorker);
//        if (System.currentTimeMillis() - this.startTime > 120000)
//            this.end();
//        return this;
//	}
//
//	private void end() {
//		this.resultCollector.tell(new ResultCollector.FinalizeMessage());
//		long discoveryTime = System.currentTimeMillis() - this.startTime;
//		this.getContext().getLog().info("Finished mining within {} ms!", discoveryTime);
//	}
//
//    private Behavior<Message> handle(Terminated signal) {
//        ActorRef<DependencyWorker.Message> dead = signal.getRef().unsafeUpcast();
//        this.dependencyWorkers.remove(dead);
//        owner.entrySet().removeIf(e -> e.getValue().equals(dead));
//        if (!this.idleWorkers.isEmpty() && !this.chunkQueue.isEmpty()) {
//            sendNextChunkTo(this.idleWorkers.pollFirst());
//        }
//        return this;
//    }
//
//    /////////////////////////
//    // Partitioning helpers //
//    /////////////////////////
//
//    private void enqueueChunk(ColumnCursor cur) {
//        final String[] payload = java.util.Arrays.copyOf(cur.buffer, cur.fill);
//        final long id = nextChunkId++;
//        final PartitionChunk chunk =
//                new PartitionChunk(id, cur.fileId, cur.columnIndex, cur.seqNo++, payload);
//        this.chunkQueue.addLast(chunk);
//        this.totalProduced++;
//        if (!this.idleWorkers.isEmpty()) {
//            ActorRef<DependencyWorker.Message> w = this.idleWorkers.pollFirst();
//            sendNextChunkTo(w);
//        }
//        cur.fill = 0;
//        if (cur.buffer.length != CHUNK_SIZE) cur.buffer = new String[CHUNK_SIZE];
//    }
//
//    private void flushAllCursorsOfFile(int fileId) {
//        final List<ColumnCursor> cursors = this.cursorsByFile.get(fileId);
//        for (ColumnCursor cur : cursors) {
//            if (cur.fill > 0) enqueueChunk(cur);
//        }
//    }
//
//    private boolean allFilesDone() {
//        for (boolean d : this.fileDone) if (!d) return false;
//        return true;
//    }
//
//    private void logPartitionProgressIfDone() {
//        if (allFilesDone()) {
//            long remaining = this.chunkQueue.size();
//            this.getContext().getLog().info(
//                    "Partitioning complete: produced={}, dispatched={}, remaining={} (CHUNK_SIZE={})",
//                    this.totalProduced, this.totalDispatched, remaining, CHUNK_SIZE
//            );
//            if (remaining == 0) {
//                this.getContext().getLog().info("✅ All chunks dispatched. Finalizing...");
//                this.resultCollector.tell(new ResultCollector.FinalizeMessage());
//                long discoveryTime = System.currentTimeMillis() - this.startTime;
//                this.getContext().getLog().info("Finished within {} ms!", discoveryTime);
//            } else {
//                this.getContext().getLog().info(
//                        "Waiting for workers to consume remaining {} chunks...", remaining
//                );
//            }
//        } else {
//            this.getContext().getLog().info("Miner still working... waiting for remaining files to finish.");
//        }
//    }
//
//    private boolean sendNextChunkTo(ActorRef<DependencyWorker.Message> worker) {
//        PartitionChunk picked = null;
//        boolean claimedNow = false;
//        boolean steal = false;
//        // Pass 1: honor affinity
//        Iterator<PartitionChunk> it = this.chunkQueue.iterator();
//        while (it.hasNext()) {
//            PartitionChunk ch = it.next();
//            ColKey key = new ColKey(ch.getFileId(), ch.getColumnIndex());
//            ActorRef<DependencyWorker.Message> w = owner.get(key);
//            if (w != null && !this.dependencyWorkers.contains(w)) {
//                owner.remove(key);
//                w = null;
//            }
//            if (w == null || w.equals(worker)) {
//                if (w == null) { owner.put(key, worker); claimedNow = true; }
//                picked = ch; it.remove(); break;
//            }
//        }
//        // Pass 2: allow reassignment (steal) if everything produced
//        if (picked == null && allFilesDone() && !this.chunkQueue.isEmpty()) {
//            it = this.chunkQueue.iterator();
//            if (it.hasNext()) {
//                PartitionChunk ch = it.next(); it.remove();
//                ColKey key = new ColKey(ch.getFileId(), ch.getColumnIndex());
//                owner.put(key, worker);
//                picked = ch;
//                steal = true;
//            }
//        }
//        if (picked == null) {
//            final boolean allDoneNow = allFilesDone() && this.chunkQueue.isEmpty();
//            this.getContext().getLog().info(
//                    "No chunk for {} — parking. produced={} dispatched={} remaining={} allFilesDone={} doneNow={}",
//                    worker, this.totalProduced, this.totalDispatched, this.chunkQueue.size(), allFilesDone(), allDoneNow
//            );
//            if (allDoneNow) {
//                this.getContext().getLog().info("✅ All files done and no chunks remaining. Finalizing...");
//                this.resultCollector.tell(new ResultCollector.FinalizeMessage());
//                long discoveryTime = System.currentTimeMillis() - this.startTime;
//                this.getContext().getLog().info("Finished within {} ms!", discoveryTime);
//            } else {
//                this.idleWorkers.addLast(worker);
//            }
//            return false;
//        }
//        ActorRef<LargeMessageProxy.Message> workerProxy = this.workerProxies.get(worker);
//        DependencyWorker.TaskMessage payload = new DependencyWorker.TaskMessage(this.largeMessageProxy, picked);
//        int len = (picked.getValues() == null) ? 0 : picked.getValues().length;
//        this.getContext().getLog().info(
//                "DISPATCH_VALUES -> worker={} chunk={}",
//                worker, fmtChunk(picked)
//        );
//        this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(payload, workerProxy));
//        this.totalDispatched++;
//        this.getContext().getLog().info(
//                "DISPATCH -> worker={} chunk=[{}] claim={} steal={} produced={} dispatched={} remaining={}",
//                worker, fmtChunk(picked), claimedNow, steal, this.totalProduced, this.totalDispatched, this.chunkQueue.size()
//        );
//        return true;
//    }
//
//    private void startReading() {
//        if (startedReading) return;
//        this.getContext().getLog().info("Starting to read input (workers registered).");
//        for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
//            inputReader.tell(new InputReader.ReadHeaderMessage(this.getContext().getSelf()));
//        for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
//            inputReader.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf(), 10_000));
//        this.startTime = System.currentTimeMillis();
//        startedReading = true;
//    }
//
//    private static String fmtChunk(PartitionChunk ch) {
//        int len = (ch.getValues() == null) ? 0 : ch.getValues().length;
//        return String.format("id=%d file=%d col=%d seq=%d rows=%d",
//                ch.getTaskId(), ch.getFileId(), ch.getColumnIndex(), ch.getSeqNo(), len);
//    }
//}

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

    // === IND collection / orchestration messages ===
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
    private final Map<ActorRef<DependencyWorker.Message>, ActorRef<LargeMessageProxy.Message>> workerProxies =
            new HashMap<>();


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

    // Columns can be processed by multiple workers while streaming
    private final Map<ColKey, java.util.Set<ActorRef<DependencyWorker.Message>>> owners = new java.util.HashMap<>();
    // === IND orchestration state ===
    private boolean finishingTriggered = false;
    // Full, merged values per column (in miner)
    private final Map<ColKey, java.util.Set<String>> columnValueSets = new java.util.HashMap<>();
    // Arrays per column, used as rightValues for IND checks
    private final Map<ColKey,String[]> columnValues = new java.util.HashMap<>();
    // How many RequestColumnValues replies still expected (across all workers/columns)
    private long pendingValueExports = 0;
    // How many IND checks are still in flight
    private long pendingChecks = 0;
    // Canonical owner per column AFTER merging — this is where IND checks will run
    private final Map<ColKey, ActorRef<DependencyWorker.Message>> canonicalOwners = new java.util.HashMap<>();


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
        this.getContext().getLog().info("Header received for file {} with {} columns.",
                message.getId(), message.getHeader()!=null?message.getHeader().length:0);
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
            this.getContext().getLog().info("File {} finished reading batches. Flushing remaining data...", fileId);
            this.fileDone[fileId] = true;
            flushAllCursorsOfFile(fileId);
            tryFinalizeOrStartInd();
            return this;
        }
        if (nextChunkId % 50 == 0)
            this.getContext().getLog().info("Miner working... processed {} chunks so far.", nextChunkId);
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
        this.getContext().getLog().info(
                "Received values for {} (now {} uniques). pendingExports={}",
                key, set.size(), pendingValueExports
        );

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
        // Remove dead worker from all owners sets
        owners.values().forEach(set -> set.remove(dead));
        owners.entrySet().removeIf(e -> e.getValue().isEmpty());
        if (!this.idleWorkers.isEmpty() && !this.chunkQueue.isEmpty())
            sendNextChunkTo(this.idleWorkers.pollFirst());
        return this;
    }


    /////////////////////////
    // Partitioning helpers //
    /////////////////////////

    private void enqueueChunk(ColumnCursor cur) {
        final String[] payload = java.util.Arrays.copyOf(cur.buffer, cur.fill);
        final long id = nextChunkId++;
        final PartitionChunk chunk = new PartitionChunk(id, cur.fileId, cur.columnIndex, cur.seqNo++, payload);
        this.chunkQueue.addLast(chunk); this.totalProduced++;
        if (!this.idleWorkers.isEmpty()) sendNextChunkTo(this.idleWorkers.pollFirst());
        cur.fill = 0; // reuse buffer
    }

    private void flushAllCursorsOfFile(int fileId) {
        final List<ColumnCursor> cursors = this.cursorsByFile.get(fileId);
        for (ColumnCursor cur : cursors) if (cur.fill > 0) enqueueChunk(cur);
    }

    private boolean allFilesDone() { for (boolean d : this.fileDone) if (!d) return false; return true; }
    private boolean allChunksConsumed() { return this.chunkQueue.isEmpty(); }

    private void tryFinalizeOrStartInd() {
        if (allFilesDone() && allChunksConsumed()) {
            this.getContext().getLog().info(
                    "Partitioning complete: produced={} dispatched={} remaining={} — starting IND collection.",
                    this.totalProduced, this.totalDispatched, this.chunkQueue.size()
            );
            startCollectingColumnValues();
        }
    }

    private boolean sendNextChunkTo(ActorRef<DependencyWorker.Message> worker) {
        PartitionChunk picked = null;
        boolean claimedNow = false;
        boolean steal = false;

        // Pass 1: honor affinity (column already owned by worker, or unowned)
        Iterator<PartitionChunk> it = this.chunkQueue.iterator();
        while (it.hasNext()) {
            PartitionChunk ch = it.next();
            ColKey key = new ColKey(ch.getFileId(), ch.getColumnIndex());

            java.util.Set<ActorRef<DependencyWorker.Message>> ws = owners.get(key);
            if (ws != null) {
                // Clean out any workers that have died
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
                // worker may be a new owner
                if (ws.add(worker)) {
                    claimedNow = true;
                }
                picked = ch;
                it.remove();
                break;
            }
        }

        // Pass 2: allow stealing when all input is read but chunks remain
        if (picked == null && allFilesDone() && !this.chunkQueue.isEmpty()) {
            it = this.chunkQueue.iterator();
            if (it.hasNext()) {
                PartitionChunk ch = it.next();
                it.remove();
                ColKey key = new ColKey(ch.getFileId(), ch.getColumnIndex());
                java.util.Set<ActorRef<DependencyWorker.Message>> ws =
                        owners.computeIfAbsent(key, k -> new java.util.HashSet<>());
                steal = ws.add(worker); // worker becomes additional owner
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

        this.getContext().getLog().info(
                "DISPATCH -> worker={} chunk=[id={} file={} col={} seq={} rows={}] claim={} steal={} produced={} dispatched={} remaining={}",
                worker, picked.getTaskId(), picked.getFileId(), picked.getColumnIndex(), picked.getSeqNo(),
                (picked.getValues()==null?0:picked.getValues().length),
                claimedNow, steal, this.totalProduced, this.totalDispatched, this.chunkQueue.size()
        );
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

    /////////////////////////////
    // === IND orchestration ===
    /////////////////////////////

    private void startCollectingColumnValues() {
        if (pendingValueExports > 0 || !columnValueSets.isEmpty())
            return; // already collecting or collected

        // Collect all columns seen in headers
        java.util.List<ColKey> columns = new java.util.ArrayList<>();
        for (int f = 0; f < headerLines.length; f++) {
            String[] hdr = headerLines[f];
            if (hdr == null) continue;
            for (int c = 0; c < hdr.length; c++)
                columns.add(new ColKey(f, c));
        }

        // For each column, ask ALL workers that own it for their partial values
        for (ColKey k : columns) {
            java.util.Set<ActorRef<DependencyWorker.Message>> ws = owners.get(k);
            if (ws == null || ws.isEmpty()) {
                // Column might be empty / never streamed; treat as empty set
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
            // No one had any data; still need to move on to IND checks (will find none)
            redistributeColumnsToCanonicalOwnersAndDispatchIndChecks();
        } else {
            this.getContext().getLog().info(
                    "Requesting column values from {} worker-column combinations.",
                    pendingValueExports
            );
        }
    }


    private void dispatchIndChecks() {
        java.util.List<ColKey> cols = new java.util.ArrayList<>(columnValues.keySet());
        long checks = 0L;

        for (int i = 0; i < cols.size(); i++) {
            ColKey a = cols.get(i);
            String[] leftArr = columnValues.get(a);
            if (leftArr == null || leftArr.length == 0) continue; // empty dependent set

            ActorRef<DependencyWorker.Message> w = canonicalOwners.get(a);
            if (w == null) continue; // no worker to host this column

            ActorRef<LargeMessageProxy.Message> workerProxy = this.workerProxies.get(w);

            for (int j = 0; j < cols.size(); j++) {
                if (i == j) continue; // skip A ⊆ A
                ColKey b = cols.get(j);
                String[] rightArr = columnValues.get(b);
                if (rightArr == null || rightArr.length == 0) continue; // empty reference

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
        // Decide a canonical owner per column and send it the full set
        for (var e : columnValueSets.entrySet()) {
            ColKey k = e.getKey();
            java.util.Set<String> set = e.getValue();

            // Decide canonical owner: just pick the first current owner if any
            java.util.Set<ActorRef<DependencyWorker.Message>> ws = owners.get(k);
            if (ws == null || ws.isEmpty()) {
                // No worker had values; skip
                continue;
            }
            ActorRef<DependencyWorker.Message> canonical = ws.iterator().next();
            canonicalOwners.put(k, canonical);

            // materialize to array and remember for shipping as rightValues
            String[] full = set.toArray(new String[0]);
            columnValues.put(k, full);

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