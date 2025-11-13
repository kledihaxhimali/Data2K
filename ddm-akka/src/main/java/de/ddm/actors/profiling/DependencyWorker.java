package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import java.util.Set;

public class DependencyWorker extends AbstractBehavior<DependencyWorker.Message> {

    ////////////////////
    // Actor Messages //
    ////////////////////

    public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage { }

    @Getter @NoArgsConstructor @AllArgsConstructor
    public static class ReceptionistListingMessage implements Message {
        private static final long serialVersionUID = -5246338806092216222L;
        Receptionist.Listing listing;
    }

    @Getter @NoArgsConstructor @AllArgsConstructor
    public static class TaskMessage implements Message {
        private static final long serialVersionUID = -4667745204456518160L;
        ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
        int task; // legacy (ignored when chunk != null)
        DependencyMiner.PartitionChunk chunk;
        public TaskMessage(ActorRef<LargeMessageProxy.Message> minerProxy, DependencyMiner.PartitionChunk chunk) {
            this.dependencyMinerLargeMessageProxy = minerProxy; this.chunk = chunk; this.task = 0;
        }
    }

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "dependencyWorker";
    public static Behavior<Message> create() { return Behaviors.setup(DependencyWorker::new); }

    private DependencyWorker(ActorContext<Message> context) {
        super(context);
        final ActorRef<Receptionist.Listing> listingResponseAdapter =
                context.messageAdapter(Receptionist.Listing.class, ReceptionistListingMessage::new);
        context.getSystem().receptionist().tell(
                Receptionist.subscribe(DependencyMiner.dependencyMinerService, listingResponseAdapter)
        );
        this.largeMessageProxy = this.getContext().spawn(
                LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast(), false),
                LargeMessageProxy.DEFAULT_NAME
        );
    }

    private static final class ColKey {
        final int f, c; ColKey(int f,int c){ this.f=f; this.c=c; }
        @Override public int hashCode(){ return (f*31)^c; }
        @Override public boolean equals(Object o){ if (!(o instanceof ColKey k)) return false; return k.f==f && k.c==c; }
    }

    private final java.util.Map<ColKey, java.util.HashSet<String>> colSets = new java.util.HashMap<>();

    /////////////////
    // Actor State //
    /////////////////

    private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(ReceptionistListingMessage.class, this::handle)
                .onMessage(TaskMessage.class, this::handle)
                .onMessage(DependencyMiner.RequestColumnValues.class, this::handle)
                .onMessage(DependencyMiner.CheckIndTask.class, this::handle)
                .onMessage(DependencyMiner.InstallColumnValues.class, this::handle)
                .build();
    }

    private Behavior<Message> handle(ReceptionistListingMessage message) {
        Set<ActorRef<DependencyMiner.Message>> dependencyMiners =
                message.getListing().getServiceInstances(DependencyMiner.dependencyMinerService);
        for (ActorRef<DependencyMiner.Message> dependencyMiner : dependencyMiners)
            dependencyMiner.tell(new DependencyMiner.RegistrationMessage(this.getContext().getSelf(), this.largeMessageProxy));
        return this;
    }

    private Behavior<Message> handle(TaskMessage message) {
        if (message.getChunk() != null) {
            var ch  = message.getChunk();
            var key = new ColKey(ch.getFileId(), ch.getColumnIndex());
            var set = colSets.computeIfAbsent(key, k -> new java.util.HashSet<>(8192));
            String[] vals = ch.getValues();
            if (vals != null) {
                for (String v : vals) {
                    if (v == null) continue;
                    v = v.trim();
                    set.add(v);
                }
            }
            LargeMessageProxy.LargeMessage completion =
                    new DependencyMiner.CompletionMessage(this.getContext().getSelf(), ch.getSeqNo());
            this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(completion, message.getDependencyMinerLargeMessageProxy()));
            return this;
        }
        LargeMessageProxy.LargeMessage completionMessage =
                new DependencyMiner.CompletionMessage(this.getContext().getSelf(), message.getTask());
        this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(completionMessage, message.getDependencyMinerLargeMessageProxy()));
        return this;
    }

    private Behavior<Message> handle(DependencyMiner.RequestColumnValues m) {
        var key = new ColKey(m.getFileId(), m.getColumnIndex());
        var set = colSets.getOrDefault(key, new java.util.HashSet<>());
        String[] arr = set.toArray(new String[0]);
        DependencyMiner.ColumnValuesMessage out =
                new DependencyMiner.ColumnValuesMessage(m.getFileId(), m.getColumnIndex(), arr);
        this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(out, m.getMinerProxy()));
        return this;
    }

    private Behavior<Message> handle(DependencyMiner.InstallColumnValues m) {
        var key = new ColKey(m.getFileId(), m.getColumnIndex());
        java.util.HashSet<String> set = new java.util.HashSet<>(
                Math.max(16, (m.getValues() == null ? 0 : m.getValues().length))
        );
        if (m.getValues() != null) {
            for (String v : m.getValues()) {
                if (v == null) continue;
                v = v.trim();
                if (!v.isEmpty()) set.add(v);
            }
        }
        colSets.put(key, set);
        return this;
    }

    private Behavior<Message> handle(DependencyMiner.CheckIndTask task) {
        var leftKey = new ColKey(task.getLeftFile(), task.getLeftCol());
        var left = colSets.getOrDefault(leftKey, new java.util.HashSet<String>());
        java.util.HashSet<String> right = new java.util.HashSet<>(
                Math.max(16, (task.getRightValues() == null ? 0 : task.getRightValues().length))
        );
        if (task.getRightValues() != null) {
            for (String v : task.getRightValues()) {
                if (v == null) continue;
                v = v.trim();
                if (!v.isEmpty()) right.add(v);
            }
        }
        boolean holds;

        if (left.isEmpty()) {
            this.getContext().getLog().warn(
                    "Dependent column values are empty. Validation failed. left={}:{}",
                    task.getLeftFile(), task.getLeftCol()
            );
            holds = false;
        }
        else if (right.isEmpty()) {
            this.getContext().getLog().warn(
                    "Referenced column values are empty. Validation failed. right={}:{}",
                    task.getRightFile(), task.getRightCol()
            );
            holds = false;
        }
        else if (task.getLeftFile() == task.getRightFile()
                && task.getLeftCol() == task.getRightCol()) {
            holds = false;
        }
        else {
            holds = right.containsAll(left);
            if (holds) {
                this.getContext().getLog().info(
                        "IND found: Column '{}' (fileId={}) is a subset of column '{}' (fileId={}). sizes: dep={}, ref={}",
                        task.getLeftCol(), task.getLeftFile(), task.getRightCol(), task.getRightFile(),
                        left.size(), right.size()
                );
            }
        }
        DependencyMiner.IndCheckResult result =
                new DependencyMiner.IndCheckResult(
                        task.getLeftFile(), task.getLeftCol(),
                        task.getRightFile(), task.getRightCol(),
                        holds
                );
        this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(result, task.getMinerProxy()));
        return this;
    }
}