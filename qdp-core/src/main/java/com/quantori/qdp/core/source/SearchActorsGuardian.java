package com.quantori.qdp.core.source;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.receptionist.Receptionist;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class SearchActorsGuardian {
  private final int maxAmountOfSearchActors;
  private final Map<ActorRef<SearchActor.Command>, LocalDateTime> actorRegistry;

  static Behavior<Void> create(int maxAmountOfSearchActors) {
    return Behaviors.setup(
            (ActorContext<Receptionist.Listing> context) -> {
              context
                  .getSystem()
                  .receptionist()
                  .tell(
                      Receptionist.subscribe(
                          SearchActor.searchActorsKey, context.getSelf().narrow()));

              return new SearchActorsGuardian(maxAmountOfSearchActors).behavior();
            })
        .unsafeCast(); // Void
  }

  private SearchActorsGuardian(int maxAmountOfSearchActors) {
    this.maxAmountOfSearchActors = maxAmountOfSearchActors;
    this.actorRegistry = new HashMap<>(maxAmountOfSearchActors);
  }

  private Behavior<Receptionist.Listing> behavior() {
    return Behaviors.receive(Receptionist.Listing.class)
        .onMessage(Receptionist.Listing.class, this::onListing)
        .build();
  }

  private Behavior<Receptionist.Listing> onListing(Receptionist.Listing msg) {
    log.debug("OnList {}", msg.getServiceInstances(SearchActor.searchActorsKey).size());
    Set<ActorRef<SearchActor.Command>> actorList = msg.getServiceInstances(SearchActor.searchActorsKey)
        .stream()
        .filter(ref -> ref.path().address().getHost().isEmpty())
        .collect(Collectors.toSet());
    updateList(actorList);
    if (actorRegistry.size() > maxAmountOfSearchActors) {
      log.debug("Try terminate");
      getEldest(maxAmountOfSearchActors > 10 ? maxAmountOfSearchActors / 10 : 1)
          .forEach(ref -> {
            log.debug("The search actor will be removed : {}", ref.path());
            ref.tell(new SearchActor.Close(null));
          });
    }

    return Behaviors.same();
  }

  private void updateList(Set<ActorRef<SearchActor.Command>> actorList) {
    Set<ActorRef<SearchActor.Command>> newActorList = new HashSet<>(actorList);
    Set<ActorRef<SearchActor.Command>> oldActorList = actorRegistry.keySet();
    oldActorList.retainAll(newActorList);
    newActorList.removeAll(oldActorList);
    newActorList.forEach(newActorRef -> {
      actorRegistry.put(newActorRef, LocalDateTime.now());
      log.debug("A new search actor was added : {}", newActorRef.path());
    });
  }

  private Set<ActorRef<SearchActor.Command>> getEldest(int topAmount) {
    class Wrapper implements Comparable<Wrapper> {
      final ActorRef<SearchActor.Command> ref;
      final LocalDateTime element;

      Wrapper(ActorRef<SearchActor.Command> ref, LocalDateTime element) {
        this.ref = ref;
        this.element = element;
      }

      @Override
      public int compareTo(Wrapper o) {
        return (-1) * element.compareTo(o.element);
      }
    }

    PriorityQueue<Wrapper> maxHeap = new PriorityQueue<>();
    actorRegistry.entrySet().stream().map(e -> new Wrapper(e.getKey(), e.getValue())).forEach(element -> {
      maxHeap.add(element);

      if (maxHeap.size() > topAmount) {
        maxHeap.poll();
      }
    });
    return maxHeap.stream().map(w -> w.ref).collect(Collectors.toSet());
  }
}
