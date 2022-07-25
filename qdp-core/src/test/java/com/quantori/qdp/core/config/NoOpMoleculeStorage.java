//package com.quantori.qdp.core.config;
//
//import co.elastic.clients.elasticsearch.core.SearchRequest;
//import com.quantori.qdp.core.source.model.molecule.Molecule;
//import com.quantori.qdp.core.source.model.molecule.MoleculeIndex;
//import com.quantori.qdp.core.source.model.molecule.MoleculeIndexLoader;
//import com.quantori.qdp.core.source.model.molecule.MoleculePropertyMeta;
//import com.quantori.qdp.core.source.model.molecule.MoleculeSearchBuilder;
//import com.quantori.qdp.core.source.model.molecule.MoleculeStorage;
//import com.quantori.qdp.core.source.model.molecule.StorageSearchRequestAdapter;
//import com.quantori.qdp.core.source.model.molecule.elastic.MoleculeIterable;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.lang.NonNull;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.CopyOnWriteArrayList;
//import java.util.function.Consumer;
//
//import static org.mockito.ArgumentMatchers.anyInt;
//import static org.mockito.ArgumentMatchers.anyList;
//import static org.mockito.Mockito.any;
//import static org.mockito.Mockito.doAnswer;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.when;
//
//public class NoOpMoleculeStorage implements MoleculeStorage {
//  private static final Logger logger = LoggerFactory.getLogger(NoOpMoleculeStorage.class);
//  private final List<StorageSearchRequestAdapter> requests = new CopyOnWriteArrayList<>();
//  private List<String> updatedMolecules = new CopyOnWriteArrayList<>();
//  private Long requiredSize;
//  private MoleculeIterable pages;
//
//  @Override
//  public boolean createIndex(String name, Map<String, MoleculePropertyMeta.PropertyType> propertyTypes) {
//    return true;
//  }
//
//  @Override
//  public boolean addMapping(String indexName, String fieldName, MoleculePropertyMeta.PropertyType fieldTypes) {
//    return true;
//  }
//
//  @Override
//  public MoleculeIndexLoader getIndexLoader(String name) {
//    MoleculeIndexLoader indexLoader = mock(MoleculeIndexLoader.class);
//    final List<Molecule> addedMolecules = new ArrayList<>();
//    doAnswer(invocation -> {
//      addedMolecules.addAll(invocation.getArgument(0));
//      return null;
//    }).when(indexLoader).add(anyList());
//    when(indexLoader.getLoadedCount()).then(invocation -> (long) addedMolecules.size());
//    when(indexLoader.hasErrors()).thenReturn(false);
//
//    doAnswer(invocation -> {
//      updatedMolecules.addAll(invocation.getArgument(0));
//      return null;
//    }).when(indexLoader).update(anyList(), any(), any());
//    return indexLoader;
//  }
//
//  @Override
//  public MoleculeIndex getIndexSearcher(List<String> name) {
//    final MoleculeSearchBuilder searchBuilder = mock(MoleculeSearchBuilder.class);
//    doAnswer(invocation -> List.of()).when(searchBuilder).build();
//
//    MoleculeIndex index = mock(MoleculeIndex.class);
//    doAnswer(invocation -> {
//      requests.add(invocation.getArgument(0));
//      return searchBuilder;
//    }).when(index).configurableSearchBuilder(any());
//    doAnswer(invocation -> pages).when(index).createPageIterator(anyInt(), anyInt());
//    doAnswer(invocation -> pages).when(index).createPageIterator(any(StorageSearchRequestAdapter.class));
//
//    try {
//      when(index.getDocumentCount()).thenReturn(requiredSize);
//    } catch (IOException e) {
//      logger.error("Error was caught: ", e);
//    }
//    return index;
//  }
//
//  @Override
//  public CompletableFuture<Boolean> deleteIndex(String indexName) {
//    return null;
//  }
//
//  public List<StorageSearchRequestAdapter> getRequests() {
//    return Collections.unmodifiableList(requests);
//  }
//
//  public void clearRequests() {
//    requests.clear();
//  }
//
//  public void setUpdatedMoleculesTrap(List<String> trap) {
//    updatedMolecules = trap;
//  }
//
//  public void setRequiredIndexSize(long size) {
//    requiredSize = size;
//  }
//
//  public void setSearchPages(List<List<Molecule>> pages) {
//    this.pages = new MoleculeIterable (){
//      @Override
//      public MoleculeIterable configure(Consumer<SearchRequest.Builder> configurer) {
//        return this;
//      }
//
//      @Override
//      public long getTotalSearchResult() {
//        return pages.stream().mapToLong(List::size).sum();
//      }
//
//      @NonNull
//      @Override
//      public Iterator<List<Molecule>> iterator() {
//        return pages.iterator();
//      }
//    };
//  }
//}
