package com.quantori.qdp.core.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;

import com.quantori.qdp.core.source.model.DataLibrary;
import com.quantori.qdp.core.source.model.DataLibraryType;
import com.quantori.qdp.core.source.model.DataLoader;
import com.quantori.qdp.core.source.model.DataSearcher;
import com.quantori.qdp.core.source.model.DataSource;
import com.quantori.qdp.core.source.model.DataStorage;
import com.quantori.qdp.core.source.model.TransformationStep;
import com.quantori.qdp.core.source.model.TransformationStepBuilder;
import com.quantori.qdp.core.source.model.molecule.Molecule;
import com.quantori.qdp.core.source.model.molecule.search.FetchWaitMode;
import com.quantori.qdp.core.source.model.molecule.search.ProcessingSettings;
import com.quantori.qdp.core.source.model.molecule.search.Request;
import com.quantori.qdp.core.source.model.molecule.search.RequestStructure;
import com.quantori.qdp.core.source.model.molecule.search.SearchRequest;
import com.quantori.qdp.core.source.model.molecule.search.SearchResult;
import com.quantori.qdp.core.source.model.molecule.search.SearchStrategy;
import com.quantori.qdp.core.source.model.molecule.search.StorageResultItem;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class MoleculeServiceTest {
  private static final String TEST_STORAGE = "test_storage";
  private static final String TEST_STORAGE_2 = "test_storage-2";
  private static final String LIBRARY_NAME = "qdp_mol_service_name";
  private static final int MAX_UPLOADS = 3;
  public static final Request BLANK_STORAGE_REQUEST = new Request() {
  };
  public static final Function<StorageResultItem, com.quantori.qdp.core.source.model.molecule.search.SearchResultItem>
      RESULT_ITEM_NUMBER_FUNCTION = i -> new SearchResultItem(((StorageItem) i).getNumber());

  static Stream<FetchWaitMode> waitModes() {
    return Arrays.stream(
        FetchWaitMode.values());
  }

  @SuppressWarnings("unchecked")
  @Test
  void registerMoleculeStorage() throws ExecutionException, InterruptedException {
    MoleculeService service = new MoleculeService();

    var storage = Mockito.mock(DataStorage.class);
    service.registerMoleculeStorage(storage, TEST_STORAGE, MAX_UPLOADS);
    var storage2 = Mockito.mock(DataStorage.class);
    service.registerMoleculeStorage(storage2, TEST_STORAGE_2, MAX_UPLOADS);

    var listOfSources = service.listSources().toCompletableFuture().get();
    assertEquals(2, listOfSources.size());

    assertEquals(1, listOfSources.stream().filter(s -> s.storageName.equals(TEST_STORAGE)).count());
    assertEquals(1, listOfSources.stream().filter(s -> s.storageName.equals(TEST_STORAGE_2)).count());
  }

  @SuppressWarnings("unchecked")
  @Test
  void loadMoleculesFromDataSource() throws Exception {
    var storage = Mockito.mock(DataStorage.class);
    var loader = Mockito.mock(DataLoader.class);
    Mockito.doNothing().when(loader).add(Mockito.any());
    Mockito.when(storage.dataLoader(Mockito.any())).thenReturn(loader);

    DataSource<Molecule> source = (DataSource<Molecule>) Mockito.mock(DataSource.class);
    Mockito.when(source.createIterator()).thenReturn(List.of(new Molecule()).iterator());

    Function<Molecule, Molecule> func = (qdpMolecule) -> {
      qdpMolecule.setId("transformed");
      return qdpMolecule;
    };

    TransformationStep<Molecule, Molecule> step = TransformationStepBuilder.builder(func).build();

    MoleculeService service = new MoleculeService();
    service.registerMoleculeStorage(storage, TEST_STORAGE, MAX_UPLOADS);

    var stat = service.loadMoleculesFromDataSource(TEST_STORAGE, LIBRARY_NAME, source, step)
        .toCompletableFuture().get();
    assertFalse(stat.isFailed());
    assertEquals(1, stat.getCountOfSuccessfullyProcessed());

    ArgumentCaptor<Molecule> qdpMoleculeCaptor = ArgumentCaptor.forClass(Molecule.class);
    Mockito.verify(loader).add(qdpMoleculeCaptor.capture());
    assertEquals("transformed", qdpMoleculeCaptor.getValue().getId());

    Mockito.verify(source, times(1)).createIterator();
    Mockito.verify(source, times(1)).close();
  }

  @SuppressWarnings("unchecked")
  @Test
  void getDataStorageIndexes() throws ExecutionException, InterruptedException {
    MoleculeService service = new MoleculeService();
    DataLibrary index = new DataLibrary(LIBRARY_NAME, DataLibraryType.MOLECULE, Map.of());
    var storage = Mockito.mock(DataStorage.class);
    Mockito.when(storage.getLibraries()).thenReturn(List.of(index));

    service.registerMoleculeStorage(storage, TEST_STORAGE, MAX_UPLOADS);
    service.createDataStorageIndex(TEST_STORAGE, index);
    var result = service.getDataStorageIndexes(TEST_STORAGE).toCompletableFuture().get();

    ArgumentCaptor<DataLibrary> valueCapture = ArgumentCaptor.forClass(DataLibrary.class);
    Mockito.verify(storage).createLibrary(valueCapture.capture());
    assertEquals(LIBRARY_NAME, valueCapture.getValue().getName());
    assertEquals(1, result.size());
    assertEquals(LIBRARY_NAME, result.get(0).getName());
  }

  @Test
  void testSearch() {
    MoleculeService service = new MoleculeService();

    DataStorage<Molecule> testStorage = new IntRangeDataStorage(10);
    service.registerMoleculeStorage(testStorage, TEST_STORAGE, 100);
    var request = SearchRequest.builder()
        .requestStructure(RequestStructure.builder()
            .user("user")
            .storageName(TEST_STORAGE)
            .indexNames(List.of("testIndex"))
            .storageRequest(BLANK_STORAGE_REQUEST)
            .resultFilter(i -> true)
            .resultTransformer(RESULT_ITEM_NUMBER_FUNCTION)
            .build())
        .processingSettings(ProcessingSettings.builder()
            .hardLimit(10)
            .pageSize(10)
            .strategy(SearchStrategy.PAGE_FROM_STREAM)
            .bufferSize(15)
            .parallelism(1)
            .build())
        .build();
    List<SearchResultItem> resultItems = new ArrayList<>();
    SearchResult searchResult = service.search(request).toCompletableFuture().join();
    assertEquals(10, searchResult.getResults().size());
    assertFalse(searchResult.isSearchFinished());
    resultItems.addAll((Collection<? extends SearchResultItem>) searchResult.getResults());
    for (int i = 0; i < 4; i++) {
      searchResult = service.nextSearchResult(searchResult.getSearchId(), 10, "user").toCompletableFuture().join();
      assertEquals(10, searchResult.getResults().size());
      assertFalse(searchResult.isSearchFinished());
      resultItems.addAll((Collection<? extends SearchResultItem>) searchResult.getResults());
    }
    for (int i = 0; i < 5; i++) {
      searchResult = service.nextSearchResult(searchResult.getSearchId(), 6, "user").toCompletableFuture().join();
      assertEquals(6, searchResult.getResults().size());
      assertFalse(searchResult.isSearchFinished());
      resultItems.addAll((Collection<? extends SearchResultItem>) searchResult.getResults());
    }
    for (int i = 0; i < 2; i++) {
      searchResult = service.nextSearchResult(searchResult.getSearchId(), 7, "user").toCompletableFuture().join();
      assertEquals(7, searchResult.getResults().size());
      assertFalse(searchResult.isSearchFinished());
      resultItems.addAll((Collection<? extends SearchResultItem>) searchResult.getResults());
    }
    searchResult = service.nextSearchResult(searchResult.getSearchId(), 7, "user").toCompletableFuture().join();
    assertEquals(6, searchResult.getResults().size());
    assertTrue(searchResult.isSearchFinished());
    resultItems.addAll((Collection<? extends SearchResultItem>) searchResult.getResults());
    String actual = resultItems.stream().map(SearchResultItem::getNumber).collect(Collectors.joining(""));
    String expected = IntStream.range(0, 100).mapToObj(Integer::toString).collect(Collectors.joining(""));
    assertEquals(expected, actual);
  }


  @ParameterizedTest(name = "testSearchInLoop ({arguments})")
  @MethodSource("waitModes")
  void testSearchInLoop(FetchWaitMode fetchWaitMode) {
    MoleculeService service = new MoleculeService();

    DataStorage<Molecule> testStorage = new IntRangeDataStorage(10);
    service.registerMoleculeStorage(testStorage, TEST_STORAGE, 100);
    var request = SearchRequest.builder()
        .requestStructure(RequestStructure.builder()
            .user("user")
            .storageName(TEST_STORAGE)
            .indexNames(List.of("testIndex"))
            .storageRequest(BLANK_STORAGE_REQUEST)
            .resultFilter(i -> true)
            .resultTransformer(RESULT_ITEM_NUMBER_FUNCTION)
            .build())
        .processingSettings(ProcessingSettings.builder()
            .hardLimit(8)
            .pageSize(8)
            .strategy(SearchStrategy.PAGE_FROM_STREAM)
            .bufferSize(15)
            .parallelism(1)
            .build())
        .build();
    List<SearchResultItem> resultItems = new ArrayList<>();
    SearchResult searchResult = service.search(request).toCompletableFuture().join();
    resultItems.addAll((Collection<? extends SearchResultItem>)searchResult.getResults());
    while (!searchResult.isSearchFinished()) {
      searchResult = service.nextSearchResult(searchResult.getSearchId(), 8, "user").toCompletableFuture().join();
      resultItems.addAll((Collection<? extends SearchResultItem>)searchResult.getResults());
    }
    String actual = resultItems.stream().map(SearchResultItem::getNumber).collect(Collectors.joining(""));
    String expected = IntStream.range(0, 100).mapToObj(Integer::toString).collect(Collectors.joining(""));
    assertEquals(expected, actual);
  }

  @Test
  void testSearchExceptionInTransformer() {
    MoleculeService service = new MoleculeService();
    DataStorage<Molecule> testStorage = new IntRangeDataStorage(10);
    service.registerMoleculeStorage(testStorage, TEST_STORAGE, 100);
    var request = SearchRequest.builder()
        .requestStructure(RequestStructure.builder()
            .storageName(TEST_STORAGE)
            .indexNames(List.of("testIndex"))
            .storageRequest(BLANK_STORAGE_REQUEST)
            .resultFilter(i -> true)
            .resultTransformer(i -> {
              SearchResultItem result = new SearchResultItem(((StorageItem) i).getNumber());
              if (result.getNumber().equals("5")) {
                throw new RuntimeException("wrong number");
              }
              return result;
            })
            .build())
        .processingSettings(ProcessingSettings.builder()
            .hardLimit(10)
            .pageSize(10)
            .strategy(SearchStrategy.PAGE_FROM_STREAM)
            .bufferSize(15)
            .parallelism(1)
            .build())
        .build();

    SearchResult searchResult = service.search(request).toCompletableFuture().join();
    assertEquals(10, searchResult.getResults().size());
    assertEquals(IntStream.range(0, 11).filter(i -> i != 5).mapToObj(Integer::toString).collect(Collectors.toList()),
            ((List<SearchResultItem>)searchResult.getResults()).stream()
                .map(SearchResultItem::getNumber).toList());
  }

  @Test
  void testSearchExceptionInFilter() {
    MoleculeService service = new MoleculeService();
    DataStorage<Molecule> testStorage = new IntRangeDataStorage(10);
    service.registerMoleculeStorage(testStorage, TEST_STORAGE, 100);
    var request = SearchRequest.builder()
        .requestStructure(RequestStructure.builder()
            .storageName(TEST_STORAGE)
            .indexNames(List.of("testIndex"))
            .storageRequest(BLANK_STORAGE_REQUEST)
            .resultFilter(i -> {
              if (((StorageItem) i).getNumber() % 2 == 0) {
                throw new RuntimeException("wrong number");
              }
              return true;
            })
            .resultTransformer(RESULT_ITEM_NUMBER_FUNCTION)
            .build())
        .processingSettings(ProcessingSettings.builder()
            .hardLimit(10)
            .pageSize(10)
            .strategy(SearchStrategy.PAGE_FROM_STREAM)
            .bufferSize(15)
            .parallelism(1)
            .build())
        .build();

    SearchResult searchResult = service.search(request).toCompletableFuture().join();
    assertEquals(10, searchResult.getResults().size());
    assertEquals(IntStream.range(0, 20).filter(i -> (i % 2) != 0).mapToObj(Integer::toString).collect(Collectors.toList()),
            ((List<SearchResultItem>)searchResult.getResults()).stream()
                .map(SearchResultItem::getNumber).toList());
  }

  @Test
  void testSearchExceptionInDataSearcher() {
    MoleculeService service = new MoleculeService();
    String errorMessage = "Cannot load data";
    DataStorage<Molecule> testStorage = new DataStorage<>() {
      @Override
      public DataSearcher dataSearcher(SearchRequest searchRequest) {
        return new DataSearcher() {

          int count;

          @Override
          public List<? extends StorageResultItem> next() {
            if (count == 0) {
              count++;
              return IntStream.range(0, 5)
                  .mapToObj(StorageItem::new)
                  .collect(Collectors.toList());

            } else {
              throw new RuntimeException(errorMessage);
            }
          }

          @Override
          public void close() {

          }
        };
      }
    };
    service.registerMoleculeStorage(testStorage, TEST_STORAGE, 100);
    var request = SearchRequest.builder()
        .requestStructure(RequestStructure.builder()
            .storageName(TEST_STORAGE)
            .indexNames(List.of("testIndex"))
            .storageRequest(BLANK_STORAGE_REQUEST)
            .resultFilter(i -> true)
            .resultTransformer(RESULT_ITEM_NUMBER_FUNCTION)
            .build())
        .processingSettings(ProcessingSettings.builder()
            .hardLimit(8)
            .pageSize(8)
            .strategy(SearchStrategy.PAGE_FROM_STREAM)
            .bufferSize(15)
            .parallelism(1)
            .build())
        .build();
    CompletionException completionException =
        assertThrows(CompletionException.class, () -> service.search(request).toCompletableFuture().join());
    assertTrue(completionException.getMessage().contains(errorMessage));
  }


  @Test
  void testSearchExceptionInDataStorage() {
    MoleculeService service = new MoleculeService();
    String errorMessage = "Implementation error";
    DataStorage<Molecule> testStorage = new DataStorage<Molecule>() {
      @Override
      public DataSearcher dataSearcher(SearchRequest searchRequest) {
        throw new RuntimeException(errorMessage);
      }
    };
    service.registerMoleculeStorage(testStorage, TEST_STORAGE, 100);
    var request = SearchRequest.builder()
        .requestStructure(RequestStructure.builder()
            .storageName(TEST_STORAGE)
            .indexNames(List.of("testIndex"))
            .storageRequest(BLANK_STORAGE_REQUEST)
            .resultFilter(i -> true)
            .resultTransformer(RESULT_ITEM_NUMBER_FUNCTION)
            .build())
        .processingSettings(ProcessingSettings.builder()
            .hardLimit(8)
            .pageSize(8)
            .strategy(SearchStrategy.PAGE_FROM_STREAM)
            .bufferSize(15)
            .parallelism(1)
            .build())
        .build();
    CompletionException completionException =
        assertThrows(CompletionException.class, () -> service.search(request).toCompletableFuture().join());
    assertTrue(completionException.getMessage().contains(errorMessage));
  }

  public static class StorageItem implements StorageResultItem {
    private final int number;

    public StorageItem(int i) {
      this.number = i;
    }

    public int getNumber() {
      return number;
    }
  }

  public static class SearchResultItem implements com.quantori.qdp.core.source.model.molecule.search.SearchResultItem {
    private final String number;

    public SearchResultItem(int number) {
      this.number = Integer.toString(number);
    }

    public String getNumber() {
      return number;
    }

    @Override
    public String toString() {
      return "SearchResultItem{" +
          "number='" + number + '\'' +
          '}';
    }
  }

  public static class IntRangeDataStorage implements DataStorage<Molecule> {

    private final int chunks;

    public IntRangeDataStorage(int chunks) {
      this.chunks = chunks;
    }

    @Override
    public DataSearcher dataSearcher(SearchRequest searchRequest) {
      return new DataSearcher() {
        int counter;

        @Override
        public List<? extends StorageResultItem> next() {
          if (counter >= chunks) {
            return List.of();
          } else {
            List<StorageResultItem> result = IntStream.range(counter * 10, (counter + 1) * 10)
                .mapToObj(StorageItem::new)
                .collect(Collectors.toList());
            counter++;
            return result;
          }
        }

        @Override
        public void close() throws Exception {

        }
      };
    }
  }

}