package com.quantori.cqp.api.service;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import com.quantori.cqp.api.StorageException;
import com.quantori.cqp.api.model.Library;
import com.quantori.cqp.api.model.LibraryType;
import com.quantori.cqp.api.model.Property;
import com.quantori.cqp.api.model.upload.Molecule;
import com.quantori.cqp.core.model.ItemWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;


@ExtendWith(MockitoExtension.class)
class StorageLibraryTest {

  @Mock
  private StorageLibrary storageLibrary;

  @Mock
  private StorageMolecules storageMolecules;

  @Mock
  private ItemWriter<Molecule> itemWriter;

  private static final String LIBRARY_ID = UUID.randomUUID().toString();

  @ParameterizedTest
  @EnumSource(value = LibraryType.class, names = {"molecules", "reactions"})
  void testCreateLibrary(LibraryType libraryType) {
    Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
    String libraryName = "test_create_library_" + libraryType;

    Library library = new Library();
    library.setName(libraryName);
    library.setCreatedStamp(now.plusSeconds(1));
    library.setUpdatedStamp(now.plusSeconds(1));

    when(storageLibrary.createLibrary(eq(libraryName), eq(libraryType), any())).thenReturn(library);

    Library result = storageLibrary.createLibrary(libraryName, libraryType, null);
    assertEquals(libraryName, result.getName());
    assertEquals(result.getCreatedStamp(), result.getUpdatedStamp());
  }

  @Test
  void testCreateLibrary_withPropertiesMapping() {
    Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
    Map<String, Property> propertiesMapping = Map.of(
            "test1", new Property("test 1", Property.PropertyType.STRING),
            "test2", new Property("test 2", Property.PropertyType.DATE),
            "test3", new Property("test 3", Property.PropertyType.DECIMAL)
    );

    Library library = new Library();
    library.setName("test_create_library_properties_mapping");
    library.setCreatedStamp(now.plusSeconds(1));
    library.setUpdatedStamp(now.plusSeconds(1));
    library.setId(LIBRARY_ID);

    when(storageLibrary.createLibrary(anyString(), eq(LibraryType.molecules), eq(propertiesMapping), any()))
            .thenReturn(library);
    when(storageLibrary.getPropertiesMapping(LIBRARY_ID)).thenReturn(propertiesMapping);

    Library result = storageLibrary.createLibrary("test_create_library_properties_mapping",
            LibraryType.molecules, propertiesMapping, null);

    assertEquals("test_create_library_properties_mapping", result.getName());
    assertEquals(result.getCreatedStamp(), result.getUpdatedStamp());
    assertEquals(propertiesMapping, storageLibrary.getPropertiesMapping(result.getId()));

    Map<String, Property> newProperties = Map.of(
            "test4", new Property("test 4", Property.PropertyType.STRING),
            "test5", new Property("test 5", Property.PropertyType.DATE),
            "test6", new Property("test 6", Property.PropertyType.DECIMAL)
    );
    Map<String, Property> all = new LinkedHashMap<>(propertiesMapping);
    all.putAll(newProperties);

    when(storageLibrary.addPropertiesMapping(LIBRARY_ID, newProperties)).thenReturn(true);
    when(storageLibrary.getPropertiesMapping(LIBRARY_ID)).thenReturn(all);

    boolean status = storageLibrary.addPropertiesMapping(LIBRARY_ID, newProperties);
    assertTrue(status);
    assertEquals(all, storageLibrary.getPropertiesMapping(LIBRARY_ID));
  }

  @Test
  void testCreateLibrary_addExistingPropertyMapping() {
    Map<String, Property> initial = Map.of(
            "test1", new Property("test 1", Property.PropertyType.STRING)
    );
    Map<String, Property> duplicate = Map.of(
            "test1", new Property("test 1", Property.PropertyType.STRING)
    );

    Library library = new Library();
    library.setId(LIBRARY_ID);

    when(storageLibrary.createLibrary(any(), any(), any(), any())).thenReturn(library);
    doThrow(new StorageException("A library %s already contains the following properties test1".formatted(LIBRARY_ID)))
            .when(storageLibrary).addPropertiesMapping(eq(LIBRARY_ID), eq(duplicate));

    Library result = storageLibrary.createLibrary("lib", LibraryType.molecules, initial, null);

    Exception exception = assertThrows(StorageException.class, () ->
            storageLibrary.addPropertiesMapping(result.getId(), duplicate)
    );
    assertEquals("A library %s already contains the following properties test1".formatted(LIBRARY_ID), exception.getMessage());
  }

  @Test
  void testCreateLibrary_updatePropertyMapping() {
    Library library = new Library();
    library.setId(LIBRARY_ID);

    Map<String, Property> newMapping = Map.of(
            "test1", new Property("new test 1", Property.PropertyType.DATE, 1, true, false)
    );

    when(storageLibrary.createLibrary(any(), any(), any(), any())).thenReturn(library);
    when(storageLibrary.updatePropertiesMapping(eq(LIBRARY_ID), eq(newMapping))).thenReturn(true);
    when(storageLibrary.getPropertiesMapping(eq(LIBRARY_ID))).thenReturn(
            Map.of("test1", new Property("new test 1", Property.PropertyType.STRING, 1, true, false))
    );

    Library result = storageLibrary.createLibrary("lib", LibraryType.molecules, Map.of(), null);
    boolean status = storageLibrary.updatePropertiesMapping(result.getId(), newMapping);
    assertTrue(status);

    Map<String, Property> actual = storageLibrary.getPropertiesMapping(result.getId());
    assertEquals("new test 1", actual.get("test1").getName());
    assertEquals(Property.PropertyType.STRING, actual.get("test1").getType());
  }

  @Test
  void testCreateLibrary_updateNonExistingPropertyMapping() {
    Library library = new Library();
    library.setId(LIBRARY_ID);

    Map<String, Property> newMapping = Map.of(
            "test4", new Property("test 4", Property.PropertyType.STRING)
    );

    when(storageLibrary.createLibrary(any(), any(), any(), any())).thenReturn(library);
    doThrow(new StorageException("A library %s does not contain the following properties test4".formatted(LIBRARY_ID)))
            .when(storageLibrary).updatePropertiesMapping(eq(LIBRARY_ID), eq(newMapping));

    Library result = storageLibrary.createLibrary("lib", LibraryType.molecules, Map.of(), null);
    Exception exception = assertThrows(StorageException.class, () ->
            storageLibrary.updatePropertiesMapping(result.getId(), newMapping)
    );
    assertEquals("A library %s does not contain the following properties test4".formatted(LIBRARY_ID), exception.getMessage());
  }

  @Test
  void testCreateLibrary_updateGeneratedPropertyMapping() {
    Library library = new Library();
    library.setId(LIBRARY_ID);

     Molecule m = new Molecule();
    m.setMolProperties(Map.of("test1", "1", "test2", "1.0", "test3", "some value"));
    m.setSmiles("");
    m.setStructure(new byte[0]);

    doNothing().when(itemWriter).write(any());
    itemWriter.write(m);

    Map<String, Property> properties = Map.of(
            "test1", new Property("test1", Property.PropertyType.DECIMAL),
            "test2", new Property("test2", Property.PropertyType.DECIMAL),
            "test3", new Property("test3", Property.PropertyType.STRING)
    );

    when(storageLibrary.getPropertiesMapping(LIBRARY_ID)).thenReturn(properties);
    when(storageLibrary.updatePropertiesMapping(LIBRARY_ID, properties)).thenReturn(true);

    boolean status = storageLibrary.updatePropertiesMapping(LIBRARY_ID, properties);
    assertTrue(status);
    assertEquals(properties, storageLibrary.getPropertiesMapping(LIBRARY_ID));
  }

  @ParameterizedTest
  @EnumSource(value = LibraryType.class, names = {"molecules", "reactions"})
  void testUpdateLibrary(LibraryType libraryType) {
    Library library = new Library();
    library.setId(LIBRARY_ID);
    library.setName("test_update_library_" + libraryType);
    library.setStructuresCount(0);
    Instant now = Instant.now();
    library.setCreatedStamp(now);
    library.setUpdatedStamp(now);

    when(storageLibrary.createLibrary(any(), any(), any())).thenReturn(library);
    when(storageLibrary.updateLibrary(any())).thenReturn(library);
    when(storageLibrary.getLibraryById(LIBRARY_ID)).thenReturn(Optional.of(library));

    Library created = storageLibrary.createLibrary("lib", libraryType, null);
    created.setName("test_update_library_" + libraryType + "_new_name");

    Library updated = storageLibrary.updateLibrary(created);
    assertEquals("test_update_library_" + libraryType + "_new_name", updated.getName());
    assertEquals(created, storageLibrary.getLibraryById(LIBRARY_ID).orElseThrow());
  }

  @Test
  void testUpdateLibrary_parallel() throws InterruptedException {
    String libraryName = "test_update_library_parallel";
    Library baseLibrary = new Library();
    baseLibrary.setId(LIBRARY_ID);
    baseLibrary.setName(libraryName);

    when(storageLibrary.createLibrary(any(), any(), any())).thenReturn(baseLibrary);
    when(storageLibrary.updateLibrary(any())).thenAnswer(invocation -> invocation.getArgument(0));

    Library library = storageLibrary.createLibrary(libraryName, LibraryType.molecules, null);

    int threadCounts = 10;
    CountDownLatch latch = new CountDownLatch(threadCounts);
    Thread[] threads = new Thread[threadCounts];
    Map<String, String> resultMap = new ConcurrentHashMap<>();

    for (int i = 0; i < threadCounts; i++) {
      threads[i] = new Thread(() -> {
        Library update = new Library();
        String newName = library.getName() + "_" + UUID.randomUUID();
        update.setId(library.getId());
        update.setName(newName);
        latch.countDown();
        try {
          latch.await();
          Library updated = storageLibrary.updateLibrary(update);
          resultMap.put(newName, updated.getName());
        } catch (InterruptedException e) {
          fail(e);
        }
      });
      threads[i].start();
    }

    for (Thread thread : threads) {
      thread.join();
    }

    assertAll(resultMap.entrySet().stream()
            .map(entry -> (Executable) () -> assertEquals(entry.getKey(), entry.getValue()))
            .collect(Collectors.toList()));
  }

  @ParameterizedTest
  @EnumSource(value = LibraryType.class, names = {"molecules", "reactions"})
  void testDeleteLibrary(LibraryType libraryType) {
    Library library = new Library();
    library.setId(LIBRARY_ID);
    library.setName("lib");
    library.setType(libraryType);

    when(storageLibrary.createLibrary(any(), any(), any())).thenReturn(library);
    when(storageLibrary.deleteLibrary(library)).thenReturn(true).thenReturn(false);

    Library result = storageLibrary.createLibrary("lib", libraryType, null);
    assertTrue(storageLibrary.deleteLibrary(result));
    assertFalse(storageLibrary.deleteLibrary(result));
  }

  @ParameterizedTest
  @EnumSource(value = LibraryType.class, names = {"metrics", "any"})
  void testDeleteLibraryWrongType(LibraryType libraryType) {
    Library library = new Library();
    library.setId("1");
    library.setName("name");
    library.setType(libraryType);

    doThrow(new StorageException("A library type must be molecule or reaction"))
            .when(storageLibrary).deleteLibrary(library);

    assertThrows(StorageException.class, () -> storageLibrary.deleteLibrary(library));
  }
}