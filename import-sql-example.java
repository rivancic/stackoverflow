package com.rivancic.java.sqlimport;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class ReadSqlTest {

  enum DirectoryPriority {
    DLL("DDL",0), Table("Table",1), Function("Function",2), StoredProcedure("Stored Procedure",3);
    private int priority;
    private String directoryName;
    private static HashMap<String, DirectoryPriority> map;
    DirectoryPriority(String directoryName, int priority) {
      this.priority = priority;
      this.directoryName = directoryName;
    }

    public int getPriority() {
      return priority;
    }

    public String getDirectoryName() {
      return directoryName;
    }

    static{
      createMapOfTextAndEnum();
    }

    public static void createMapOfTextAndEnum() {
      map = new HashMap<>();
      for (DirectoryPriority directoryPriority : DirectoryPriority.values()) {
        map.put(directoryPriority.getDirectoryName(), directoryPriority);
      }
    }

    public static DirectoryPriority getDirectoryPriority(String directoryName) {
      return map.get(directoryName);
    }
  }

  class SqlFile implements Comparable<SqlFile> {

    private DirectoryPriority directoryPriority;
    private Path path;

    public SqlFile(Path sourcePath, Path path) {
      this.path = path;
      String directoryName = path.subpath(sourcePath.getNameCount(), sourcePath.getNameCount()+1).getFileName().toString();
      directoryPriority = DirectoryPriority.getDirectoryPriority(directoryName);
    }

    @Override
    public String toString() {
      return path.toString() + " " + directoryPriority.toString();
    }

    @Override
    public int compareTo(SqlFile o) {
      return this.directoryPriority.getPriority() - o.directoryPriority.getPriority();
    }
  }

  @Test
  public void testSqlImport() {

    final Path sourcePath = Paths.get("/tmp/extracted-sql-import-root/");
    try (Stream<Path> walk = Files.walk(sourcePath)) {

      walk.filter(isSqlFile)
              .map(filePath -> new SqlFile(sourcePath, filePath))
              .sorted()
              .forEach(processSqlFile);

    } catch (IOException iOException) {
      // TODO handle exception
    }
  }

  // filter only SQL files
  Predicate<Path> isSqlFile = (path) -> Files.isRegularFile(path) && path.getFileName().toString().endsWith(".sql");

  private Consumer<? super SqlFile> processSqlFile = sqlFile -> {

    System.out.println(sqlFile);
    // Copy SQL to Database
  };
}
