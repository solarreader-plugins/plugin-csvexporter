/*
 * Copyright (c) 2024-2025 Stefan Toengi
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package de.schnippsche.solarreader.plugins.csvexporter;

import de.schnippsche.solarreader.backend.exporter.AbstractExporter;
import de.schnippsche.solarreader.backend.exporter.TransferData;
import de.schnippsche.solarreader.backend.singleton.GlobalUsrStore;
import de.schnippsche.solarreader.backend.table.Table;
import de.schnippsche.solarreader.backend.table.TableCell;
import de.schnippsche.solarreader.backend.table.TableColumn;
import de.schnippsche.solarreader.backend.table.TableRow;
import de.schnippsche.solarreader.backend.util.Setting;
import de.schnippsche.solarreader.frontend.ui.HtmlInputType;
import de.schnippsche.solarreader.frontend.ui.HtmlWidth;
import de.schnippsche.solarreader.frontend.ui.UIInputElementBuilder;
import de.schnippsche.solarreader.frontend.ui.UIList;
import de.schnippsche.solarreader.frontend.ui.ValueText;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ResourceBundle;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import org.tinylog.Logger;

/**
 * The CsvExporter class is an implementation of the ExporterInterface that handles the export of
 * data to CSV files. It supports configuration options such as the delimiter, terminator, line
 * break, and output path.
 */
public class CsvExporter extends AbstractExporter {
  private static final String REQUIRED_ERROR = "csvexporter.required.error";
  private static final String DELIMITER = "delimiter";
  private static final String TERMINATOR = "terminator";
  private static final String LINEBREAK = "linebreak";
  private static final String PATH = "path";
  private final BlockingQueue<TransferData> queue;
  private Path directory;
  private String currentDelimiter;
  private String currentTerminator;
  private String currentLinebreaker;
  private Thread consumerThread;
  private volatile boolean running;

  /** Constructs a new CsvExporter with default configuration. */
  public CsvExporter() {
    super();
    this.queue = new LinkedBlockingQueue<>();
  }

  @Override
  public ResourceBundle getPluginResourceBundle() {
    return ResourceBundle.getBundle("csvexporter", locale);
  }

  /** Initializes the CSV exporter by starting the consumer thread. */
  @Override
  public void initialize() {
    Logger.debug("initialize csv exporter");
    running = true;
    consumerThread = new Thread(this::processQueue, "CsvExporterThread");
    consumerThread.start();
  }

  /** Shuts down the CSV exporter by stopping the consumer thread. */
  @Override
  public void shutdown() {
    running = false;
    if (consumerThread != null && consumerThread.isAlive()) {
      consumerThread.interrupt();
      try {
        consumerThread.join();
      } catch (InterruptedException e) {
        Logger.warn("shutdown csv exporter interrupted");
        Thread.currentThread().interrupt();
      }
    }
    Logger.debug("shutdown csv exporter finished");
  }

  /**
   * Adds a new transfer data entry to the export queue.
   *
   * @param transferData the transfer data to add.
   */
  @Override
  public void addExport(TransferData transferData) {
    if (transferData.getTables().isEmpty()) {
      Logger.debug("no exporting tables, skip export");
      return;
    }
    Logger.debug("add export to '{}'", exporterData.getName());
    exporterData.setLastCall(transferData.getTimestamp());
    queue.add(transferData);
  }

  /**
   * Tests the connection to the exporter with the provided configuration.
   *
   * @param setting the configuration setting.
   * @return an empty string if the connection test is successful.
   * @throws IOException if the directory does not exist, is not a directory, or is not writable.
   */
  @Override
  public String testExporterConnection(Setting setting) throws IOException {
    String directoryPath = setting.getConfigurationValueAsString(PATH, "");
    Path testDirectory = Paths.get(directoryPath);
    if (!Files.exists(testDirectory))
      throw new IOException(resourceBundle.getString("csvexporter.error.directorynotexists"));
    if (!Files.isDirectory(testDirectory))
      throw new IOException(resourceBundle.getString("csvexporter.error.notdirectory"));
    if (!Files.isWritable(testDirectory))
      throw new IOException(resourceBundle.getString("csvexporter.error.notwritable"));

    return "";
  }

  /**
   * Retrieves the exporter dialog for the current locale.
   *
   * @return an optional UIList containing the dialog elements.
   */
  @Override
  public Optional<UIList> getExporterDialog() {
    UIList uiList = new UIList();
    uiList.addElement(
        new UIInputElementBuilder()
            .withName(PATH)
            .withType(HtmlInputType.TEXT)
            .withColumnWidth(HtmlWidth.FULL)
            .withRequired(true)
            .withTooltip(resourceBundle.getString("csvexporter.path.tooltip"))
            .withLabel(resourceBundle.getString("csvexporter.path.text"))
            .withPlaceholder(resourceBundle.getString("csvexporter.path.text"))
            .withInvalidFeedback(resourceBundle.getString(REQUIRED_ERROR))
            .build());

    uiList.addElement(
        new UIInputElementBuilder()
            .withName(DELIMITER)
            .withType(HtmlInputType.TEXT)
            .withColumnWidth(HtmlWidth.THIRD)
            .withTooltip(resourceBundle.getString("csvexporter.delimiter.tooltip"))
            .withLabel(resourceBundle.getString("csvexporter.delimiter.text"))
            .withPlaceholder(resourceBundle.getString("csvexporter.delimiter.text"))
            .build());

    uiList.addElement(
        new UIInputElementBuilder()
            .withName(TERMINATOR)
            .withColumnWidth(HtmlWidth.THIRD)
            .withRequired(true)
            .withTooltip(resourceBundle.getString("csvexporter.terminator.tooltip"))
            .withLabel(resourceBundle.getString("csvexporter.terminator.text"))
            .withPlaceholder(resourceBundle.getString("csvexporter.terminator.text"))
            .withInvalidFeedback(resourceBundle.getString(REQUIRED_ERROR))
            .build());

    uiList.addElement(
        new UIInputElementBuilder()
            .withName(LINEBREAK)
            .withColumnWidth(HtmlWidth.THIRD)
            .withRequired(true)
            .withTooltip(resourceBundle.getString("csvexporter.linebreak.tooltip"))
            .withLabel(resourceBundle.getString("csvexporter.linebreak.text"))
            .withPlaceholder(resourceBundle.getString("csvexporter.linebreak.text"))
            .withInvalidFeedback(resourceBundle.getString(REQUIRED_ERROR))
            .build());

    return Optional.of(uiList);
  }

  /**
   * Retrieves the default exporter configuration.
   *
   * @return a map containing the default configuration parameters.
   */
  @Override
  public Setting getDefaultExporterSetting() {
    Setting setting = new Setting();
    setting.setConfigurationValue(PATH, "export");
    setting.setConfigurationValue(LINEBREAK, "\\n");
    setting.setConfigurationValue(TERMINATOR, "\"");
    setting.setConfigurationValue(DELIMITER, ",");
    return setting;
  }

  /** Processes the export queue by taking each entry and exporting it. */
  private void processQueue() {
    while (running) {
      try {
        TransferData transferData = queue.take();
        doStandardExport(transferData);
      } catch (InterruptedException e) {
        Logger.warn("processQueue interrupted");
        Thread.currentThread().interrupt();
        break;
      }
    }
  }

  /**
   * Retrieves the column names from a list of ValueText objects.
   *
   * @param colData the list of ValueText objects.
   * @return a list of column names.
   */
  private List<String> getColumnNames(List<ValueText> colData) {
    return colData.stream().map(ValueText::getText).collect(Collectors.toList());
  }

  /**
   * Retrieves the column values from a list of ValueText objects.
   *
   * @param colData the list of ValueText objects.
   * @return a list of column values.
   */
  private List<String> getColumnValues(List<ValueText> colData) {
    return colData.stream().map(ValueText::getValue).collect(Collectors.toList());
  }

  /**
   * Retrieves the calculated string value for a table column and row.
   *
   * @param tableColumn the table column.
   * @param tableRow the table row.
   * @return the calculated string value.
   */
  private String getCalculatedString(Table table, TableColumn tableColumn, TableRow tableRow) {
    Optional<TableCell> optionalTableCell = table.getTableCell(tableColumn, tableRow);
    return optionalTableCell.map(TableCell::getCalculatedAsString).orElse(null);
  }

  /**
   * Converts special characters in a string to their corresponding characters.
   *
   * @param original the original string.
   * @return the string with special characters converted.
   */
  private String convertSpecialChars(String original) {
    return original.replace("\\n", "\n").replace("\\r", "\r").replace("\\t", "\t");
  }

  /**
   * Exports a table to a CSV file.
   *
   * @param table the table to export.
   * @param zonedDateTime the timestamp for the export.
   * @throws IOException if an I/O error occurs.
   */
  @Override
  protected void exportTable(Table table, ZonedDateTime zonedDateTime) throws IOException {
    Logger.debug("directory:'{}', tablename:'{}'", directory, table.getTableName());
    Path exportPath = directory.resolve(table.getTableName() + ".csv");
    long currentTimestampSeconds = GlobalUsrStore.getInstance().getUtcTimestamp(zonedDateTime);
    boolean mustBuildHeader = !Files.exists(exportPath);
    try (BufferedWriter writer =
        Files.newBufferedWriter(
            exportPath,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.APPEND)) {
      TableColumn timestampColumn = table.getTimestampColumn();
      for (TableRow tableRow : table.getRows()) {
        List<TableColumn> columns = table.getColumnsWithoutTimestamp();
        List<ValueText> colData = new ArrayList<>();
        for (TableColumn tableColumn : columns) {
          colData.add(
              new ValueText(
                  getCalculatedString(table, tableColumn, tableRow), tableColumn.getColumnName()));
        }
        // last column is timestamp
        Optional<TableCell> optionalTableCell = table.getTableCell(timestampColumn, tableRow);
        long timestamp =
            optionalTableCell
                .map(TableCell::getCalculatedAsTimestampSeconds)
                .orElse(currentTimestampSeconds);
        colData.add(new ValueText(Long.toString(timestamp), "timestamp"));
        if (mustBuildHeader) {
          String headerLine =
              currentTerminator
                  + String.join(
                      currentTerminator + currentDelimiter + currentTerminator,
                      getColumnNames(colData))
                  + currentTerminator;
          writer.write(headerLine);
          writer.write(currentLinebreaker);
          mustBuildHeader = false;
        }
        String dataLine =
            currentTerminator
                + String.join(
                    currentTerminator + currentDelimiter + currentTerminator,
                    getColumnValues(colData))
                + currentTerminator;
        writer.write(dataLine);
        writer.write(currentLinebreaker);
      }
    }
  }

  /** Updates the configuration of the exporter based on the exporter data. */
  protected void updateConfiguration() {
    Setting setting = exporterData.getSetting();
    directory = Paths.get(setting.getConfigurationValueAsString(PATH, ""));
    currentDelimiter = convertSpecialChars(setting.getConfigurationValueAsString(DELIMITER, ","));
    currentTerminator = setting.getConfigurationValueAsString(TERMINATOR, "\"");
    currentLinebreaker =
        convertSpecialChars(setting.getConfigurationValueAsString(LINEBREAK, "\\n"));
  }
}
