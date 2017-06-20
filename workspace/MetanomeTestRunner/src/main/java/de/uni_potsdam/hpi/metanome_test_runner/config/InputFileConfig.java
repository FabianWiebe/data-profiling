package de.uni_potsdam.hpi.metanome_test_runner.config;

import java.io.File;

public class InputFileConfig {
	public String datasetName;
	public String folderPath = "data" + File.separator;
	public String fileEnding = ".csv";
	public String nullString = "";
	public char separator;
	public char quotechar = '\"';
	public char escape = '\\';
	public int skipLines = 0;
	public boolean strictQuotes = false;
	public boolean ignoreLeadingWhiteSpace = true;
	public boolean hasHeader;
	public boolean skipDifferingLines = true; // Skip lines that differ from the dataset's schema
}
