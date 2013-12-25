package edu.uiuc.helium.dgrep;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * This class implements the unix Grep command like functionality. It provides
 * the options to search in Key/Value fields of a log file
 */
public class Grep {

    private static final Logger logger = LoggerFactory.getLogger(Grep.class);
    private static final String defaultKeyValueSeparator = ":";
    private String keyValueSeparator;
    private File logFile;
    private static Options options = new Options();
    private CommandLineParser parser = new GnuParser();

    static {
	options.addOption("key", true, "Pattern to look in Key");
	options.addOption("value", true, "Pattern to look in Value");
    }

    Grep(String keyValueSeparator, File logFile) {
	this.keyValueSeparator = keyValueSeparator;
	this.logFile = logFile;
    }

    Grep(File logFile) {
	this(defaultKeyValueSeparator, logFile);
    }

    /**
     * Searches for pattern in Key fields of log file
     * 
     * @param pattern
     *            pattern to search
     * @return Matching Lines from the file
     */
    public List<String> searchKey(String pattern) {
	logger.info("Searching for pattern {} in Keys of log file: {}",
		pattern, logFile);
	Pattern keyRegex = Pattern.compile(pattern);
	return findInLogFile(keyRegex, 0);

    }

    /**
     * 
     * @param pattern
     *            search for pattern in logfile
     * @return Matching Lines from the file
     */
    private List<String> findInLogFile(Pattern pattern, int index) {
	List<String> results = new ArrayList<String>();
	try {
	    /**
	     * read file line by line and see if the pattern exists in file
	     */
	    BufferedReader rdr = new BufferedReader(new FileReader(logFile));
	    String next;
	    while ((next = rdr.readLine()) != null) {
		String[] keyValuePair = next.split(keyValueSeparator, 2);
		if (keyValuePair.length == 2) {
		    Matcher m = pattern.matcher(keyValuePair[index]);
		    if (m.find()) {
			results.add(next);
		    }
		}
	    }
	    rdr.close();
	} catch (Exception e) {
	    logger.error(
		    "Error while searching for pattern: {} in logfile: {}",
		    pattern.pattern(), logFile, e);
	}
	return results;
    }

    /**
     * Searches for pattern in Value fields of log file
     * 
     * @param pattern
     *            pattern to search
     * @return Matching Lines from the file
     */
    public List<String> searchValue(String pattern) {
	logger.info("Searching for pattern {} in Values of log file: {}",
		pattern, logFile);
	// To search in value fields, prefix the pattern with Key Value
	// Separator
	Pattern valueRegex = Pattern.compile(pattern);
	return findInLogFile(valueRegex, 1);
    }

    /**
     * 
     * @param command
     *            Command of the form [ --key, <<pattern>> ] or [ --value,
     *            <<pattern>> ]
     * @return Matching Lines from the file
     */
    public List<String> search(String[] command) {
	try {
	    CommandLine cmd = parser.parse(options, command);
	    /**
	     * Check if it a key based search or value based search
	     */
	    if (cmd.hasOption("key")) {
		return searchKey(cmd.getOptionValue("key"));
	    } else if (cmd.hasOption("value")) {
		return searchValue(cmd.getOptionValue("value"));
	    }
	} catch (ParseException e) {
	    logger.error("Invalid Command {}", Arrays.toString(command));
	}
	return null;
    }

    /**
     * 
     * @param command
     *            Command of the form --key <<pattern>> or --value <<pattern>>
     * @return Matching Lines from the file
     */
    public List<String> search(String command) {
	String[] cmd = command.split("\\s+");
	return search(cmd);
    }

    /**
     * checks if the command is a valid command and follows the required syntax
     * 
     * @param command
     *            Command Input by User
     * @return true if command is valid else false
     */
    public static boolean isValidCommand(String[] command) {
	boolean isValid = true;
	// command should have a length of two
	isValid = isValid && (command.length == 2);
	CommandLineParser parser = new GnuParser();
	try {
	    CommandLine cmd = parser.parse(options, command);
	    /**
	     * Command should have either --key or --value options
	     */
	    isValid = isValid
		    && ((cmd.hasOption("key") && cmd.getOptionValue("key") != null) || (cmd
			    .hasOption("value") && cmd.getOptionValue("value") != null));
	} catch (ParseException e) {
	    isValid = false;
	}
	return isValid;
    }
}
