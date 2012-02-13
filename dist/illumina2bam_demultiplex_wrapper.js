#!/usr/bin/env node

/*
 * About
 * Will transform a samplesheet with multiple lanes into barcode files divided by lanes
 *     Headers in the samplesheet will be transformed to lowercase, some will then be replaced if specified below,
 *     and headers that adher to the format of "HeaderName (hn)" transforms to hn:headername - these are added to the 
 *     @RG header of the resulting bam/sam as custom attributes "hn:[...]"
 * Will start concurrent processes of illumina2bam and BamIndexDecoder for each lane in the samplesheet
 * Will perform rudimentary sanity checks on the resulting barcode files, which implies inheherent issues in the provided samplesheet
 */

var settings = {};

//The following headers (keys) in the samplesheet will be replaced by the values below
settings.replaceHeaders = {
	"library": "library_name",
	"sample": "sample_name",
	"index": "barcode_sequence"
}

//These values and headers are required in resulting barcode files
settings.requiredValues = [
	"pr:project",
	"sample_name",
	"library_name",
	"barcode_sequence",
	"fcid",
	"readstring"
];

var util  = require('util'),
	spawn = require('child_process').spawn,
	path = require('path'),
	wrench = require('wrench'),
	fs = require("fs");

var argv = require('optimist')
	    .usage('Wrapper for performing illumina bcl to bam encoding and demultiplexing.\nUsage: $0')
	    .demand('s')
	    .demand('b')
	    .demand('o')
	    .demand('t')
		.default('f', 'bam')
		.default('m', 0)
		.default('d', 2)
		.default('n', 0)
		.default('debug', false)
		.default('force', false)
		.default('im', "2g")
		.default('ib', "1g")
		.default('omitLanes', "")
	    .alias('v', 'verbose')
	    .alias('s', 'samplesheet')
	    .alias('b', 'basecallsDirectory')
	    .alias('o', 'outputDirectory')
	    .alias('t', 'tempDirectory')
		.options('f', {
			default : 'bam',
		})
		.options('m', {
			default : 0,
		})
		.options('d', {
			default : 2,
		})
		.options('n', {
			default : 0,
		})
	    .describe('s', 'Samplesheet')
	    .describe('b', 'Basecalls directory')
	    .describe('o', 'Output directory, sub dirs /project/RunID will be created')
	    .describe('t', 'Temp directory, sub dirs will be created')
	    .describe('f', 'Output format [bam|sam], default to \'bam\'')
	    .describe('v', 'Verbose output')
	    .describe('m', 'Maximum mismatches for a barcode to be considered a match')
	    .describe('d', 'Minimum difference between number of mismatches in the best and second best barcodes for a barcode to be considered a match')
	    .describe('n', 'Maximum allowable number of no-calls in a barcode read before it is considered unmatchable')
	    .describe('im', 'Maximum memory heap size for illumina2bam process, defaults to 2g')
	    .describe('ib', 'Maximum memory heap size for BamIndexDecoder process, defaults to 1g')
	    .describe('debug', 'Parse the first tile in each lane')
	    .describe('force', 'Disables check if library already exists, hence overwrites files if the already exist')
	    .describe('omitLanes', 'Comma separated list with numbers identifying lanes to omit')
	    .argv
	;

var runId = null;
var mainSampleSheet = null;
var intensitiesDirectory = path.normalize(argv.basecallsDirectory + "/../.");
var outputDirs = {};

argv.tempDirectory = path.normalize(argv.tempDirectory + "/" + getRunId() + "/");

if (argv.f !== "bam") {
	argv.f = "sam";
}

//Create main log
wrench.mkdirSyncRecursive(path.normalize(argv.outputDirectory + "/Undetermined/" + getRunId() + "/"));
var mainFileLog = fs.createWriteStream(path.normalize(argv.outputDirectory + "/Undetermined/" + getRunId() + "/demultiplex.log"), {'flags': 'a', encoding: "utf8", mode: 0666});
var mainLog = {
	file: mainFileLog,
	"put": function(message) {
		if (argv.verbose) {
			util.print(message);
		}
		this.file.write(message);
	},
	"info": function(message) {
		if (argv.verbose) {
			console.log("INFO: " + message);
		}
		this.file.write("\nINFO: " + message);
	},
	"error": function(message) {
		console.error("ERROR: " + message);
		this.file.write("\nERROR: " + message);
	}
};

//Create temp directory
wrench.mkdirSyncRecursive(argv.tempDirectory, 0777);

//Read sample sheet and send for parsing, send parsed info to "start" function
fs.readFile(argv.samplesheet, "utf8", function(err, sampleSheet) {
	if (err) {
		mainLog.error(err);
	} else {
		mainSampleSheet = sampleSheet;
		parseSheetToBarcodes(sampleSheet, start);
	}
});

//Send splitted barcodes to run function, recieve result and send it to "handleRunResult"
function start(err, barcodeFiles, barcodes) {
	if (err) {
		mainLog.error(err);
	} else {
		run(barcodeFiles, barcodes, handleRunResult);
	}
}

//Everything is done
function handleRunResult(err, result) {
	if (err) {
		mainLog.error(err)
	} else {
		if (argv.verbose) {
			console.log("Final results: \n\t- " + result.join("\n\t- "));
		}
		mainLog.put("\nFinal results: \n\t- " + result.join("\n\t- ") + "\n")
	}
}


function run(barcodeFiles, barcodes, callback) {

	var counter = 0;
	
	for (lane in barcodeFiles) {
		
		if (omitLane(lane)) {
			mainLog.info("Omitting lane " + lane);
		} else {

			var debug = "; FIRST_TILE=1101; TILE_LIMIT=1";
			if (!argv.debug) {
				debug = "";
			}

			var readIndex = extractFirstAttribute(barcodes[lane], "readstring").replace(",", "");
			createOutputDirectories(barcodes[lane]);

			var undeterminedDir = path.normalize(argv.outputDirectory + "/Undetermined/" + getRunId() + "/");
			wrench.mkdirSyncRecursive(undeterminedDir, 0777);

			var argsIllumina2bam = ("-d64; -Xmx" + argv.im + "; -jar; " + __dirname + "/illumina2bam.jar; I=" + intensitiesDirectory + "; L=" + lane + "; O=/dev/stdout; PF=false; RI=" + readIndex + "; QUIET=true; COMPRESSION_LEVEL=0" + debug + "; TEMP_DIR=" + argv.tempDirectory).split("; ");
			var argsBamIndexDecoder = ("-d64; -Xmx" + argv.ib + "; -jar; " + __dirname + "/BamIndexDecoder.jar; I=/dev/stdin; OUTPUT_DIR=" + path.normalize(argv.outputDirectory) + "; OUTPUT_FORMAT=" + argv.f + "; OUTPUT_PREFIX=na; BARCODE_FILE=" + path.normalize(barcodeFiles[lane]) + "; M=" + path.normalize(argv.outputDirectory) + "/Undetermined/" + getRunId() + "/demultiplex_metrics_" + lane + ".txt; MAX_MISMATCHES=" + argv.m + "; MIN_MISMATCH_DELTA=" + argv.d + "; MAX_NO_CALLS=" + argv.n).split("; ");

			//Perform sanity checks
			var sanity = sanityChecks.checkEverything(barcodes[lane]);

			if (sanity.result) {
				//Everything seems ok, begin parsing and demultiplexing for this lane
				counter++;
				var results = [];

				//Create log for this lane
				var fileLog = fs.createWriteStream(argv.outputDirectory + "/Undetermined/" + getRunId() + "/demultiplex_" + lane + ".log", {'flags': 'a', encoding: "utf8", mode: 0666});
				var log = {
					file: fileLog,
					"put": function(message) {
						this.file.write(message);
						mainLog.put(message);
					},
					"info": function(message) {
						this.file.write("\nINFO: " + message);
						mainLog.info(message);
					},
					"error": function(message) {
						this.file.write("\nERROR: " + message);
						mainLog.error(message);
					}
				};

				decodeLane(argsIllumina2bam, argsBamIndexDecoder, lane, log, function(err, result) {
					counter--;

					if (err) {
						mainLog.error(err);
						results.push(err);
					} else {
						mainLog.info(result);
						results.push(result);
					}

					if (counter === 0) {
						//Everything is done

						//Copy non lane specific files
						var copyToUndeterminedDir = {
							"/../../../RunInfo.xml": "RunInfo.xml",
							"/../../../runParameters.xml": "runParameters.xml",
							"/../../../InterOp/": "InterOp/",
							"/../../../First_Base_Report.htm": "First_Base_Report.htm",
							"/../../../Config/": "Config/",
							"/../../../Recipe/": "Recipe/",
							"/../../Status.htm": "Status.htm",
							"/../../Status_Files/": "Status_Files/",
							"/../../reports/": "reports/",
							"/../RTAConfiguration.xml": "RTAConfiguration.xml",
							"/../config.xml": "IntensitiesConfig.xml",
							"./config.xml": "BaseCallsConfig.xml",
							"./BustardSummary.xml": "BustardSummary.xml",
							"./BustardSummary.xsl": "BustardSummary.xsl"
						}

						var undeterminedDir = path.normalize(argv.outputDirectory + "/Undetermined/" + getRunId() + "/");

						copyFilesAndDirs(copyToUndeterminedDir, argv.basecallsDirectory, undeterminedDir, mainLog);

						var copyToRunIdDir = {
							"/../../../RunInfo.xml": "RunInfo.xml",
							"/../../../runParameters.xml": "runParameters.xml",
							"/../../../InterOp/": "InterOp/"
						}

						//Copy to each output dir
						for (outputDir in outputDirs) {
							copyFilesAndDirs(copyToRunIdDir, argv.basecallsDirectory, outputDir, mainLog);
						}

						//Create project specific samplesheets
						createOutputSampleSheets(mainSampleSheet, outputDirs);

						//Remove temp directory
						wrench.rmdirSyncRecursive(argv.tempDirectory, false);

						callback(null, results);
					}
				});
			} else {
				callback("Problem with barcode file " + barcodeFiles[lane] + " in lane " + lane + ": " + sanity.message);
			}
			
		}
		
	}
}

argv.omitLanes = argv.omitLanes.toString().replace(/\s/g, "").split(",");

function omitLane(lane) {
	var omit = false;
	argv.omitLanes.forEach(function(laneNr) {
		if (parseInt(laneNr) === parseInt(lane)) {
			omit = true;
			return false;
		}
	});
	return omit;
}

function decodeLane(argsIllumina2bam, argsBamIndexDecoder, lane, log, callback) {
	
	var illumina2bam = spawn('java', argsIllumina2bam, {cwd: process.cwd, env: process.env, setsid: true});
	var bamIndexDecoder = spawn('java', argsBamIndexDecoder, {cwd: process.cwd, env: process.env, setsid: true});

	illumina2bam.stdout.on('data', function (data) {
		bamIndexDecoder.stdin.write(data);
	});

	illumina2bam.stderr.on('data', function (data) {
		log.put(data);
	});

	illumina2bam.on('exit', function (code) {
		if (code !== 0) {
			log.error('illumina2bam process exited with code ' + code);
		}
		bamIndexDecoder.stdin.end();
	});

	bamIndexDecoder.stderr.on('data', function (data) {
		log.put(data);
	});

	bamIndexDecoder.on('exit', function (code) {
		if (code !== 0) {
			log.error('bamIndexDecoder process exited with code ' + code);
			callback('bamIndexDecoder process exited with code ' + code, "");
		} else {

			//Copy some files
			var copyToUndeterminedDir = {};
			copyToUndeterminedDir["[temp]/intensitiesconfig_lane_" + lane + ".xml"] = "IntensitiesConfig_lane_" + lane + ".xml";
			copyToUndeterminedDir["[temp]/basecallsconfig_lane_" + lane + ".xml"] = "BaseCallsConfig_lane_" + lane + ".xml";
			copyToUndeterminedDir["[temp]/barcodes_" + lane + ".txt"] = "barcodes_" + lane + ".txt";

			var undeterminedDir = path.normalize(argv.outputDirectory + "/Undetermined/" + getRunId() + "/");
			copyFilesAndDirs(copyToUndeterminedDir, argv.basecallsDirectory, undeterminedDir, log);

			//Copy metrics files
			for(outputDir in outputDirs) {
				var projectName = outputDir.split("/");
				projectName = projectName[projectName.length - 3];
				if (projectExistsInLane(projectName, lane)) {
					var copyToRunDir = {};
					copyToRunDir[path.normalize(argv.outputDirectory) + "/Undetermined/" + getRunId() + "/demultiplex_metrics_" + lane + ".txt"] = "demultiplex_metrics_" + lane + ".txt";
					copyFilesAndDirs(copyToRunDir, "", outputDir, log);
				}
				
			}
		
			callback(null, "Finished lane " + lane);
				
		}

	});
}

function projectExistsInLane(projectName, lane) {
	var sampleSheet = mainSampleSheet;

	var lines = sampleSheet.split("\n").filter(function(line) {
		return (line.length > 0 && line.substr(0, 1) !== "#");
	});
	
	var headers = sampleSheet.split("\n")[0].split("\t").map(function(header) {
		if (header.substr(0, 1) === "#") {
			header = header.substr(1);
		}
		return header;
	});
	
	var projectIndex = getIndexForHeaderName("Project (pr)", headers);
	var laneIndex = getIndexForHeaderName("Lane", headers);
	var exists = false;
	
	if (projectIndex === null || laneIndex === null) {
		console.error("Could not find \"Project (pr)\" or \"Lane\" header in samplesheet");
	} else {
		lines.forEach(function(line) {
			line = line.split("\t");
			if (line[laneIndex] === lane && line[projectIndex] === projectName) {
				exists = true;
				return false;
			}
		});
	}
	return exists;
}

function createOutputSampleSheets(sampleSheet, outputDirs) {
	//Create new samplesheets in the output dirs
	var lines = sampleSheet.split("\n").filter(function(line) {
		return (line.length > 0 && line.substr(0, 1) !== "#");
	});
	
	var headers = sampleSheet.split("\n")[0].split("\t").map(function(header) {
		if (header.substr(0, 1) === "#") {
			header = header.substr(1);
		}
		return header;
	});
	
	var projectIndex = getIndexForHeaderName("Project (pr)", headers);
	
	if (projectIndex === null) {
		console.error("Could not find \"Project (pr)\" header in samplesheet");
	} else {
		for (outputDir in outputDirs) {
			var projectName = outputDir.split("/");
			projectName = projectName[projectName.length - 3];
			//Create samplesheet file
			var sampleSheetFileOut = fs.createWriteStream(path.normalize(outputDir + "/" + projectName + "_" + getRunId() + "_samplesheet.txt"), {'flags': 'w', encoding: "utf8", mode: 0666});
			sampleSheetFileOut.write("#" + headers.join("\t") + "\n");
			//Start checking each line
			lines.forEach(function(line) {
				if (line.split("\t")[projectIndex] === projectName) {
					sampleSheetFileOut.write(line + "\n");
				}
			});
		}
	}
}

function getIndexForHeaderName(headerName, headers) {
	var resultIndex = null;
	headers.forEach(function(header, index) {
		if (header.toLowerCase() === headerName.toLowerCase()) {
			resultIndex = index;
			return false;
		}
	});
	return resultIndex;
}

function parseSheetToBarcodes(sampleSheet, callback) {
	var lines = sampleSheet.split("\n");
	var headers = normalizeHeaders(lines[0].split("\t"));
	
	lines = lines.splice(1);

	var lanes = splitLinesByLanes(headers, lines);

	var barcodes = {};
	for (lane in lanes) {
		var contents = [headers.join("\t")];
		lanes[lane].forEach(function(line) {
			contents.push(line.join("\t"));
		});
		barcodes[lane] = contents.join("\n");
	}

	//Write to files
	var barcodeFiles = {};
	for (barcode in barcodes) {
		var barcodeFilePath = path.normalize(argv.tempDirectory + "/barcodes_" + barcode + ".txt");
		fs.writeFileSync(barcodeFilePath, barcodes[barcode], "utf8");
		barcodeFiles[barcode] = barcodeFilePath;
	}

	callback(null, barcodeFiles, barcodes);
}

function splitLinesByLanes(headers, lines) {
	var lanes = {};

	var laneIndex = null;
	for (var i = 0; i < headers.length; i++) {
		if (headers[i] === "lane") {
			laneIndex = i;
			break;
		}
	}

	lines.filter(function(line) {
		return (line.length > 0 && line.substr(0, 1) !== "#");
	}).forEach(function(line) {
		line = line.split("\t");
		if (lanes[line[laneIndex]] == undefined) {
			lanes[line[laneIndex]] = [];
		}
		lanes[line[laneIndex]].push(line);
	});
	
	return lanes;
}

function normalizeHeaders(headers) {

	if (headers.length > 0 && headers[0].substr(0, 1) === "#") {
		headers[0] = headers[0].substr(1);
	}

	headers = headers.map(function(header) {
		header = header.toLowerCase();
		if (header.indexOf("(") > 0 && header.indexOf(")") > 0) {
			var shortName = header.split("(")[1].split(")")[0];
			var longName = header.split("(")[0].replace(" ", "");
			header = shortName + ":" + longName;
		}
		
		if (settings.replaceHeaders[header] !== undefined) {
			header = settings.replaceHeaders[header];
		}
		
		return header;
	});

	return headers;
}

function getRunId() {
	if (runId === null || runId === undefined) {
		var runInfoXml = fs.readFileSync(path.normalize(argv.basecallsDirectory + "/../../../RunInfo.xml"), "utf8");
		runId = runInfoXml.split("<Run Id=\"")[1];
		runId = runId.split("\"")[0];
	}

	return runId;
}

function extractFirstAttribute(barcodes, name) {
	var lines = barcodes.split("\n");
	var header = lines[0].split("\t");
	lines = lines.splice(1);
	var index = null;
	for (var i = 0; i < header.length; i++) {
		if (header[i] === name) {
			index = i;
			break;
		}
	}
	return lines[0].split("\t")[index];
}

function createOutputDirectories(barcodes) {
	var lines = barcodes.split("\n");
	var headers = lines[0].split("\t");
	lines = lines.splice(1);
	var projectIndex = getIndexForHeaderName("pr:project", headers);

	if (projectIndex !== null) {
		lines.forEach(function(line) {
			var projectName = line.split("\t")[projectIndex];
			var outputDir = path.normalize(argv.outputDirectory + "/" + projectName + "/" + getRunId() + "/");
			if (outputDirs[outputDir] === undefined) {
				wrench.mkdirSyncRecursive(outputDir, 0777);
				outputDirs[outputDir] = true;
			}
		});
	}
}

function copyFilesAndDirs(list, fromDir, toDir, log) {
	for (name in list) {
		var fromPath = "";
		if (name.indexOf("[temp]") > -1) {
			fromPath = path.normalize(name.replace("[temp]", argv.tempDirectory));
		} else {
			fromPath = path.normalize(fromDir + "/" + name);
		}
		var toPath = path.normalize(toDir + "/" + list[name]);
		if (path.existsSync(fromPath)) {
			var object = fs.lstatSync(fromPath);
			if (object.isDirectory()) {
				log.info("Copying folder " + fromPath + " to " + toPath);
				wrench.copyDirSyncRecursive(fromPath, toPath);
			} else if (object.isFile()) {
				log.info("Copying file " + fromPath + " to " + toPath);
				copyFileSync(fromPath, toPath);
			} else {
				log.error("Skipping copy of " + fromPath + ", is neither dir nor file.");
			}
		} else {
			log.error("Skipping copy of " + fromPath + ", does not exist.");
		}
	}
}

function copyFileSync(srcFile, destFile) {
	var BUF_LENGTH, buff, bytesRead, fdr, fdw, pos;
	BUF_LENGTH = 64 * 1024;
	buff = new Buffer(BUF_LENGTH);
	
	fdr = fs.openSync(srcFile, 'r');
	fdw = fs.openSync(destFile, 'w');
	bytesRead = 1;
	pos = 0;
	while (bytesRead > 0) {
		bytesRead = fs.readSync(fdr, buff, 0, BUF_LENGTH, pos);
		fs.writeSync(fdw, buff, 0, bytesRead);
		pos += bytesRead;
	}
	fs.closeSync(fdr);
	return fs.closeSync(fdw);
}

var sanityChecks = {
	checkEverything: function(barcodes) {

		var sanity = {result: true, message: null};
		var lines = barcodes.split("\n");
		var headers = lines[0].split("\t");

		lines = lines.splice(1);

		if (sanity.result) {
			//Check if required values exist
			sanity = sanityChecks.hasRequiredValues(headers, lines);
		}

		if (sanity.result) {
			//Check if barcode appears twice in the same lane
			sanity = sanityChecks.uniqueBarcodesInLane(headers, lines);
		}	

		if (sanity.result) {
			//Check if barcode length is equal
			sanity = sanityChecks.equalBarcodeLength(headers, lines);
		}

		if (sanity.result) {
			//Check if ReadStrings are equal for all barcodes in this lane
			sanity = sanityChecks.equalReadStrings(headers, lines);
		}

		if (sanity.result) {
			//Check if FCID are equal for all barcodes in this lane
			sanity = sanityChecks.equalFCID(headers, lines);
		}

		if (sanity.result && !argv.force) {
			//Check if library already exists
			sanity = sanityChecks.checkLibrary(headers, lines);
		}

		return sanity;
	},
	
	uniqueBarcodesInLane: function(headers, lines) {
		var sanity = {result: true, message: null};
		var barcodeIndex = getIndexForHeaderName("barcode_sequence", headers);

		var existingBarcodes = {};
		lines.forEach(function(line, index) {
			line = line.split("\t");
			var currentBarcode = line[barcodeIndex];
			if (existingBarcodes[currentBarcode] !== undefined) {
				sanity.result = false;
				sanity.message = "Barcode \"" + currentBarcode + "\" appears more than once.";
				return false;
			} else {
				existingBarcodes[currentBarcode] = true;
			}
		});
		return sanity;
	},

	equalReadStrings: function(headers, lines) {
		var sanity = {result: true, message: null};
		var readstringIndex = getIndexForHeaderName("readstring", headers);

		var firstReadString = null;
		lines.forEach(function(line, index) {
			line = line.split("\t");
			var currentReadString = line[readstringIndex];
			if (firstReadString === null) {
				firstReadString = currentReadString;
			} else if (currentReadString !== firstReadString) {
				sanity.result = false;
				sanity.message = "ReadString \"" + currentReadString + "\" differs from first ReadString (" + firstReadString + ")";
				return false;
			}
		});
		return sanity;
	},

	equalFCID: function(headers, lines) {
		var sanity = {result: true, message: null};
		var fcidIndex = getIndexForHeaderName("fcid", headers);

		var firstFCID = null;
		lines.forEach(function(line, index) {
			line = line.split("\t");
			var currentFCID = line[fcidIndex];
			if (firstFCID === null) {
				firstFCID = currentFCID;
			} else if (currentFCID !== firstFCID) {
				sanity.result = false;
				sanity.message = "FCID \"" + currentFCID + "\" differs from first FCID (" + firstFCID + ")";
				return false;
			}
		});
		return sanity;
	},

	equalBarcodeLength: function(headers, lines) {
		var sanity = {result: true, message: null};
		var barcodeIndex = getIndexForHeaderName("barcode_sequence", headers);

		var firstBarcode = null;
		lines.forEach(function(line, index) {
			line = line.split("\t");
			var currentBarcode = line[barcodeIndex];
			if (firstBarcode === null) {
				firstBarcode = currentBarcode;
			} else if (currentBarcode.length !== firstBarcode.length) {
				sanity.result = false;
				sanity.message = "Barcode \"" + currentBarcode + "\" differs in length in comparison to first barcode (" + firstBarcode + ")";
				return false;
			}
		});
		return sanity;
	},

	checkLibrary: function(headers, lines) {
		var sanity = {result: true, message: null};

		var libraryIndex = getIndexForHeaderName("library_name", headers);
		var projectIndex = getIndexForHeaderName("pr:project", headers);

		lines.forEach(function(line, index) {
			line = line.split("\t");
			var libraryName = line[libraryIndex];
			var projectName = line[projectIndex];

			//Find project folder
			var projectDirPath = path.normalize(argv.outputDirectory + "/" + projectName + "/");
			var runFolders = fs.readdirSync(projectDirPath);
			runFolders.forEach(function(runFolderName, index) {
				var runFolderPath = path.normalize(projectDirPath + "/" + runFolderName);
				var runFolderStat = fs.lstatSync(runFolderPath);
				if (runFolderStat.isDirectory()) {
					var files = fs.readdirSync(runFolderPath);
					files.forEach(function(fileName) {
						var filePath = path.normalize(runFolderPath + "/" + fileName);
						if (fileName.indexOf("_") > -1 && fileName.length > 4 && (fileName.substr(fileName.length - 4) === ".sam" || fileName.substr(fileName.length - 4) === ".bam")) {
							if (fileName.split("_")[0] === libraryName) {
	 							sanity.result = false;
								sanity.message = "It appears that the library \"" + libraryName + "\" already exists in project \"" + projectName + "\" (Path: " + filePath + "). Use --force flag if you wish to override this check.";
								return false;
							}
						}
					});
				}
				if (!sanity.result) {
					return false;
				}
			});
		});
		return sanity;
	},

	hasRequiredValues: function(headers, lines) {
		var sanity = {result: true, message: null};

		settings.requiredValues.forEach(function(headerName, index) {
			var valueIndex = getIndexForHeaderName(headerName, headers);
			if (valueIndex !== null) {
				//Check if each row contains the required value
				for (var i = 0; i < lines.length; i++) {
					var line = lines[i].split("\t");
					if (line[valueIndex] === undefined || line[valueIndex] === null || line[valueIndex] === "") {
						sanity.result = false;
						sanity.message = "Attribute " + headerName + " on line " + (i + 2) + " has no value."
						return false; 
						break;
					}
				}
			} else {
				//Fix name of header
				var actualHeaderName = headerName;
				for (originalHeader in settings.replaceHeaders) {
					if (actualHeaderName === settings.replaceHeaders[originalHeader]) {
						actualHeaderName = originalHeader;
					}
				}
				if (actualHeaderName.indexOf(":") > -1) {
					actualHeaderName = actualHeaderName.substr(4) + " (" + actualHeaderName.substr(0, 2) + ")";
				}

				sanity.result = false;
				sanity.message = "Header " + headerName + " (" + actualHeaderName + " in samplesheet " + argv.samplesheet + ") does not exist.";
				return false;
			}
		});

		return sanity;
	}
	
};
