#!/usr/bin/env node

var util  = require('util'),
	spawn = require('child_process').spawn,
	path = require('path'),
	wrench = require('wrench');
//	winston = require('winston');

var argv = require('optimist')
	    .usage('Wrapper for performing illumina bcl to bam encoding and demultiplexing.\nUsage: $0')
	    .demand('s')
	    .demand('i')
	    .demand('o')
		.default('f', 'bam')
	    .alias('v', 'verbose')
	    .alias('s', 'samplesheet')
	    .alias('i', 'inputDirectory')
	    .alias('o', 'outputDirectory')
		.options('f', {
			default : 'bam',
		})
	    .describe('s', 'Samplesheet')
	    .describe('i', 'Input directory, should contain \'./Data/Intensities/\'')
	    .describe('o', 'Output directory')
	    .describe('f', 'Output format [bam|sam], default to \'bam\'')
	    .describe('v', 'Verbose output')
	    .argv
	;

if (argv.f !== "bam") {
	argv.f = "sam";
}

var intensitiesDirectory = path.normalize(argv.inputDirectory + "/Data/Intensities/");

var fs = require("fs");

fs.readFile(argv.samplesheet, "utf8", function(err, sampleSheet) {
	if (err) {
		console.log("Error: " + err);
	} else {
		parseSheetToBarcodes(sampleSheet, start);
	}
});

function start(err, barcodeFiles, barcodes) {
	if (err) {
		console.log("Error: " + err);
	} else {
		run(barcodeFiles, barcodes, handleResult);
	}
}

function handleResult(err, result) {
	if (err) {
		console.log("Error: " + err);
	} else {
		console.log(result);
	}
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
		var barcodeFilePath = intensitiesDirectory + "barcodes_" + barcode + ".txt";
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

	lines.forEach(function(line) {
		if (line.substr(0, 1) !== "#") {
			line = line.split("\t");
			if (lanes[line[laneIndex]] == undefined) {
				lanes[line[laneIndex]] = [];
			}
			lanes[line[laneIndex]].push(line);
		}
	});
	
	return lanes;
}

function normalizeHeaders(headers) {

	var replace = {
		"library": "library_name",
		"sample": "sample_name",
		"index": "barcode_sequence"
	}

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
		
		if (replace[header] !== undefined) {
			header = replace[header];
		}
		
		return header;
	});

	return headers;
}
var runId = null;
function getRunId() {
	if (runId === null) {
		var runInfoXml = fs.readFileSync(intensitiesDirectory + "RunInfo.xml", "utf8");
		runId = runInfoXml.split("<Run Id=\"")[1];
		runId = runId.split("\"")[0];
	}

	return runId;
}

function extractAttribute(barcodes, name) {
	var lines = barcodes.split("\n");
	var header = lines[0];
	lines = lines.splice(1);
	var index = null;
	header = header.split("\t");
	for (var i = 0; i < header.length; i++) {
		if (header[i] === name) {
			index = i;
			break;
		}
	}
	return lines[0].split("\t")[index];
}

function run(barcodeFiles, barcodes, callback) {

	var counter = 0;
	for (lane in barcodeFiles) {
		
		var debug = "; FIRST_TILE=1101; TILE_LIMIT=1";
//		debug = "";
		var readIndex = extractAttribute(barcodes[lane], "readstring").replace(",", "");
		var project = extractAttribute(barcodes[lane], "pr:project");
		var outputDir = argv.outputDirectory + "/" + project + "/" + getRunId() + "/";
		outputDir = path.normalize(outputDir);
		
		wrench.mkdirSyncRecursive(outputDir, 0777);

		var argsIllumina2bam = ("-d64; -Xmx2048m; -jar; illumina2bam.jar; I=" + intensitiesDirectory + "; L=" + lane + "; O=/dev/stdout; PF=false; RI=" + readIndex + "; QUIET=true; COMPRESSION_LEVEL=0" + debug).split("; ");
		var argsBamIndexDecoder = ("-d64; -Xmx1024m; -jar; BamIndexDecoder.jar; I=/dev/stdin; OUTPUT_DIR=" + outputDir + "; OUTPUT_FORMAT=" + argv.f + "; OUTPUT_PREFIX=notused; BARCODE_FILE=" + barcodeFiles[lane] + "; M=" + outputDir + "/metrics.m").split("; ");

		decodeLane(argsIllumina2bam, argsBamIndexDecoder, lane, outputDir, function(err, result) {
			if (err) {
				return callback(err);
			}
			callback(null, result)
		});
	}
}

function decodeLane(argsIllumina2bam, argsBamIndexDecoder, lane, outputDir, callback) {
	
	var illumina2bam = spawn('java', argsIllumina2bam, {cwd: __dirname, env: process.env, setsid: true});
	var bamIndexDecoder = spawn('java', argsBamIndexDecoder, {cwd: __dirname, env: process.env, setsid: true});

	var fileLog = fs.createWriteStream(outputDir + "lane_" + lane + ".log", {'flags': 'a', encoding: "utf8", mode: 0666});
	var log = {
				file: fileLog,
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

	bamIndexDecoder.stdout.on('data', function (data) {
	//	console.log(data);
	});

	bamIndexDecoder.stderr.on('data', function (data) {
//		util.print(data);
		log.put(data);
	});

	bamIndexDecoder.on('exit', function (code) {
		if (code !== 0) {
			log.error('bamIndexDecoder process exited with code ' + code);
		} else {
			//Perform cleanup
			var undeterminedDir = path.normalize(argv.outputDirectory + "/Undetermined/" + getRunId() + "/");

			wrench.mkdirSyncRecursive(undeterminedDir, 0777);

			var files = fs.readdirSync(outputDir);
			for (var i = 0; i < files.length; i++) {
				var fileName = files[i];
				if (fileName.indexOf("Undetermined_") === 0 || fileName.indexOf("Controls_") === 0 && fileName.split("_")[2].substr(0, 1) === lane) {
					//console.log("From: " + outputDir + fileName + " To: " + undeterminedDir + fileName);
					fs.renameSync(outputDir + fileName, undeterminedDir + fileName);
				}
			}

			//copy some files
			var copyToUndeterminedDir = {
				"Data/Intensities/RunInfo.xml": "RunInfo.xml",
				"runParameters.xml": "runParameters.xml",
				"InterOp/": "InterOp/",
				"First_Base_Report.htm": "First_Base_Report.htm",
				"Config/": "Config/",
				"Recipe/": "Recipe/",
				"Data/Status.htm": "Status.htm",
				"Data/Status_Files/": "Status_Files/",
				"Data/reports/": "reports/",
				"Data/Intensities/RTAConfiguration.xml": "RTAConfiguration.xml",
				"Data/Intensities/config.xml": "IntensitiesConfig.xml",
				"Data/Intensities/BaseCalls/config.xml": "BaseCallsConfig.xml",
				"Data/Intensities/BaseCalls/BustardSummary.xml": "BustardSummary.xml",
				"Data/Intensities/BaseCalls/BustardSummary.xsl": "BustardSummary.xsl"
			}
			copyToUndeterminedDir["Data/Intensities/config_lane_" + lane + ".xml"] = "IntensitiesConfig_lane_" + lane + ".xml";
			copyToUndeterminedDir["Data/Intensities/BaseCalls/config_lane_" + lane + ".xml"] = "BaseCallsConfig_lane_" + lane + ".xml";

			copyFilesAndDirs(copyToUndeterminedDir, argv.inputDirectory, undeterminedDir, log);

			var copyToRunIdDir = {
				"Data/Intensities/RunInfo.xml": "RunInfo.xml",
				"runParameters.xml": "runParameters.xml",
				"InterOp/": "InterOp/"
			}

			copyFilesAndDirs(copyToRunIdDir, argv.inputDirectory, outputDir, log);

			callback(null, "Done");
				
		}

	});
	
}

function copyFilesAndDirs(list, fromDir, toDir, log) {
	for (name in list) {
		var fromPath = fromDir + name;
		var toPath = toDir + list[name];
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

