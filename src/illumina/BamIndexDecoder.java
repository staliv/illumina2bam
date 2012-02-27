/*
 * Copyright (C) 2011 GRL
 *
 * This library is free software. You can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package illumina;

import net.sf.picard.cmdline.Option;
import net.sf.picard.cmdline.StandardOptionDefinitions;
import net.sf.picard.cmdline.Usage;
import net.sf.picard.io.IoUtil;
import net.sf.picard.metrics.MetricsFile;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.picard.util.Log;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMFileWriter;
import net.sf.samtools.SAMFileWriterFactory;
import net.sf.samtools.SAMReadGroupRecord;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMRecordIterator;

/**
 * This class is used decode the multiplexed bam file.
 * 
 * Each read in BAM file will be marked in its read name and read group,
 * There is an option to output bam file by tag.
 * 
 * The bar code list can be passed in through command line
 * or by a file with extra information barcode name, library name, sample name and description, which are separated by tab
 * this file must have a header: barcode_sequence	barcode_name	library_name	sample_name	description
 * 
 * The read group will be changed and re-added in.
 * 
 * @author gq1@sanger.ac.uk
 * 
 */

public class BamIndexDecoder extends Illumina2bamCommandLine {
    
    private final Log log = Log.getInstance(BamIndexDecoder.class);
    
    private final String programName = "BamIndexDecoder";
    
    private final String programDS = "A command-line tool to decode multiplexed bam file";
   
    @Usage(programVersion= version)
    public final String USAGE = this.getStandardUsagePreamble() + this.programDS + ". ";
   
    @Option(shortName= StandardOptionDefinitions.INPUT_SHORT_NAME, doc="The input SAM or BAM file to decode.")
    public File INPUT;
    
    @Option(shortName=StandardOptionDefinitions.OUTPUT_SHORT_NAME, doc="The output file after decoding.", mutex = {"OUTPUT_DIR"} )
    public File OUTPUT;
    
    @Option(doc="The output directory for bam files for each barcode if you want to split the output", mutex = {"OUTPUT"})
    public File OUTPUT_DIR;
    
    @Option(doc="The prefix for bam or sam file when you want to split output by barcodes", mutex = {"OUTPUT"})
    public String OUTPUT_PREFIX;
    
    @Option(doc="The extension name for split file when you want to split output by barcodes: bam or sam", mutex = {"OUTPUT"})
    public String OUTPUT_FORMAT;
    
    @Option(doc="The tag name used to store barcode read in bam records")
    public String BARCODE_TAG_NAME = "BC";

    @Option(doc="Barcode sequence.  These must be unique, and all the same length.", mutex = {"BARCODE_FILE"})
    public List<String> BARCODE = new ArrayList<String>();

    @Option(doc="Tab-delimited file of barcode sequences, and optionally barcode name and library name.  " +
            "Barcodes must be unique, and all the same length.  Column headers must be 'barcode_sequence', " +
            "'barcode_name', and 'library_name'.", mutex = {"BARCODE"})
    public File BARCODE_FILE;

    @Option(doc="Per-barcode and per-lane metrics written to this file.", shortName = StandardOptionDefinitions.METRICS_FILE_SHORT_NAME)
    public File METRICS_FILE;

    @Option(doc="Maximum mismatches for a barcode to be considered a match.")
    public int MAX_MISMATCHES = 1;

    @Option(doc="Minimum difference between number of mismatches in the best and second best barcodes for a barcode to be considered a match.")
    public int MIN_MISMATCH_DELTA = 1;

    @Option(doc="Maximum allowable number of no-calls in a barcode read before it is considered unmatchable.")
    public int MAX_NO_CALLS = 2;

    private int barcodeLength;
    
    private IndexDecoder indexDecoder;
    
    private SAMFileWriter out;
    private SAMFileWriter controlsOut;
    private SAMFileWriter filterOut;
    private HashMap<String, SAMFileWriter> outputList;
    private HashMap<String, SAMFileWriter> outputFilterList;
    private HashMap<String, String> barcodeNameList;
    
    public BamIndexDecoder() {
    }

    @Override
    protected int doWork() {
        
        this.log.info("Checking input and output file");
        IoUtil.assertFileIsReadable(INPUT);
        if(OUTPUT != null){
            IoUtil.assertFileIsWritable(OUTPUT);
        }
        if(OUTPUT_DIR != null){
            IoUtil.assertDirectoryIsWritable(OUTPUT_DIR);
        }
        IoUtil.assertFileIsWritable(METRICS_FILE);
        
        log.info("Open input file: " + INPUT.getName());
        final SAMFileReader in  = new SAMFileReader(INPUT);        
        final SAMFileHeader header = in.getFileHeader();
        
        this.generateOutputFile(header);
                
        log.info("Decoding records");        
        SAMRecordIterator inIterator = in.iterator();
        while(inIterator.hasNext()){
            
            String barcodeRead = null;

            SAMRecord record = inIterator.next();            
            String readName = record.getReadName();
            boolean isPaired = record.getReadPairedFlag();
            boolean isPf = ! record.getReadFailsVendorQualityCheckFlag();
            Object barcodeReadObject = record.getAttribute(this.BARCODE_TAG_NAME);
            if(barcodeReadObject != null){
                barcodeRead = barcodeReadObject.toString();
            }
            
            SAMRecord pairedRecord = null;
            
            if(isPaired){
                
                pairedRecord = inIterator.next();
                String readName2 = pairedRecord.getReadName();
                boolean isPaired2 = pairedRecord.getReadPairedFlag();
                
                if( !readName.equals(readName2) || !isPaired2 ){
                    throw new RuntimeException("The paired reads are not together: " + readName + " " + readName2);
                }
                
                Object barcodeReadObject2= pairedRecord.getAttribute(this.BARCODE_TAG_NAME);
                if(barcodeReadObject != null
                        && barcodeReadObject2 != null
                        && ! barcodeReadObject.equals(barcodeReadObject2) ){
                    
                    //throw new RuntimeException("barcode read bases are different in paired two reads: "
                    //        + barcodeReadObject + " " + barcodeReadObject2);
                } else if( barcodeRead == null && barcodeReadObject2 != null ){
                    barcodeRead = barcodeReadObject2.toString();
                }                
            }
            
            if(barcodeRead == null ){
                barcodeRead = "";
                isPf = true;
                //    throw new RuntimeException("No barcode read found for record: " + readName );
            }
            
            if(barcodeRead.length() < this.barcodeLength){
                throw new RuntimeException("The barcode read length is less than barcode length: " + readName );
            }else{            
                barcodeRead = barcodeRead.substring(0, this.barcodeLength);
            }

            IndexDecoder.BarcodeMatch match = this.indexDecoder.extractBarcode(barcodeRead, isPf);
            String barcode = match.barcode;
            
            if( match.matched ) {
               barcode = barcode.toUpperCase();
            } else {
               barcode = "undetermined";
            }
            
//            String barcodeName = this.barcodeNameList.get(barcode);

            record.setReadName(readName + "." + barcode);
            record.setAttribute("RG", record.getAttribute("RG") + "." + barcode);
            if (isPaired) {
                pairedRecord.setReadName(readName + "." + barcode);
                pairedRecord.setAttribute("RG", pairedRecord.getAttribute("RG") + "." + barcode);
            }
            
            boolean isControl = (record.getAttribute("XC") != null);
            boolean isFiltered = record.getReadFailsVendorQualityCheckFlag();

            //Write to file
            if (isControl) {
                this.controlsOut.addAlignment(record);
                if(isPaired){
                    this.controlsOut.addAlignment(pairedRecord);
                }
            } else if (isFiltered) {

                SAMFileWriter filterOut = (OUTPUT != null) ? this.filterOut : this.outputFilterList.get(barcode);

                filterOut.addAlignment(record);
                if(isPaired){
                    filterOut.addAlignment(pairedRecord);
                }
            } else {
                SAMFileWriter currentOut = (OUTPUT != null) ? this.out : this.outputList.get(barcode);

                currentOut.addAlignment(record);
                if(isPaired){
                    currentOut.addAlignment(pairedRecord);
                }
            }
            
        }
        
        this.closeOutputList();
        
        log.info("Decoding finished");
        
        
        log.info("Writing out metrics file");        
        final MetricsFile<IndexDecoder.BarcodeMetric, Integer> metrics = getMetricsFile();        
        indexDecoder.writeMetrics(metrics, METRICS_FILE);
        
        log.info("All finished");

        return 0;
    }
    
    public void generateOutputFile(SAMFileHeader header) {
        
        List<IndexDecoder.NamedBarcode> barcodeList = indexDecoder.getNamedBarcodes(); 

        String fcid = "unknown";
        String lane = "unknown";
        String runFolder = "unknown";

        this.barcodeNameList = new HashMap<String, String>();
        
        List<SAMReadGroupRecord> oldReadGroupList = header.getReadGroups();        
        List<SAMReadGroupRecord> fullReadGroupList = new ArrayList<SAMReadGroupRecord>();
        
        if (OUTPUT_DIR != null) {
            log.info("Open a list of output bam/sam file per barcode");
            outputList = new HashMap<String, SAMFileWriter>();
            outputFilterList = new HashMap<String, SAMFileWriter>();
        }

        for (int count = 0; count <= barcodeList.size(); count++) {

            String barcodeName = null;
            String barcode = null;
            IndexDecoder.NamedBarcode namedBarcode = null;
            List<SAMReadGroupRecord> readGroupList = new ArrayList<SAMReadGroupRecord>();

            if ( count != 0 ) {
                namedBarcode = barcodeList.get(count - 1);
                barcodeName = namedBarcode.barcodeName;
                barcode = namedBarcode.barcode;
                barcode = barcode.toUpperCase();
            }else{
                namedBarcode = new IndexDecoder.NamedBarcode("undetermined");
                barcode = "undetermined";
                barcodeName = "undetermined";
            }

            if (barcodeName == null || barcodeName.equals("")) {
                barcodeName = Integer.toString(count);
            }
            

            for(SAMReadGroupRecord r : oldReadGroupList){
                    
                    SAMReadGroupRecord newReadGroupRecord = new SAMReadGroupRecord(r.getId() + "." + barcode, r);
                    String pu = newReadGroupRecord.getPlatformUnit();

                    //Fetch fcid and lane from ID tag of RG-header
                    if (count == 0 && r.getId().contains(".")) {
                        fcid = r.getId().split("\\.")[0];
                        lane = r.getId().split("\\.")[1];
                    }
                    
                    //Fetch runFolderId from rf-tag of RG-header
                    if (count == 0 && r.getAttribute("rf") != null) {
                        runFolder = r.getAttribute("rf");
                        //Remove rf-tag from RG-header
                        r.setAttribute("rf", null);
                    }
                    
                    
                    if(namedBarcode != null){
                        if( namedBarcode.libraryName != null && !namedBarcode.libraryName.equals("") ){
                           newReadGroupRecord.setLibrary(namedBarcode.libraryName);
                        }
                        if( namedBarcode.sampleName !=null && !namedBarcode.sampleName.equals("") ){
                           newReadGroupRecord.setSample(namedBarcode.sampleName);
                        }
                        if(namedBarcode.description != null && !namedBarcode.description.equals("") ){
                            newReadGroupRecord.setDescription(namedBarcode.description);
                        }
                        if(namedBarcode.insertSize > -1){
                            newReadGroupRecord.setPredictedMedianInsertSize(namedBarcode.insertSize);
                        }
                        if(namedBarcode.sequencingCenter != null && !namedBarcode.sequencingCenter.equals("") ){
                            newReadGroupRecord.setSequencingCenter(namedBarcode.sequencingCenter);
                        }

                        //Add possible endUserTags
                        if (namedBarcode.endUserTags != null && !(namedBarcode.endUserTags == null) && !namedBarcode.endUserTags.isEmpty()) {
                            for (final String tagName : namedBarcode.endUserTags.keySet()) {
                                final String tagValue = namedBarcode.endUserTags.get(tagName);
                                if (tagValue != null && !tagValue.equals("")) {
                                    newReadGroupRecord.setAttribute(tagName, tagValue);
                                }
                            }
                        }
                    }
                    readGroupList.add(newReadGroupRecord);
            }
            fullReadGroupList.addAll(readGroupList);


            if (OUTPUT_DIR != null) {
                
                if (count != 0 && !namedBarcode.flowCellId.equals(fcid) && !fcid.equals("unknown")) {
                    throw new RuntimeException("FCID \"" + namedBarcode.flowCellId + "\" from barcode file differs from FCID in current sample: \"" + fcid + "\"");
                }
                
                //Base directories on /project/run_folder_id/
                
                //Library_FCID_Lane_Index_pf.bam (for reads that pass the filter)
                String barcodeBamOutputName = OUTPUT_DIR
                        + File.separator
                        + namedBarcode.project
                        + File.separator
                        + runFolder
                        + File.separator
                        + namedBarcode.libraryName
                        + "_"
                        + namedBarcode.flowCellId
                        + "_"
                        + namedBarcode.lane
                        + "_"
                        + ((!"".equals(namedBarcode.barcode)) ? namedBarcode.barcode + "_" : "")
                        + "pf"
                        + "."
                        + OUTPUT_FORMAT;
                
                if (count == 0) {
                    barcodeBamOutputName = OUTPUT_DIR
                        + File.separator
                        + namedBarcode.project
                        + File.separator
                        + runFolder
                        + File.separator
                        + "Undetermined_"
                        + fcid
                        + "_"
                        + lane
                        + "_pf"
                        + "."
                        + OUTPUT_FORMAT;                    
                }

                final SAMFileHeader outputHeader = header.clone();
                outputHeader.setReadGroups(readGroupList);
                this.addProgramRecordToHead(outputHeader, this.getThisProgramRecord(programName, programDS));
                final SAMFileWriter outPerBarcode = new SAMFileWriterFactory().makeSAMOrBAMWriter(outputHeader, true, new File(barcodeBamOutputName));
                outputList.put(barcode, outPerBarcode);

                //Library_FCID_Lane_Index_non_pf.bam (for reads that don't pass the filter)
                String barcodeFilterBamOutputName = OUTPUT_DIR
                        + File.separator
                        + namedBarcode.project
                        + File.separator
                        + runFolder
                        + File.separator
                        + namedBarcode.libraryName
                        + "_"
                        + namedBarcode.flowCellId
                        + "_"
                        + namedBarcode.lane
                        + "_"
                        + ((!"".equals(namedBarcode.barcode)) ? namedBarcode.barcode + "_" : "")
                        + "non_pf"
                        + "."
                        + OUTPUT_FORMAT;

                if (count == 0) {
                    barcodeFilterBamOutputName = OUTPUT_DIR
                        + File.separator
                        + namedBarcode.project
                        + File.separator
                        + runFolder
                        + File.separator
                        + "Undetermined_"
                        + fcid
                        + "_"
                        + lane
                        + "_non_pf"
                        + "."
                        + OUTPUT_FORMAT;                    
                }
                
                
                final SAMFileWriter outPerFilterBarcode = new SAMFileWriterFactory().makeSAMOrBAMWriter(outputHeader, true, new File(barcodeFilterBamOutputName));
                outputFilterList.put(barcode, outPerFilterBarcode);

            }
            barcodeNameList.put(barcode, barcodeName);
        }

        //The bam files for control reads should be named: Controls_FCID_Lane_pf.bam and Controls_FCID_Lane_non_pf.bam
        String barcodeControlBamOutputName;
        
        if (OUTPUT_DIR != null) {
            barcodeControlBamOutputName = OUTPUT_DIR
                    + File.separator
                    + "Undetermined"
                    + File.separator
                    + runFolder
                    + File.separator
                    + "Controls_"
                    + fcid
                    + "_"
                    + lane
                    + "."
                    + OUTPUT_FORMAT;
        } else {
            barcodeControlBamOutputName = OUTPUT.getAbsolutePath().replace("\\.sam", "_Controls_" + fcid + "_" + lane + ".sam").replace("\\.bam", "_Controls_" + fcid + "_" + lane + ".bam");
        }

        final SAMFileHeader outputControlsHeader = header.clone();
        outputControlsHeader.setReadGroups(fullReadGroupList);
        this.addProgramRecordToHead(outputControlsHeader, this.getThisProgramRecord(programName, programDS));
        final SAMFileHeader outputHeader = header.clone();
        this.controlsOut = new SAMFileWriterFactory().makeSAMOrBAMWriter(outputHeader, true, new File(barcodeControlBamOutputName));

        if (OUTPUT != null) {
            log.info("Open output file with header: " + OUTPUT.getName());
            final SAMFileHeader singleOutputHeader = header.clone();
            singleOutputHeader.setReadGroups(fullReadGroupList);
            this.addProgramRecordToHead(singleOutputHeader, this.getThisProgramRecord(programName, programDS));
            this.out = new SAMFileWriterFactory().makeSAMOrBAMWriter(singleOutputHeader, true, OUTPUT);

            String filteredFileName = OUTPUT.getAbsolutePath();
            filteredFileName = filteredFileName.replaceAll("\\.sam", "_" + fcid + "_" + lane + "_non_pf.sam").replaceAll("\\.bam", "_" + fcid + "_" + lane + "_non_pf.bam");
            log.info("Open filtered output file with header: " + filteredFileName);
            final SAMFileHeader filteredOutputHeader = header.clone();
            filteredOutputHeader.setReadGroups(fullReadGroupList);
            this.addProgramRecordToHead(filteredOutputHeader, this.getThisProgramRecord(programName, programDS));
            this.filterOut = new SAMFileWriterFactory().makeSAMOrBAMWriter(filteredOutputHeader, true, new File(filteredFileName));

        }

    }
    
    public void closeOutputList(){
        if( this.outputList != null ){
            for(SAMFileWriter writer: this.outputList.values()){
                writer.close();
            }
        }
        if( this.outputFilterList != null ){
            for(SAMFileWriter writer: this.outputFilterList.values()){
                writer.close();
            }
        }
        if( this.controlsOut != null ){
            this.controlsOut.close();
        }

        if( this.filterOut != null ){
            this.filterOut.close();
        }
        if(this.out != null){
           this.out.close();
        }
        
    }

    /**
     *
     * @return null if command line is valid.  If command line is invalid, returns an array of error message
     *         to be written to the appropriate place.
     */
    @Override
    protected String[] customCommandLineValidation() {
        
        final ArrayList<String> messages = new ArrayList<String>();

        if (BARCODE_FILE != null) {
            this.indexDecoder = new IndexDecoder(BARCODE_FILE);
        } else {
            this.indexDecoder = new IndexDecoder(BARCODE);
        }
        
        indexDecoder.setMaxMismatches(this.MAX_MISMATCHES);
        indexDecoder.setMaxNoCalls(MAX_NO_CALLS);
        indexDecoder.setMinMismatchDelta(this.MIN_MISMATCH_DELTA);
        
        indexDecoder.prepareDecode(messages);
        this.barcodeLength = indexDecoder.getBarcodeLength();

        if (messages.isEmpty()) {
            return null;
        }
        return messages.toArray(new String[messages.size()]);
    }

    public static void main(final String[] argv) {
        System.exit(new BamIndexDecoder().instanceMain(argv));
    }

}
