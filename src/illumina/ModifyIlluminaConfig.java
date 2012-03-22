/*
 * Tool for modifying the illumina config files
 * 
 */
package illumina;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.sf.picard.cmdline.Option;
import net.sf.picard.cmdline.Usage;
import net.sf.picard.io.IoUtil;
import net.sf.picard.util.Log;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Text;
import org.jdom.input.SAXBuilder;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;
import org.jdom.xpath.XPath;

/**
 * Allows you to control how the reads are setup in your config.xml files.
 * READ_IDENTIFIER can include values for I, J, Y and N. Example I4J1Y73N5:
 * I4 = Index 4 bp
 * J1 = Include 1 bp in index but do not use for Index Decoding - useful when a cycle goes bad
 * Y73 = Read 73 bp
 * N5 = Skip 5 bp
 * Example for paired end reads with double index/barcode: I4Y73N5I4Y73N5
 * @author Staffan Living - staffan.living@gmail.com
 */
public class ModifyIlluminaConfig extends Illumina2bamCommandLine {

    private final Log log = Log.getInstance(ModifyIlluminaConfig.class);
    
    private final String programName = "ModifyIlluminaConfig";
    
    private final String programDS = "A command-line tool to modify the illumina config.xml files";
   
    @Usage(programVersion= version)
    public final String USAGE = this.getStandardUsagePreamble() + this.programDS + ". ";
   
    @Option(shortName="I", doc="The Intensities directory to modify config.xml files in.")
    public File INTENSITY_DIR;

    @Option(shortName="T", doc="Directory for temporary files, needs write access.")
    public File TEMP_DIR;

    @Option(shortName="B", doc="The BaseCalls directory, using BaseCalls directory under intensities if not given.", optional=true)
    public File BASECALLS_DIR;

    @Option(shortName="RI", doc="The read identifier string, example: I4J1Y75N5I5Y75N5 = Paired end with two barcodes: Index1_5bp where last base is deemed unsuitable Read1_75bp Skip_5bp Index2_5bp Read2_75bp Skip_5bp")
    public String READ_IDENTIFIER;
    
    @Option(shortName="KEEP", doc="Keep old renamed config.xml files, defaults to true", optional=true)
    public boolean KEEP_OLD_CONFIG = true;

    @Option(shortName="L", doc="Lane number, this creates lane specific config files named config_lane_[laneNumber].xml", optional=true)
    public Integer LANE;
    
    
    @Override
    protected int doWork() {

        String configBaseFileName = "config.xml";
        String newBaseCallsConfigFileName = "basecallsconfig.xml";
        String newIntensityConfigFileName = "intensitiesconfig.xml";
        
        
        if (this.LANE != null) {
            newBaseCallsConfigFileName = "basecallsconfig_lane_" + this.LANE + ".xml";
            newIntensityConfigFileName = "intensitiesconfig_lane_" + this.LANE + ".xml";
        }
        
        File intensityConfig = new File(INTENSITY_DIR.getAbsoluteFile() + File.separator + configBaseFileName);
        IoUtil.assertFileIsReadable(intensityConfig);
        
        if (this.BASECALLS_DIR == null) {
            this.BASECALLS_DIR = new File(INTENSITY_DIR.getAbsoluteFile() + File.separator + "BaseCalls");
            log.info("BaseCalls directory not given, using " + this.BASECALLS_DIR);
        }

        File baseCallsConfig = new File(BASECALLS_DIR.getAbsoluteFile() + File.separator + configBaseFileName);
        IoUtil.assertFileIsReadable(baseCallsConfig);

        //Setup new config files
        File newIntensityConfig = new File(TEMP_DIR + File.separator + newIntensityConfigFileName);
        IoUtil.assertFileIsWritable(newIntensityConfig);
        
        File newBaseCallsConfig = new File(TEMP_DIR + File.separator + newBaseCallsConfigFileName);
        IoUtil.assertFileIsWritable(newBaseCallsConfig);

        //Copy config from base config xmls if the new files do not already exist and the files are not the same
        if (!configBaseFileName.equals(newBaseCallsConfigFileName) && !newIntensityConfig.exists()) {
            try {
                copyFile(intensityConfig, newIntensityConfig);
            } catch (IOException ex) {
                Logger.getLogger(ModifyIlluminaConfig.class.getName()).log(Level.SEVERE, null, ex);
            }

            log.info("Copied config file to " + newIntensityConfig);
            
        }
        if (!configBaseFileName.equals(newBaseCallsConfigFileName) && !newBaseCallsConfig.exists()) {
            try {
                copyFile(baseCallsConfig, newBaseCallsConfig);
            } catch (IOException ex) {
                Logger.getLogger(ModifyIlluminaConfig.class.getName()).log(Level.SEVERE, null, ex);
            }

            log.info("Copied config file to " + newBaseCallsConfig);
            
        }
        
        if (this.KEEP_OLD_CONFIG) {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyMMdd-hhmmss");
            File oldBaseCallsConfig = new File(BASECALLS_DIR.getAbsoluteFile() + File.separator + newBaseCallsConfigFileName.replaceFirst(".xml", "-") + simpleDateFormat.format(new Date()) + ".xml");
            File oldIntensityConfig = new File(INTENSITY_DIR.getAbsoluteFile() + File.separator + newBaseCallsConfigFileName.replaceFirst(".xml", "-") + simpleDateFormat.format(new Date()) + ".xml");
            try {
                copyFile(baseCallsConfig, oldBaseCallsConfig);
            } catch (IOException ex) {
                Logger.getLogger(ModifyIlluminaConfig.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
                copyFile(intensityConfig, oldIntensityConfig);
            } catch (IOException ex) {
                Logger.getLogger(ModifyIlluminaConfig.class.getName()).log(Level.SEVERE, null, ex);
            }

            log.info("Created backups of config files to " + oldIntensityConfig + " and " + oldBaseCallsConfig);
        }

        HashMap<String, List<ArrayList<Integer>>> readsAndBarcodes = getReadsAndBarcodes(this.READ_IDENTIFIER);
        try {
            modifyConfigurationFiles(newIntensityConfig, newBaseCallsConfig, readsAndBarcodes);
        } catch (IOException ex) {
            Logger.getLogger(ModifyIlluminaConfig.class.getName()).log(Level.SEVERE, null, ex);
        } catch (JDOMException ex) {
            Logger.getLogger(ModifyIlluminaConfig.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return 0;
    }

    protected void modifyConfigurationFiles(File intensityConfig, File baseCallsConfig, HashMap<String, List<ArrayList<Integer>>> readsAndBarcodes) throws IOException, JDOMException {

        setReads(intensityConfig, readsAndBarcodes, "ImageAnalysis");
        setReads(baseCallsConfig, readsAndBarcodes, "BaseCallAnalysis");
  
        setBarcodes(baseCallsConfig, readsAndBarcodes.get("barcodes"));
        
        log.info("Updated config files to comply with " + this.READ_IDENTIFIER);
    }
    
    private void setReads(File config, HashMap<String, List<ArrayList<Integer>>> readsAndBarcodes, String rootElementName) throws IOException, JDOMException {

        try {

            SAXBuilder builder = new SAXBuilder();

            Document configXml = (Document) builder.build(config);

            XPath xpath = XPath.newInstance("/" + rootElementName + "/Run/RunParameters");
            Element runParameters = (Element) xpath.selectSingleNode(configXml);
            
            //Try to find a runfolder value
            xpath = XPath.newInstance("/" + rootElementName + "/Run/RunParameters/ImagingReads/RunFolder/text()");
            List<Text> runFolders = xpath.selectNodes(configXml);
            
            String runFolder = "";
            
            if (runFolders.size() > 0) {
                runFolder = runFolders.get(0).getValue();
            }

            //Remove old values
            runParameters.removeChildren("ImagingReads");
            runParameters.removeChildren("Reads");
            
            List<ArrayList<Integer>> reads = readsAndBarcodes.get("reads");
            int index = 0;
            for (ArrayList<Integer> read : reads) {

                index++;
                
                //Create new elements for ImagingReads and Reads
                Element imagingRead = new Element("ImagingReads");
                Element newRead = new Element("Reads");

                imagingRead.setAttribute("Index", Integer.toString(index));
                newRead.setAttribute("Index", Integer.toString(index));

                Element firstCycle = new Element("FirstCycle");
                firstCycle.addContent(Integer.toString(read.get(0)));

                Element lastCycle = new Element("LastCycle");
                lastCycle.addContent(Integer.toString(read.get(1)));

                imagingRead.addContent(firstCycle);
                imagingRead.addContent(lastCycle);
                newRead.addContent((Element)firstCycle.clone());
                newRead.addContent((Element)lastCycle.clone());
                
                if (!runFolder.equals("")) {
                    Element runFolderElement = new Element("RunFolder");
                    runFolderElement.addContent(runFolder);
                    imagingRead.addContent(runFolderElement);
                    newRead.addContent((Element)runFolderElement.clone());
                }

                runParameters.addContent(imagingRead);
                runParameters.addContent(newRead);
            }

            XMLOutputter xmlOutput = new XMLOutputter();

            xmlOutput.setFormat(Format.getPrettyFormat());
            xmlOutput.output(configXml, new FileWriter(config));

        } catch (IOException io) {
            throw io;
        } catch (JDOMException e) {
            throw e;
        }
        
    }
    
    private void setBarcodes(File baseCallsConfig, List<ArrayList<Integer>> barcodes) throws IOException, JDOMException {

        try {

            SAXBuilder builder = new SAXBuilder();

            Document baseCallsXml = (Document) builder.build(baseCallsConfig);

            XPath xpath = XPath.newInstance("/BaseCallAnalysis/Run/RunParameters");
            Element runParameters = (Element) xpath.selectSingleNode(baseCallsXml);
            
            //Remove old element if it exists
            runParameters.removeChildren("Barcode");
            
            Element barcodesElement = new Element("Barcode");
            
            for (ArrayList<Integer> barcodeCycle : barcodes) {


                int firstBasePair = barcodeCycle.get(0);
                int lastBasePair = barcodeCycle.get(1);
                
                for (int i = firstBasePair; i <= lastBasePair; i++) {
                    //Create new Cycle elements for Barcodes
                    Element cycle = new Element("Cycle");
                    cycle.setAttribute("Use", "true");
                    cycle.addContent(Integer.toString(i));

                    barcodesElement.addContent(cycle);
                }
                
            }
            
            runParameters.addContent(barcodesElement);

            XMLOutputter xmlOutput = new XMLOutputter();

            xmlOutput.setFormat(Format.getPrettyFormat());
            xmlOutput.output(baseCallsXml, new FileWriter(baseCallsConfig));

        } catch (IOException io) {
            throw io;
        } catch (JDOMException e) {
            throw e;
        }
        
    }
    
    protected HashMap<String, List<ArrayList<Integer>>> getReadsAndBarcodes(String readIdentifier) {

        HashMap<String, List<ArrayList<Integer>>> readsAndBarcodes = new HashMap<String, List<ArrayList<Integer>>>();

       //Parse the read identifier
        List<ArrayList<Integer>> reads = new ArrayList<ArrayList<Integer>>();
        List<ArrayList<Integer>> barcodes = new ArrayList<ArrayList<Integer>>();
        
        int currentPosition = 1;
        int basePairs = 0;
        
        readIdentifier = readIdentifier.toUpperCase();
        boolean hasCreatedNewBarcode = false;
        ArrayList<Integer> barcode;
        
        for (int i = 0; i < readIdentifier.length(); i++) {
            char character = readIdentifier.charAt(i);
            switch (character) {
                case 'J':
                    if (hasCreatedNewBarcode) {
                        //Fetch last barcode
                        barcode = barcodes.get(barcodes.size() - 1);
                        //Remove last end position
                        barcode.remove(barcode.size() - 1);
                        //Remove from container lists
                        barcodes.remove(barcodes.size() - 1);
                        reads.remove(reads.size() - 1);
                    } else {
                        //Create new barcode
                        barcode = new ArrayList<Integer>();
                        barcode.add(currentPosition);
                        hasCreatedNewBarcode = true;
                    }
                    //Determine length of index
                    basePairs = getNextNumber(readIdentifier, i + 1);
                    currentPosition = currentPosition + basePairs;
                    barcode.add(currentPosition - 1);
                    //Add barcode to reads and barcodes
                    barcodes.add(barcode);
                    reads.add(barcode);
                    break;
                case 'I':
                    if (hasCreatedNewBarcode) {
                        //Fetch last barcode
                        barcode = barcodes.get(barcodes.size() - 1);
                        //Remove last end position
                        barcode.remove(barcode.size() - 1);
                        //Remove from container lists
                        barcodes.remove(barcodes.size() - 1);
                        reads.remove(reads.size() - 1);
                    } else {
                        //Create new barcode
                        barcode = new ArrayList<Integer>();
                        barcode.add(currentPosition);
                        hasCreatedNewBarcode = true;
                    }
                    //Determine length of index
                    basePairs = getNextNumber(readIdentifier, i + 1);
                    currentPosition = currentPosition + basePairs;
                    barcode.add(currentPosition - 1);
                    //Add barcode to reads and barcodes
                    barcodes.add(barcode);
                    reads.add(barcode);
                    break;
                case 'Y':
                    hasCreatedNewBarcode = false;
                    //Create new read
                    ArrayList<Integer> read = new ArrayList<Integer>();
                    //Determine length of index
                    basePairs = getNextNumber(readIdentifier, i + 1);
                    read.add(currentPosition);
                    currentPosition = currentPosition + basePairs;
                    read.add(currentPosition - 1);
                    reads.add(read);
                    break;
                case 'N':
                    hasCreatedNewBarcode = false;
                    basePairs = getNextNumber(readIdentifier, i + 1);
                    currentPosition = currentPosition + basePairs;
                    break;
                default:
                    break;
            }
        }

        readsAndBarcodes.put("reads", reads);
        readsAndBarcodes.put("barcodes", barcodes);
        
        return readsAndBarcodes;
    }
    
    protected int getNextNumber(String readIdentifier, int startPosition) {
        String nextNumber = "";
        for (int i = startPosition; i < readIdentifier.length(); i++) {
            if (readIdentifier.substring(i, i + 1).matches("[0-9]")) {
                nextNumber += readIdentifier.substring(i, i + 1);
            } else {
                break;
            }
        }
        return Integer.parseInt(nextNumber);
    }

    public static void copyFile(File sourceFile, File destFile) throws IOException {
        if (!destFile.exists()) {
            destFile.createNewFile();
        }
        FileInputStream fIn = null;
        FileOutputStream fOut = null;
        FileChannel source = null;
        FileChannel destination = null;
        try {
            fIn = new FileInputStream(sourceFile);
            source = fIn.getChannel();
            fOut = new FileOutputStream(destFile);
            destination = fOut.getChannel();
            long transfered = 0;
            long bytes = source.size();
            while (transfered < bytes) {
                transfered += destination.transferFrom(source, 0, source.size());
                destination.position(transfered);
            }
        } finally {
            if (source != null) {
                source.close();
            } else if (fIn != null) {
                fIn.close();
            }
            if (destination != null) {
                destination.close();
            } else if (fOut != null) {
                fOut.close();
            }
        }
    }

    public static void main(final String[] argv) {
        System.exit(new ModifyIlluminaConfig().instanceMain(argv));
    }

}
