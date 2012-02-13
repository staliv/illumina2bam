/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package illumina;

import java.io.IOException;
import java.util.List;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author staffan
 */
public class ModifyIlluminaConfigTest {
    
    ModifyIlluminaConfig modifier = new ModifyIlluminaConfig();
    
    public ModifyIlluminaConfigTest() {
        
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    @Test
    public void testMain() throws IOException {

        System.out.println("testMain");
        
        String[] args = {
            "I=testdata/modify/Intensities",
            "KEEP=false",
            "RI=I4Y73N5I4Y73N5",
            "TEMP_DIR=testdata/modify"
        };

        //Create config files from _config.xml
        File intensityConfigSource = new File("testdata/modify/Intensities/_config.xml");
        File intensityConfig = new File("testdata/modify/Intensities/config.xml");
        intensityConfig.deleteOnExit();
        
        ModifyIlluminaConfig.copyFile(intensityConfigSource, intensityConfig);
        
        File baseCallsConfigSource = new File("testdata/modify/Intensities/BaseCalls/_config.xml");
        File baseCallsConfig = new File("testdata/modify/Intensities/BaseCalls/config.xml");
        baseCallsConfig.deleteOnExit();
        
        ModifyIlluminaConfig.copyFile(baseCallsConfigSource, baseCallsConfig);
        
        modifier.instanceMain(args);
        
        assertEquals(modifier.getCommandLine(), "illumina.ModifyIlluminaConfig INTENSITY_DIR=testdata/modify/Intensities TEMP_DIR=testdata/modify READ_IDENTIFIER=I4Y73N5I4Y73N5 KEEP_OLD_CONFIG=false    VERBOSITY=INFO QUIET=false VALIDATION_STRINGENCY=STRICT COMPRESSION_LEVEL=5 MAX_RECORDS_IN_RAM=500000 CREATE_INDEX=false CREATE_MD5_FILE=false"); 

        //TODO: Compare new config files to correct templates

    }

    /**
     * Test of getReadsAndBarcodes method, of class ModifyIlluminaConfig.
     */
    @Test
    public void testGetReadsAndBarcodes() {
        System.out.println("getReadsAndBarcodes");
        ModifyIlluminaConfig instance = new ModifyIlluminaConfig();
        HashMap expResult = new HashMap<String, List<ArrayList<Integer>>>();

        List<ArrayList<Integer>> barcodes = new ArrayList<ArrayList<Integer>>();
        ArrayList<Integer> barcode1 = new ArrayList<Integer>(Arrays.asList(1,4));
        ArrayList<Integer> barcode2 = new ArrayList<Integer>(Arrays.asList(82,85));

        barcodes.add(barcode1);
        barcodes.add(barcode2);

        List<ArrayList<Integer>> reads = new ArrayList<ArrayList<Integer>>();
        ArrayList<Integer> read1 = new ArrayList<Integer>(Arrays.asList(5,77));
        ArrayList<Integer> read2 = new ArrayList<Integer>(Arrays.asList(86,158));

        reads.add(barcode1);
        reads.add(read1);
        reads.add(barcode2);
        reads.add(read2);
        
        expResult.put("reads", reads);
        expResult.put("barcodes", barcodes);
        
        HashMap result = instance.getReadsAndBarcodes("I4Y73N4I4Y73N4");
        assertEquals(expResult, result);

    }

    /**
     * Test of getNextNumber method, of class ModifyIlluminaConfig.
     */
    @Test
    public void testGetNextNumber() {
        System.out.println("getNextNumber");
        String readIdentifier = "I75Y30";
        int startPosition = 1;
        ModifyIlluminaConfig instance = new ModifyIlluminaConfig();
        int expResult = 75;
        int result = instance.getNextNumber(readIdentifier, startPosition);
        assertEquals(expResult, result);
    }

    /**
     * Test of copyFile method, of class ModifyIlluminaConfig.
     */
    @Test
    public void testCopyFile() throws Exception {
/*        System.out.println("copyFile");
        File sourceFile = null;
        File destFile = null;
        ModifyIlluminaConfig.copyFile(sourceFile, destFile);
        // TODO review the generated test code and remove the default call to fail.
*/
//        fail("The test case is a prototype.");
    }

}
