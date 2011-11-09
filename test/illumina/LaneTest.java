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
 * This is the test class for Lane
 *
 */

package illumina;

import java.util.ArrayList;
import java.util.TimeZone;
import java.util.Date;
import net.sf.samtools.SAMReadGroupRecord;
import java.io.FileReader;
import java.io.BufferedReader;
import net.sf.samtools.SAMFileWriter;
import net.sf.samtools.SAMFileHeader;
import java.io.IOException;
import net.sf.samtools.SAMFileWriterFactory;
import java.io.File;
import java.util.Arrays;
import net.sf.samtools.SAMProgramRecord;
import java.util.HashMap;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Guoying Qi
 */
public class LaneTest {

    private static String intensityDir = "testdata/110323_HS13_06000_B_B039WABXX/Data/Intensities";
    private static String baseCallDir = "testdata/110323_HS13_06000_B_B039WABXX/Data/Intensities/BaseCalls";
    private static int laneNumber = 1;
    private static boolean includeSecondCall = true;
    private static boolean pfFilter = true;
    private static String barcodeSeqTagName = "RT";
    private static String barcodeQualTagName = "QT";

    private static File output = new File("testdata/6000_1.bam");

    private static Lane lane;

    public LaneTest() {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
    }

    @BeforeClass
    public static void setUpClass() throws Exception {        
        lane = new Lane(intensityDir, baseCallDir, laneNumber, includeSecondCall, pfFilter, output, barcodeSeqTagName, barcodeQualTagName);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        lane = null;
    }

    @Test
    public void readBaseCallProgramOK(){
        SAMProgramRecord baseCallProgram = lane.readBaseCallProgramRecord();
        assertEquals( baseCallProgram.getId(), "basecalling");
        assertEquals( baseCallProgram.getProgramName(), "RTA");
        assertEquals( baseCallProgram.getProgramVersion(), "1.10.36.0");
        assertEquals( baseCallProgram.getAttribute("DS"), "Basecalling Package");
    }

    @Test
    public void readTileListOK(){
        int [] expectedTileList = {
            1101,1102,1103,1104,1105,1106,1107,1108,
            1201,1202,1203,1204,1205,1206,1207,1208,
            2101,2102,2103,2104,2105,2106,2107,2108,
            2201,2202,2203,2204,2205,2206,2207,2208
        };
        assertArrayEquals(expectedTileList, lane.readTileList());
    }

    @Test
    public void reduceTileListOK(){

        int [] givenTileList = {
            1101,1102,1103,1104,1105,1106,1107,1108,
            1201,1202,1203,1204,1205,1206,1207,1208,
            2101,2102,2103,2104,2105,2106,2107,2108,
            2201,2202,2203,2204,2205,2206,2207,2208
        };
        lane.setTileList(givenTileList);

        lane.reduceTileList(1102, 2);
        
        int [] expectedTileList = {1102,1103};
        assertArrayEquals(lane.getTileList(), expectedTileList);
    }

    @Test (expected = RuntimeException.class)
    public void reduceTileListFirstTileException(){

        int [] givenTileList = {
            1101,1102,1103,1104,1105,1106,1107,1108,
            1201,1202,1203,1204,1205,1206,1207,1208,
            2101,2102,2103,2104,2105,2106,2107,2108,
            2201,2202,2203,2204,2205,2206,2207,2208
        };
        lane.setTileList(givenTileList);

        lane.reduceTileList(3308, 2);
    }

    @Test (expected = RuntimeException.class)
    public void reduceTileListTileLimitException(){

        int [] givenTileList = {
            1101,1102,1103,1104,1105,1106,1107,1108,
            1201,1202,1203,1204,1205,1206,1207,1208,
            2101,2102,2103,2104,2105,2106,2107,2108,
            2201,2202,2203,2204,2205,2206,2207,2208
        };
        lane.setTileList(givenTileList);

        lane.reduceTileList(1103, 33);
    }

    @Test
    public void readRunfolderOK(){
        assertEquals(lane.readRunfoder(), "110323_HS13_06000_B_B039WABXX");
    }
 
    @Test
    public void readRunDateOK(){
        Date runDate = lane.readRunDate();
        long expected = 1300838400000L;
        assertEquals(runDate.getTime(), expected);
    }
    
    @Test
    public void readInstrumentAndRunIDOK(){
        assertEquals(lane.readInstrumentAndRunID(), "HS13_6000");
    }

    @Test
    public void reaCycleRangeByReadOK(){
        int[][] expected = {
            {1,2},
            {51,52}
        };
        assertArrayEquals(lane.readCycleRangeByRead(), expected);
    }

    @Test
    public void reaBarCodeIndexCyclesNotOK(){
        assertNull(lane.readBarCodeIndexCycles());
    }

    @Test
    public void checkCycleRangeByReadOK() throws Exception{
        HashMap<String, int[]> cycleRangeByRead = lane.checkCycleRangeByRead();
        int [] read1CycleRange = {1, 2};
        assertArrayEquals(cycleRangeByRead.get("read1"), read1CycleRange);
        int [] read2CycleRange = {51, 52};
        assertArrayEquals(cycleRangeByRead.get("read2"), read2CycleRange);
    }
    
    @Test
    public void checkReadCycleRangeOK() throws Exception {
        String intensityDir2 = "testdata/110405_HS17_06067_A_B035CABXX/Data/Intensities";
        String baseCallDir2 = "testdata/110405_HS17_06067_A_B035CABXX/Data/Intensities/BaseCalls";
        int laneNumber2 = 1;
        boolean includeSecondCall2 = true;
        boolean pfFilter2 = true;

        Lane lane2 = new Lane(intensityDir2, baseCallDir2, laneNumber2, includeSecondCall2, pfFilter2, output, barcodeSeqTagName, barcodeQualTagName);

        ArrayList<ArrayList<Integer>> barCodeCycleLists = lane2.readBarCodeIndexCycles();
        ArrayList<ArrayList<Integer>> expected = new ArrayList<ArrayList<Integer>>();
        ArrayList<Integer> barCodeCycle = new ArrayList<Integer>(Arrays.asList(76,77,78,79,80,81,82,83));
        expected.add(barCodeCycle);
        assertEquals(expected, barCodeCycleLists);

        HashMap<String, int[]> cycleRangeByRead = lane2.checkCycleRangeByRead();
        int [] read1CycleRange = {1, 75};
        assertArrayEquals(cycleRangeByRead.get("read1"), read1CycleRange);
        int [] read2CycleRange = {84, 158};
        assertArrayEquals(cycleRangeByRead.get("read2"), read2CycleRange);
        int [] readIndexCycleRange = {76, 83};
        assertArrayEquals(cycleRangeByRead.get("readIndex1"), readIndexCycleRange);

    }

    @Test
    public void readInstrumentProgramOK(){
        SAMProgramRecord instrumentProgram = lane.readInstrumentProgramRecord();
        assertEquals( instrumentProgram.getId(), "SCS");
        assertEquals( instrumentProgram.getProgramName(), "RTA");
        assertEquals( instrumentProgram.getProgramVersion(), "1.10.36.0");
        assertEquals( instrumentProgram.getAttribute("DS"), "Controlling software on instrument");
    }

    @Test
    public void readConfigsOK() throws Exception{
        assertTrue(lane.readConfigs());
    }

    @Test
    public void generateHeaderOK() throws Exception{

        SAMReadGroupRecord readGroup = new SAMReadGroupRecord("1");
        lane.setReadGroup(readGroup);

        SAMFileHeader header = lane.generateHeader();
        File tempBamFile = File.createTempFile("test", ".bam", new File("testdata/"));
        tempBamFile.deleteOnExit();

        SAMFileWriterFactory factory = new SAMFileWriterFactory();
        factory.setCreateMd5File(true);
        SAMFileWriter outputSam = factory.makeSAMOrBAMWriter(header, true, tempBamFile);

        outputSam.close();

        File md5File = new File(tempBamFile.getAbsolutePath() + ".md5");
        md5File.deleteOnExit();
        BufferedReader md5Stream = new BufferedReader(new FileReader(md5File));
        String md5 = md5Stream.readLine();
        assertEquals(md5, "91006c6f261a94bd15896f3d0e8028bd");
    }

    @Test
    public void generateOutputSamStreamOK() throws Exception{

        SAMFileWriterFactory.setDefaultCreateMd5File(true);
        SAMFileWriter outputSam = lane.generateOutputSamStream();
        assertNotNull(outputSam);
        outputSam.close();
        output.deleteOnExit();

        File md5File = new File(output + ".md5");
        md5File.deleteOnExit();
        BufferedReader md5Stream = new BufferedReader(new FileReader(md5File));
        String md5 = md5Stream.readLine();
        assertEquals(md5, "91006c6f261a94bd15896f3d0e8028bd");
    }

    @Test
    public void processTilesOK() throws IOException, Exception{

        String id = "HS13_6000";
        int[] cycleRangeRead1 = {1, 2};
        int[] cycleRangeRead2 = {51, 52};
        int[] cycleRangeIndex = {50, 50};
        int[] tileList = {1101};

        HashMap<String, int[]> cycleRangeByRead = new HashMap<String, int[]>(3);
        cycleRangeByRead.put("read1", cycleRangeRead1);
        cycleRangeByRead.put("read2", cycleRangeRead2);
        cycleRangeByRead.put("readIndex1", cycleRangeIndex);

        lane.setCycleRangeByRead(cycleRangeByRead);
        lane.setId(id);
        lane.setTileList(tileList);

        File tempBamFile = File.createTempFile("test", ".bam", new File("testdata/"));
        tempBamFile.deleteOnExit();

        SAMFileWriterFactory factory = new SAMFileWriterFactory();
        factory.setCreateMd5File(true);
        SAMFileHeader header = new SAMFileHeader();
        SAMFileWriter outputSam = factory.makeSAMOrBAMWriter(header, true, tempBamFile);

        assertTrue(lane.processTiles(outputSam));

        outputSam.close();

        File md5File = new File(tempBamFile.getAbsolutePath() + ".md5");
        md5File.deleteOnExit();
                
        BufferedReader md5Stream = new BufferedReader(new FileReader(md5File));
        String md5 = md5Stream.readLine();
        assertEquals(md5, "3e256b176c26283991ce0704457f0d3d");
    }
    
    
    public void checkGARunOK() throws Exception {
        String intensityDir3 = "testdata/110519_IL33_06284/Data/Intensities/";
        String baseCallDir3 = "testdata/110519_IL33_06284/Data/Intensities/BaseCalls";
        int laneNumber3 = 8;
        boolean includeSecondCall2 = false;
        boolean pfFilter2 = true;
        File output3 = new File("testdata/6284_8.bam");

        Lane lane3 = new Lane(intensityDir3, baseCallDir3, laneNumber3, includeSecondCall2, pfFilter2, output3, barcodeSeqTagName, barcodeQualTagName);

        ArrayList<ArrayList<Integer>> barCodeCycleLists = lane3.readBarCodeIndexCycles();
        ArrayList<ArrayList<Integer>> expected = new ArrayList<ArrayList<Integer>>();
        ArrayList<Integer> barCodeCycle = new ArrayList<Integer>(Arrays.asList(77,77));
        expected.add(barCodeCycle);

        assertEquals(barCodeCycleLists, expected);

        HashMap<String, int[]> cycleRangeByRead = lane3.checkCycleRangeByRead();
        int [] read1CycleRange = {10, 11};
        assertArrayEquals(cycleRangeByRead.get("read1"), read1CycleRange);
        int [] read2CycleRange = {94, 95};
        assertArrayEquals(cycleRangeByRead.get("read2"), read2CycleRange);
        int [] readIndexCycleRange = {77, 77};
        assertArrayEquals(cycleRangeByRead.get("readIndex1"), readIndexCycleRange);
        
        int [] tileList = lane3.getTileList();
        assertEquals(tileList.length, 120);
        assertEquals(tileList[0], 1);
        
        assertEquals(lane3.getBaseCallProgram().getProgramName(), "Bustard");
        assertEquals(lane3.getBaseCallProgram().getProgramVersion(), "1.8.1a2");

    }
}
