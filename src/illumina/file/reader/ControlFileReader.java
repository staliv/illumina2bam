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
 * 
 */
package illumina.file.reader;

import java.io.EOFException;
import java.io.IOException;
import net.sf.picard.util.Log;
import sun.tools.tree.UnsignedShiftRightExpression;

/**
 *
 * This class is a reader of a control file
 * 
 * @author Guoying Qi
 * @author Staffan Living
 */
public class ControlFileReader extends IlluminaFileReader {
    
    private final Log log = Log.getInstance(ControlFileReader.class);
    
    private final int EXPECTED_CONTROL_VERSION = 2;
    private int currentCluster = 0;
    private int totalClusters = 0;
    private int currentControlClusters = 0;

    /**
     *
     * @param controlFileName control file name
     * @throws Exception
     */
    public ControlFileReader(String controlFileName) throws Exception {

        super(controlFileName);
        this.readFileHeader();
    }

    /**
     *
     * @throws IOException
     */
    private void readFileHeader() throws Exception {

        //fisrt four bytes are empty
        //it should be zero for new version of control file, backward compatibility
        int emptyBytes = this.readFourBytes(inputStream);
        if (emptyBytes != 0) {
            
            log.warn("The first four bytes are not zero: " + emptyBytes + ". This is an old format control file.");
            this.totalClusters = emptyBytes;
            return;
        }

        //next four bytes should be version and greater or equal to the expected
        int version = this.readFourBytes(inputStream);
        if (version != this.EXPECTED_CONTROL_VERSION) {
            log.error("Unexpected version byte: " + version);
            throw new Exception("Unexpected version number in control file");
        }

        //next four bytes should be the total number of clusters
        this.totalClusters = this.readFourBytes(inputStream);
        log.info("The total number of clusters: " + this.getTotalClusters());
    }

    @Override
    public boolean hasNext() {

        return (this.getCurrentCluster() < this.getTotalClusters()) ? true : false;
    }

    @Override
    public Object next() {

        try {
            int nextByte = this.inputStream.readUnsignedShort();

            if (nextByte == -1) {
                log.warn("There is no more cluster in Control file after cluster " + this.getCurrentCluster() + " in file " + this.getFileName());
                return null;
            }

            this.currentCluster++;
            /*
            Bit0: always empty (0)
            Bit1: was the read identified as a control?
            Bit2: was the match ambiguous?
            Bit3: did the read match the phiX tag?
            Bit4: did the read align to match the phiX tag?
            Bit5: did the read match the control index sequence? (specified in controls.fata, TGTCACA)
            Bits6,7: reserved for future use
            Bits8..15: the report key for the matched record in the controls.fasta file (specified by the REPOControl FilesRT_ KEY metadata)
            */
            nextByte = nextByte & 0x2;
            if (nextByte != 0) {
                this.currentControlClusters++;
            }

            return new Integer(nextByte);

        } catch (IOException ex) {
            log.error(ex, "Problem to read control file");
        }

        return null;
    }

    /**
     * @return the currentCluster
     */
    public int getCurrentCluster() {
        return currentCluster;
    }

    /**
     * @return the totalClusters
     */
    public int getTotalClusters() {
        return totalClusters;
    }

    /**
     * @return the currentClusters
     */
    public int getCurrentControlClusters() {
        return currentControlClusters;
    }

    public static void main(String args[]) throws Exception {

        String controlFileName = "testdata/110323_HS13_06000_B_B039WABXX/Data/Intensities/BaseCalls/L001/s_1_1101.control";
        if(args.length > 0  && args[0] != null){
            controlFileName = args[0];
        }

        ControlFileReader control = new ControlFileReader(controlFileName);

        int numberControlCluster = 0;
        while (control.hasNext()) {
            int nextCluster = (Integer) control.next();

            if (nextCluster != 0) {
                numberControlCluster++;
            }
        }
        System.out.println(numberControlCluster);
        System.out.println(control.getCurrentCluster());
        System.out.println(control.getCurrentControlClusters());

        //control.next();
    }
}
