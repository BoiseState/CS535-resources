import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.io.Writable;

public class TrackStatistics implements Writable
{
    public static final int COL_USERID = 0;
    public static final int COL_TRACKID = 1;
    public static final int COL_SCROBBLE = 2;
    public static final int COL_RADIO = 3;
    public static final int COL_SKIP = 4;

    private int userId;
    private int trackId;
    private int scrobble;
    private int radioPlay;
    private int skip;


    public TrackStatistics(Random random) {
	this.userId = random.nextInt(1000);
	this.trackId = random.nextInt(100);
	this.scrobble = Math.abs(random.nextInt() % 2);
	this.radioPlay = Math.abs(random.nextInt() % 2);
	this.skip = Math.abs(random.nextInt() % 2);
    }


    public TrackStatistics() {
    }


    public TrackStatistics(int userId, int trackId, int scrobble, int radioPlay, int skip) {
	this.userId = userId;
	this.trackId = trackId;
	this.scrobble = scrobble;
	this.radioPlay = radioPlay;
	this.skip = skip;
    }


    public void write(DataOutput out) throws IOException {
	out.writeInt(userId);
	out.writeChar(' ');
	out.writeInt(trackId);
	out.writeChar(' ');
	out.writeInt(scrobble);
	out.writeChar(' ');
	out.writeInt(radioPlay);
	out.writeChar(' ');
	out.writeInt(skip);
    }


    public void readFields(DataInput in) throws IOException {
	this.userId = in.readInt();
	this.trackId = in.readInt();
	this.scrobble = in.readInt();
	this.radioPlay = in.readInt();
	this.skip = in.readInt();
    }


    /**
     * @return the userId
     */
    public int getUserId() {
	return userId;
    }


    /**
     * @param userId
     *            the userId to set
     */
    public void setUserId(int userId) {
	this.userId = userId;
    }


    /**
     * @return the trackId
     */
    public int getTrackId() {
	return trackId;
    }


    /**
     * @param trackId
     *            the trackId to set
     */
    public void setTrackId(int trackId) {
	this.trackId = trackId;
    }


    /**
     * @return the scrobble
     */
    public int getScrobble() {
	return scrobble;
    }


    /**
     * @param scrobble
     *            the scrobble to set
     */
    public void setScrobble(int scrobble) {
	this.scrobble = scrobble;
    }


    /**
     * @return the radioPlay
     */
    public int getRadioPlay() {
	return radioPlay;
    }


    /**
     * @param radioPlay
     *            the radioPlay to set
     */
    public void setRadioPlay(int radioPlay) {
	this.radioPlay = radioPlay;
    }


    /**
     * @return the skip
     */
    public int getSkip() {
	return skip;
    }


    /**
     * @param skip
     *            the skip to set
     */
    public void setSkip(int skip) {
	this.skip = skip;
    }


    public String toString() {
	return userId + " " + trackId + " " + scrobble + " " + radioPlay + " " + skip;
    }

}
