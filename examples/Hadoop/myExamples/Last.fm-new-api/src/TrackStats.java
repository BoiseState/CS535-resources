import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class TrackStats implements WritableComparable<TrackStats>
{

    private IntWritable listeners;
    private IntWritable plays;
    private IntWritable scrobbles;
    private IntWritable radioPlays;
    private IntWritable skips;


    public TrackStats() {
	this.listeners = new IntWritable(0);
	this.plays = new IntWritable(0);
	this.scrobbles = new IntWritable(0);
	this.radioPlays = new IntWritable(0);
	this.skips = new IntWritable(0);
    }


    public TrackStats(int numListeners, int numPlays, int numScrobbles, int numRadio, int numSkips) {
	this.listeners = new IntWritable(numListeners);
	this.plays = new IntWritable(numPlays);
	this.scrobbles = new IntWritable(numScrobbles);
	this.radioPlays = new IntWritable(numRadio);
	this.skips = new IntWritable(numSkips);
    }


    public TrackStats(IntWritable numListeners, IntWritable numPlays, IntWritable numScrobbles, IntWritable numRadio,
            IntWritable numSkips) {
	this.listeners = numListeners;
	this.plays = numPlays;
	this.scrobbles = numScrobbles;
	this.radioPlays = numRadio;
	this.skips = numSkips;
    }


    @Override
    public void write(DataOutput out) throws IOException {
	listeners.write(out);
	plays.write(out);
	scrobbles.write(out);
	radioPlays.write(out);
	skips.write(out);
    }


    @Override
    public void readFields(DataInput in) throws IOException {
	listeners.readFields(in);
	plays.readFields(in);
	scrobbles.readFields(in);
	radioPlays.readFields(in);
	skips.readFields(in);
    }


    @Override
    public int hashCode() {
	return listeners.hashCode() * 163 + plays.hashCode() + scrobbles.hashCode() + radioPlays.hashCode()
	        + skips.hashCode();
    }


    @Override
    public boolean equals(Object o) {
	if (o instanceof TrackStats) {
	    TrackStats ts = (TrackStats) o;
	    return listeners.equals(ts.listeners) && plays.equals(ts.plays) && scrobbles.equals(ts.scrobbles)
	            && radioPlays.equals(ts.radioPlays) && skips.equals(ts.skips);
	}
	return false;
    }


    @Override
    public String toString() {
	return listeners + " " + plays + " " + scrobbles + " " + radioPlays + " " + skips;
    }


    @Override
    public int compareTo(TrackStats ts) {
	int cmp = listeners.compareTo(ts.listeners);
	if (cmp != 0) return cmp;
	cmp = plays.compareTo(ts.plays);
	if (cmp != 0) return cmp;
	cmp = scrobbles.compareTo(ts.scrobbles);
	if (cmp != 0) return cmp;
	cmp = radioPlays.compareTo(ts.radioPlays);
	if (cmp != 0) return cmp;
	return skips.compareTo(ts.skips);
    }


    /**
     * @param listeners
     *            the listeners to set
     */
    public void setListeners(IntWritable numListeners) {
	this.listeners = numListeners;
    }


    /**
     * @param listeners
     *            the listeners to set
     */
    public void setListeners(int numListeners) {
	this.listeners = new IntWritable(numListeners);
    }


    /**
     * @return the listeners
     */
    public int getListeners() {
	return listeners.get();
    }


    /**
     * @param plays
     *            the plays to set
     */
    public void setPlays(IntWritable plays) {
	this.plays = plays;
    }


    /**
     * @param plays
     *            the plays to set
     */
    public void setPlays(int plays) {
	this.plays = new IntWritable(plays);
    }


    /**
     * @return the plays
     */
    public int getPlays() {
	return plays.get();
    }


    /**
     * @param scrobbles
     *            the scrobbles to set
     */
    public void setScrobbles(IntWritable scrobbles) {
	this.scrobbles = scrobbles;
    }


    /**
     * @param scrobbles
     *            the scrobbles to set
     */
    public void setScrobbles(int scrobbles) {
	this.scrobbles = new IntWritable(scrobbles);
    }


    /**
     * @return the scrobbles
     */
    public int getScrobbles() {
	return scrobbles.get();
    }


    /**
     * @param radioPlays
     *            the radioPlays to set
     */
    public void setRadioPlays(IntWritable radio) {
	this.radioPlays = radio;
    }


    /**
     * @param radioPlays
     *            the radioPlays to set
     */
    public void setRadioPlays(int radio) {
	this.radioPlays = new IntWritable(radio);
    }


    /**
     * @return the radioPlays
     */
    public int getRadioPlays() {
	return radioPlays.get();
    }


    /**
     * @param skips
     *            the skips to set
     */
    public void setSkips(IntWritable skips) {
	this.skips = skips;
    }


    /**
     * @param skips
     *            the skips to set
     */
    public void setSkips(int skips) {
	this.skips = new IntWritable(skips);
    }


    /**
     * @return the skips
     */
    public int getSkips() {
	return skips.get();
    }

}
