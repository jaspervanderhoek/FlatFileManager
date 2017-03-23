package replication.helpers;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import com.mendix.core.Core;
import com.mendix.logging.ILogNode;

public class TimeMeasurement {

	private Map<String, Measurement> performanceMap;
	private String replicationName;
	private Integer orderNr = 0;
	private Measurement currentMeasurement;
	private final static ILogNode _LogNode = Core.getLogger("PERFORMANCE_TEST");
	
	public TimeMeasurement( String replicationName ) {
		this.replicationName = replicationName;
		this.performanceMap = new HashMap<String, Measurement>();
	}

	private class Measurement {
		protected Integer currentOrderNr;
		protected Long time;
		protected String level;
		private Measurement parent;

		protected Measurement( Measurement parent, Long time, Integer level ) {
			this.time = time;
			this.currentOrderNr = level;
			this.parent = parent;
			
			if( parent != null && parent.level != null )
				this.level = parent.level + "." + String.valueOf(this.currentOrderNr);
			else  
				this.level = String.valueOf(this.currentOrderNr);
		}
	}
	public synchronized void startPerformanceTest( String token ) {
		if( _LogNode.isDebugEnabled() ) {
			this.orderNr++;
			this.currentMeasurement = new Measurement( this.currentMeasurement, Calendar.getInstance().getTimeInMillis(), this.orderNr);
			this.orderNr=0;
			this.performanceMap.put(token, this.currentMeasurement );
		}
	}

	public void endPerformanceTest( String token ) {
		this.endPerformanceTest(false, token);
	}

	public synchronized void endPerformanceTest( boolean hideZero, String token ) {
		if( _LogNode.isDebugEnabled() ) {
			if( this.performanceMap.containsKey(token) ) {
				Measurement measurement = this.performanceMap.remove(token);
				long time = Calendar.getInstance().getTimeInMillis() - measurement.time;
				if( time > 0 || (time == 0 && !hideZero) )
					_LogNode.debug( this.replicationName + "  -  PTEST " + measurement.level + " :  " + token + ", milliseconds: " + time );
				
				//Set defaults back to the parent
				this.currentMeasurement = measurement.parent;
				this.orderNr = measurement.currentOrderNr;
			}
			else if( !hideZero ) {
				_LogNode.debug( this.replicationName + "  -  PTEST " + this.orderNr + " :  " + token + ", milliseconds: (unknown)" );
				this.orderNr--;
			}
			
		}
	}

}
