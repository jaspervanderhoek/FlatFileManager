package flatfilemanager.implementation;

public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String uselessvalue = "";
		for( int j = 0; j < 100; j++ ) {
			long startTime = System.currentTimeMillis();
			for( int i = 0; i < 1000; i++ ) {
				boolean splitArray = false;
				StringBuilder b = new StringBuilder();
				b.append("What's the value of levelCount. It must be >= 0 and <= the current size of the list. The fact that you are de");
				String str = "What's the value of levelCount. It must be >= 0 and <= the current size of the list. The fact that you are de";
				int[] positions = {84,79,71,70,69,61,53,23,20,9,2,1};
				String[] values1, values;
				
				if( !splitArray ){
					values1 = new String[positions.length+1];
					int prevPos = 0;
					for(int counter = positions.length-1; counter >= -1 ; counter-- ) {
						int pos = ( counter == -1 ? str.length() : positions[counter]); 
						values1[(positions.length-1-counter)] = str.substring(prevPos, pos);
						prevPos = pos;
					}

//					System.out.print( " \r\nvalues: " + values1.length );
					for( String value : values1 ) {
//						System.out.print( " | " + value );
						uselessvalue = value;
					}
				}
//				splitArray = true;
				
				if( splitArray ) {
					for(int pos : positions )
						b.insert(pos, "]|[");
					
					values = b.toString().split("\\]\\|\\[");
//					System.out.print( " \r\nvalues: " + values.length );
					for( String value : values ) { 
//						System.out.print( " | " + value );
						uselessvalue = value;
					}
				}

			}
	
			long endTime = System.currentTimeMillis();
			
			System.out.println("Duration: " + (endTime-startTime));
			System.out.println(uselessvalue);
		}
	}

}
