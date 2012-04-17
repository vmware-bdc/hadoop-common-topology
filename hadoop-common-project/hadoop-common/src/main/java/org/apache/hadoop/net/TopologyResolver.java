package org.apache.hadoop.net;


public class TopologyResolver {
  /** Path separator as a string {@value} */
  public final static String PATH_SEPARATOR_STR = "/";
  
  public static String getRack(Node host, boolean isOnVirtualization) {
	return isOnVirtualization ? getFirstHalf(host.getNetworkLocation()) 
	    : host.getNetworkLocation();
  }
  
  public static String getNodeGroup(Node host, boolean isOnVirtualization) {
	return isOnVirtualization ? getLastHalf(host.getNetworkLocation()) : null;
  }
  
  public static String getRack(String networkLocation, boolean isOnVirtualization) {
	return isOnVirtualization ? getFirstHalf(networkLocation) 
	    : networkLocation;
  }
  
  public static String getNodeGroup(String networkLocation, boolean isOnVirtualization) {
	return isOnVirtualization ? getLastHalf(networkLocation) : null;
  }
  
  private static String getFirstHalf(String wholeString) {
	  int index = wholeString.lastIndexOf(PATH_SEPARATOR_STR);
	  return wholeString.substring(0, index);
  }
  
  private static String getLastHalf(String wholeString) {
	  int index = wholeString.lastIndexOf(PATH_SEPARATOR_STR);
	  return wholeString.substring(index);
  }

}
