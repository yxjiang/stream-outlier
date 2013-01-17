package edu.fiu.yxjiang.message;

public class MessageHandlerFactory {

	public static MessageHandler getMessageHandler(String dataType) {
		if(dataType.equalsIgnoreCase("computerMetaData")) {
			return new ComputerMetadataHandler();
		}
		else {
			return null;
		} 
	}
	
}
