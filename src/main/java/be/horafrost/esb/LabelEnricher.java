package be.horafrost.esb;

import java.util.List;
import java.util.Map;

import org.apache.camel.AggregationStrategy;
import org.apache.camel.Exchange;

public class LabelEnricher implements AggregationStrategy {

	@Override
	public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
		Label originalBody = oldExchange.getIn().getBody(Label.class);
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> response =  newExchange.getIn().getBody(List.class); //resultset = list of maps
		
		if (response == null || response.size() != 1)
			return oldExchange;
		
		Map<String, Object> responseMap = response.get(0);
		
		originalBody.filename = ((String)responseMap.get("filename"));
		
		/* #	
		 * filename		STELOI.ciff */
		
		oldExchange.getIn().setBody(originalBody);
		return oldExchange;
	}
	
	
	
	
}
