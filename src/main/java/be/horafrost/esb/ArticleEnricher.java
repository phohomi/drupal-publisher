package be.horafrost.esb;

import java.util.List;
import java.util.Map;

import org.apache.camel.AggregationStrategy;
import org.apache.camel.Exchange;

public class ArticleEnricher implements AggregationStrategy {

	@Override
	public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
		Article originalBody = oldExchange.getIn().getBody(Article.class);
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> response =  newExchange.getIn().getBody(List.class); //resultset = list of maps
		
		if (response == null || response.size() != 1)
			return oldExchange;
		
		Map<String, Object> responseMap = response.get(0);
		
		if((Integer)responseMap.get("category") != 29) 
			return oldExchange;
		
		originalBody.code = ((String)responseMap.get("code"));
		originalBody.pcbstd = ((Integer)responseMap.get("pcbstd"));
		originalBody.hdbmaand = ((Integer)responseMap.get("hdbmaand"));
		originalBody.qty = ((Integer)responseMap.get("qty"));
		originalBody.zakgewicht = ((Double)responseMap.get("zakgewicht"));
		originalBody.gtin = ((String)responseMap.get("gtin"));
		originalBody.palLayout = ((String)responseMap.get("pallayout"));
		originalBody.timestamp = ((Integer)responseMap.get("timestamp"));
		originalBody.lijn1 = ((String)responseMap.get("lijn1"));
		originalBody.lijn2 = ((String)responseMap.get("lijn2"));
		originalBody.lijn3 = ((String)responseMap.get("lijn3"));
		originalBody.lijn4 = ((String)responseMap.get("lijn4"));
		originalBody.lijn5 = ((String)responseMap.get("lijn5"));
		originalBody.lijn6 = ((String)responseMap.get("lijn6"));
		originalBody.lijn7 = ((String)responseMap.get("lijn7"));
		originalBody.layout = ((String)responseMap.get("layout"));
		originalBody.artikelnoklant = ((String)responseMap.get("artikelnoklant"));
		
		/* #	
		 * code		56004065
		 * pcbstd	54
		 * hdbmaand	24
		 * qty		10
		 * zakgewicht	1
		 * gtin		04316268388573
		 * pallayout		edeka */
		
		oldExchange.getIn().setBody(originalBody);
		return oldExchange;
	}

}
