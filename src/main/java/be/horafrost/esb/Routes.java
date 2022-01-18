package be.horafrost.esb;

import org.apache.camel.Body;
import org.apache.camel.Exchange;
import org.apache.camel.Header;
import org.apache.camel.LoggingLevel;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jackson.JacksonDataFormat;
import org.apache.kafka.connect.data.Struct;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class Routes extends RouteBuilder {
	
	String articleQuery = "SELECT"
			+ "  nfoc.field_old_code_value as code,"
			+ "  nfp.field_palletisation_value as pcbstd,"
			+ "  nfsl.field_shelf_life_value as hdbmaand,"
			+ "  nfuc.field_unit_count_value as qty,"
			+ "  nfw.field_weight_value / nfuc.field_unit_count_value as zakgewicht,"
			+ "  LPAD(COALESCE(nfb.field_barcode_value, \"\"), 14, \"0\") as gtin,"
			+ "  ttfd.name as pallayout,"
			+ "  nfd.changed as timestamp"
			+ " FROM node_field_data nfd"
			+ "  left join node__field_old_code nfoc on nfd.nid = nfoc.entity_id and nfd.langcode = nfoc.langcode"
			+ "  left join node__field_shelf_life nfsl on nfd.nid = nfsl.entity_id and nfd.langcode = nfsl.langcode"
			+ "  left join node__field_weight nfw on nfd.nid = nfw.entity_id and nfd.langcode = nfw.langcode"
			+ "  left join node__field_barcode nfb on nfd.nid = nfb.entity_id and nfd.langcode = nfb.langcode"
			+ "  left join node__field_unit_count nfuc on nfd.nid = nfuc.entity_id and nfd.langcode = nfuc.langcode"
			+ "  left join node__field_palletisation nfp on nfd.nid = nfp.entity_id and nfd.langcode = nfp.langcode"
			+ "  left join node__field_pallet_label_type nfplt on nfd.nid = nfplt.entity_id and nfd.langcode = nfplt.langcode"
			+ "  left join taxonomy_term_field_data ttfd on nfplt.field_pallet_label_type_target_id = ttfd.tid and ttfd.langcode = nfplt.langcode"
			+ " WHERE nfd.nid = :#${body.id} and nfd.default_langcode = 1;";

	@Override
	public void configure() throws Exception {
		from("debezium-mysql:{{debezium.mysql.name}}?"
				+ "databaseServerId={{debezium.mysql.databaseServerId}}"
                + "&databaseHostname={{debezium.mysql.databaseHostName}}"
                + "&databaseUser={{debezium.mysql.databaseUser}}"
                + "&databasePassword={{debezium.mysql.databasePassword}}"
                + "&databaseServerName={{debezium.mysql.databaseServerName}}"
                + "&databaseHistoryFileFilename={{debezium.mysql.databaseHistoryFileName}}"
                + "&databaseIncludeList={{debezium.mysql.databaseIncludeList}}"
                + "&tableIncludeList={{debezium.mysql.tableIncludeList}}"
                + "&offsetStorageFileName={{debezium.mysql.offsetStorageFileName}}")
                .routeId("FromDebeziumMySql")
        .log("Event received from Debezium : ${body}")
	    .log("    with this identifier ${headers.CamelDebeziumIdentifier}")
	    .log("    with these source metadata ${headers.CamelDebeziumSourceMetadata}")
	    .log("    the event occured upon this operation '${headers.CamelDebeziumSourceOperation}'")
	    .log("    on this database '${headers.CamelDebeziumSourceMetadata[db]}' and this table '${headers.CamelDebeziumSourceMetadata[table]}'")
	    .log("    with the key ${headers.CamelDebeziumKey}")
	    .log("    the previous value is ${headers.CamelDebeziumBefore}")
	    .log("    the ddl sql text is ${headers.CamelDebeziumDdlSQL}")
        .choice()
        	.when(simple("${headers.CamelDebeziumSourceMetadata[db]} == 'drupal' && ${headers.CamelDebeziumSourceMetadata[table]} == 'node_field_data'"))
            	.to("direct:article").endChoice();
		
		JacksonDataFormat myFormat = new JacksonDataFormat();
		ObjectMapper myJsonMapper = new ObjectMapper(); //.registerModule(new JavaTimeModule());
		//myJsonMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
		myJsonMapper.enable(SerializationFeature.WRAP_ROOT_VALUE);
		myJsonMapper.enable(SerializationFeature.INDENT_OUTPUT);
		myFormat.setObjectMapper(myJsonMapper);
		
		from("direct:article")
		.filter().simple("${body} != null")
		.process(new Processor() {
			@Override
			public void process(Exchange exchange) throws Exception {
				exchange.getIn().setBody(new Article(exchange.getIn().getBody(Struct.class)));
			}
		})
		.enrich("sql:"+articleQuery, new ArticleEnricher())
		.setHeader(Exchange.HTTP_PATH, simple("rest/article"))
		.marshal(myFormat)
		.setHeader(Exchange.HTTP_METHOD, constant("POST"))
		.setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
		//.setHeader(...)
		.log(LoggingLevel.INFO, "Outgoing message ${body} with headers ${headers}")
		.to("http:{{node.server}}:1880");
		
	}

}
