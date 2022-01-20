package be.horafrost.esb;

import javax.activation.DataHandler;

import org.apache.camel.Body;
import org.apache.camel.Exchange;
import org.apache.camel.Header;
import org.apache.camel.LoggingLevel;
import org.apache.camel.Processor;
import org.apache.camel.attachment.AttachmentMessage;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jackson.JacksonDataFormat;
import org.apache.camel.http.base.HttpOperationFailedException;
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
			+ "  ROUND(nfw.field_weight_value / nfuc.field_unit_count_value, 3) as zakgewicht,"
			+ "  LPAD(COALESCE(nfb.field_barcode_value, \"\"), 14, \"0\") as gtin,"
			+ "  ttfd.name as pallayout,"
			+ "  nfd.changed as timestamp,"
			+ "  nfttl_0.field_thermal_transfer_label_value as lijn1,"
			+ "  nfttl_1.field_thermal_transfer_label_value as lijn2,"
			+ "  nfttl_2.field_thermal_transfer_label_value as lijn3,"
			+ "  nfttl_3.field_thermal_transfer_label_value as lijn4,"
			+ "  nfttl_4.field_thermal_transfer_label_value as lijn5,"
			+ "  nfttl_5.field_thermal_transfer_label_value as lijn6,"
			+ "  nfttl_6.field_thermal_transfer_label_value as lijn7,"
			+ "  ttfd2.name as layout,"
			+ "  nfcr.field_customer_reference_value as artikelnoklant"
			+ " FROM node_field_data nfd"
			+ "  left join node__field_old_code nfoc on nfd.nid = nfoc.entity_id and nfd.langcode = nfoc.langcode"
			+ "  left join node__field_shelf_life nfsl on nfd.nid = nfsl.entity_id and nfd.langcode = nfsl.langcode"
			+ "  left join node__field_weight nfw on nfd.nid = nfw.entity_id and nfd.langcode = nfw.langcode"
			+ "  left join node__field_barcode nfb on nfd.nid = nfb.entity_id and nfd.langcode = nfb.langcode"
			+ "  left join node__field_unit_count nfuc on nfd.nid = nfuc.entity_id and nfd.langcode = nfuc.langcode"
			+ "  left join node__field_palletisation nfp on nfd.nid = nfp.entity_id and nfd.langcode = nfp.langcode"
			+ "  left join node__field_pallet_label_type nfplt on nfd.nid = nfplt.entity_id and nfd.langcode = nfplt.langcode"
			+ "  left join taxonomy_term_field_data ttfd on nfplt.field_pallet_label_type_target_id = ttfd.tid and ttfd.langcode = nfplt.langcode"
			+ "  left join node__field_thermal_transfer_label nfttl_0 on nfd.nid = nfttl_0.entity_id and nfd.langcode = nfttl_0.langcode and nfttl_0.delta = 0"
			+ "  left join node__field_thermal_transfer_label nfttl_1 on nfd.nid = nfttl_1.entity_id and nfd.langcode = nfttl_1.langcode and nfttl_1.delta = 1"
			+ "  left join node__field_thermal_transfer_label nfttl_2 on nfd.nid = nfttl_2.entity_id and nfd.langcode = nfttl_2.langcode and nfttl_2.delta = 2"
			+ "  left join node__field_thermal_transfer_label nfttl_3 on nfd.nid = nfttl_3.entity_id and nfd.langcode = nfttl_3.langcode and nfttl_3.delta = 3"
			+ "  left join node__field_thermal_transfer_label nfttl_4 on nfd.nid = nfttl_4.entity_id and nfd.langcode = nfttl_4.langcode and nfttl_4.delta = 4"
			+ "  left join node__field_thermal_transfer_label nfttl_5 on nfd.nid = nfttl_5.entity_id and nfd.langcode = nfttl_5.langcode and nfttl_5.delta = 5"
			+ "  left join node__field_thermal_transfer_label nfttl_6 on nfd.nid = nfttl_6.entity_id and nfd.langcode = nfttl_6.langcode and nfttl_6.delta = 6"
			+ "  left join node__field_thermal_transfer_label_typ nfttlt on nfd.nid = nfttlt.entity_id  and nfd.langcode = nfttlt.langcode"
			+ "  left join taxonomy_term_field_data ttfd2 on nfttlt.field_thermal_transfer_label_typ_target_id = ttfd2.tid and nfttlt.langcode = ttfd2.langcode" 
			+ "  left join node__field_customer_reference nfcr on nfd.nid = nfcr.entity_id and nfd.langcode = nfcr.langcode"
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
		
		from("direct:article").routeId("createMessage")
		.errorHandler(deadLetterChannel("file:errors").useOriginalMessage().maximumRedeliveries(5).redeliveryDelay(3000).useExponentialBackOff())
		.onException(HttpOperationFailedException.class).maximumRedeliveries(0).end()
		.filter()/*.simple("${body} != null")*/.method(new Object() { //results in bean dependency
			@SuppressWarnings("unused")
			public boolean anonymousFilter(@Body Struct body) {
				return body != null && body.getString("type").equals("article"); //label,...
			}
		})
		.process(new Processor() {
			@Override
			public void process(Exchange exchange) throws Exception {
				exchange.getIn().setBody(new Article(exchange.getIn().getBody(Struct.class)));
			}
		})
		.enrich("sql:"+articleQuery, new ArticleEnricher())
		.marshal(myFormat)
		.to("direct:send");
		
		
		from("direct:send").routeId("postMessage").errorHandler(noErrorHandler()) //propagate error back to caller
		.setHeader(Exchange.HTTP_PATH, simple("rest/article"))
		.setHeader(Exchange.HTTP_METHOD, constant("POST"))
		.setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
		//.setHeader(...)
		.log(LoggingLevel.INFO, "Outgoing message ${body} with headers ${headers}")
		.to("http:{{node.server}}:1880");
		
		
		from("file:errors?delay=600000").routeId("retryErrors") //retry every ten minutes 
		.to("direct:send");
		
		
		from("file:errors?delay=3600000&noop=true&idempotent=false").routeId("mailErrors") //every hour
		.process(new Processor() {
			@Override
			public void process(Exchange exchange) throws Exception{
				AttachmentMessage am = exchange.getMessage(AttachmentMessage.class);
				am.addAttachment(am.getHeader("CamelFileNameOnly", String.class), new DataHandler(am.getBody(byte[].class), "plain/text"));
				am.setBody("see attached file");
			}
		})
		.toD("smtp:{{mail.server}}:{{mail.port}}?username={{mail.username}}&password={{mail.password}}&to={{mail.to}}&from={{mail.from}}"
				+ "&subject=drupal publisher could not send file ${header.CamelFileNameOnly}");
	}

}
