package be.horafrost.esb;

import org.apache.kafka.connect.data.Struct;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "code", "artikel", "pcbstd", "hdbmaand", "qty", "zakgewicht", "GTIN", "palLayout" })
public class Article {
	
	private Long id;
	@JsonProperty("code")
	public String code;
	@JsonProperty("artikel")
	public String artikel;
	@JsonProperty("pcbstd")
	public Integer pcbstd;
	@JsonProperty("hdbmaand")
	public Integer hdbmaand; 
	@JsonProperty("qty")
	public Integer qty;
	@JsonProperty("zakgewicht")
	public Double zakgewicht;
	@JsonProperty("GTIN")
	public String gtin;
	@JsonProperty("palLayout")
	public String palLayout;
	@JsonProperty("timestamp")
	public Integer timestamp;
	@JsonProperty("status")
	public Short status;
	
	public Article(Struct body) {
		this.id = body.getInt64("nid");
		this.artikel = body.getString("title");
		this.status = body.getInt16("status");
	}
	
	public Long getId() {
		return this.id;
	}
}
