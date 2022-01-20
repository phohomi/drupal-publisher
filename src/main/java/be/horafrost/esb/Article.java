package be.horafrost.esb;

import org.apache.kafka.connect.data.Struct;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

//@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "code", "artikel", "pcbstd", "hdbmaand", "qty", "zakgewicht", "GTIN", "palLayout", "timestamp", "status",
	"lijn1", "lijn2", "lijn3", "lijn4", "lijn5", "lijn6", "lijn7", "layout", "ArtikelNoKlant"})
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
	@JsonProperty("lijn1")
	public String lijn1;
	@JsonProperty("lijn2")
	public String lijn2;
	@JsonProperty("lijn3")
	public String lijn3;
	@JsonProperty("lijn4")
	public String lijn4;
	@JsonProperty("lijn5")
	public String lijn5;
	@JsonProperty("lijn6")
	public String lijn6;
	@JsonProperty("lijn7")
	public String lijn7;
	@JsonProperty("layout")
	public String layout;
	@JsonProperty("ArtikelNoKlant")
	public String artikelnoklant;
	
	public Article(Struct body) {
		this.id = body.getInt64("nid");
		this.artikel = body.getString("title");
		this.status = body.getInt16("status");
	}
	
	public Long getId() {
		return this.id;
	}
}
