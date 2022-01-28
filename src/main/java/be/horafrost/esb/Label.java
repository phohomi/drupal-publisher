package be.horafrost.esb;

import org.apache.kafka.connect.data.Struct;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Label {
	
	private Long id;
	@JsonProperty("title")
	public String title;
	@JsonProperty("status")
	public Short status;
	@JsonProperty("filename")
	public String filename;
	@JsonProperty("mail")
	public String mail;

	public Label(Struct body) {
		this.id = body.getInt64("nid");
		this.title = body.getString("title");
		this.status = body.getInt16("status");
	}
}
