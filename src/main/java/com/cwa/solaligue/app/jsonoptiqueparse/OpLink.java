
package com.cwa.solaligue.app.jsonoptiqueparse;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Setter
@Getter
public class OpLink {
  @SerializedName("containerName")
  @Expose
  private String containerName;

  @SerializedName("from")
  @Expose
  private String from;

  @SerializedName("to")
  @Expose
  private String to;

  @SerializedName("paramList")
  @Expose
  private List<ParamList> paramList;
}
