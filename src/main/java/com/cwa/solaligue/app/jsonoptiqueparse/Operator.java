
package com.cwa.solaligue.app.jsonoptiqueparse;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class Operator {
  @SerializedName("operatorName")
  @Expose
  private String operatorName;

  @SerializedName("containerName")
  @Expose
  private String containerName;

  @SerializedName("resources")
  @Expose
  private Resources resources;
}
