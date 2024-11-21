
package com.cwa.solaligue.app.jsonoptiqueparse;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class Resources {
  @SerializedName("cpu_PER")
  @Expose
  private Double cpuPER;

  @SerializedName("memory_PER")
  @Expose
  private Double memoryPER;

  @SerializedName("memory_EO_PER")
  @Expose
  private Double memoryEOPER;

  @SerializedName("time_MS")
  @Expose
  private Integer timeMS;

  @SerializedName("disk_MB")
  @Expose
  private Double diskMB;
}
